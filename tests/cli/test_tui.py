"""
Tests for the optional TUI progress interface.

The entire module is skipped when the 'textual' package is not installed so it
can live in the main test suite without breaking environments that have not
installed b2luigi[tui].
"""

import subprocess
import sys
import textwrap
import unittest
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("textual")

from b2luigi.cli.tui import (  # noqa: E402  (after importorskip)
    TaskGroup,
    TaskInstance,
    ProgressApp,
)


# ── helpers ───────────────────────────────────────────────────────────────────


def _mock_task(task_id="MyTask_x_1", class_name="MyTask", **params):
    task = MagicMock()
    task.__class__.__name__ = class_name
    task.param_kwargs = params
    task.task_id = task_id
    return task


def _mock_task_with_sig(task_id, class_name="MyTask", params=None):
    """Create a mock task with explicit significant flags.

    params: list of (name, value, significant) tuples, in parameter order.
    """
    if params is None:
        params = []
    task = MagicMock()
    task.__class__.__name__ = class_name
    task.param_kwargs = {name: val for name, val, _ in params}
    task.task_id = task_id
    param_objs = []
    for name, _val, significant in params:
        p = MagicMock()
        p.significant = significant
        param_objs.append((name, p))
    task.get_params.return_value = param_objs
    return task


# ── TaskInstance ──────────────────────────────────────────────────────────────


class TestTaskInstance(unittest.TestCase):
    def test_default_status_is_pending(self):
        inst = TaskInstance(_mock_task(x=1))
        self.assertEqual(inst.status, "PENDING")

    def test_params_str_contains_all_params(self):
        inst = TaskInstance(_mock_task(x=1, y="hello"))
        self.assertIn("x=1", inst.params_str)
        self.assertIn("y=hello", inst.params_str)

    def test_empty_params_str(self):
        inst = TaskInstance(_mock_task())
        self.assertEqual(inst.params_str, "")


# ── TaskGroup ─────────────────────────────────────────────────────────────────


class TestTaskGroup(unittest.TestCase):
    def test_get_or_add_deduplicates_by_task_id(self):
        group = TaskGroup("MyTask")
        task = _mock_task("id1", x=1)
        inst_a = group.get_or_add(task)
        inst_b = group.get_or_add(task)
        self.assertIs(inst_a, inst_b)
        self.assertEqual(group.total, 1)

    def test_counts_all_statuses(self):
        group = TaskGroup("MyTask")
        statuses = ["DONE", "DONE", "FAILED", "RUNNING", "PENDING", "PENDING"]
        for i, status in enumerate(statuses):
            inst = group.get_or_add(_mock_task(f"id{i}", x=i))
            inst.status = status
        done, failed, running, pending = group.counts
        self.assertEqual(done, 2)
        self.assertEqual(failed, 1)
        self.assertEqual(running, 1)
        self.assertEqual(pending, 2)

    def test_sorted_instances_are_sorted_by_params_str(self):
        group = TaskGroup("MyTask")
        for i in [3, 1, 2]:
            group.get_or_add(_mock_task(f"id{i}", x=i))
        params = [inst.params_str for inst in group.sorted_instances]
        self.assertEqual(params, sorted(params))


# ── _first_significant_param_val / split-class detection ─────────────────────


class TestFirstSignificantParamVal(unittest.TestCase):
    def test_returns_first_significant_value(self):
        task = _mock_task_with_sig("id1", params=[("x", "val_x", True)])
        self.assertEqual(ProgressApp._first_significant_param_val(task), "val_x")

    def test_skips_non_significant_first_param(self):
        task = _mock_task_with_sig(
            "id1",
            params=[("git_hash", "abc123", False), ("channel", "B2Kee", True)],
        )
        self.assertEqual(ProgressApp._first_significant_param_val(task), "B2Kee")

    def test_returns_none_when_all_params_non_significant(self):
        task = _mock_task_with_sig(
            "id1",
            params=[("git_hash", "abc123", False), ("num_processes", 4, False)],
        )
        self.assertIsNone(ProgressApp._first_significant_param_val(task))

    def test_returns_none_when_no_params(self):
        task = _mock_task_with_sig("id1", params=[])
        self.assertIsNone(ProgressApp._first_significant_param_val(task))

    def test_returns_none_on_get_params_exception(self):
        task = MagicMock()
        task.get_params.side_effect = RuntimeError("broken")
        self.assertIsNone(ProgressApp._first_significant_param_val(task))

    def test_value_is_stringified(self):
        task = _mock_task_with_sig("id1", params=[("n", 42, True)])
        self.assertEqual(ProgressApp._first_significant_param_val(task), "42")


class TestSplitClassDetection(unittest.TestCase):
    """Verify that _pre_populate only uses significant params for split-group keying."""

    _THRESHOLD = ProgressApp._SPLIT_THRESHOLD

    def _make_app(self, tasks):
        app = ProgressApp.__new__(ProgressApp)
        app._task_list = tasks
        app.groups = {}
        app.group_order = []
        app._task_id_map = {}
        app._split_classes = set()
        return app

    def test_non_significant_first_param_does_not_trigger_split(self):
        # Many tasks share the same non-significant first param (git_hash="abc")
        # but differ in a significant second param. Should NOT split because the
        # first *significant* param has many distinct values (not one big subgroup).
        tasks = [
            _mock_task_with_sig(
                f"id{i}",
                params=[("git_hash", "abc", False), ("channel", f"ch_{i}", True)],
            )
            for i in range(self._THRESHOLD + 5)
        ]
        for t in tasks:
            t.complete.return_value = False

        with patch("b2luigi.core.utils.task_iterator", side_effect=lambda r: [r]):
            app = self._make_app(tasks)
            app._pre_populate()

        self.assertNotIn("MyTask", app._split_classes)

    def test_significant_first_param_triggers_split_when_threshold_exceeded(self):
        # All tasks share the same *significant* first param value "run1",
        # which puts them all in one subgroup — exceeding the threshold.
        tasks = [
            _mock_task_with_sig(
                f"id{i}",
                params=[("run", "run1", True), ("idx", i, True)],
            )
            for i in range(self._THRESHOLD + 5)
        ]
        for t in tasks:
            t.complete.return_value = False

        with patch("b2luigi.core.utils.task_iterator", side_effect=lambda r: [r]):
            app = self._make_app(tasks)
            app._pre_populate()

        self.assertIn("MyTask", app._split_classes)

    def test_group_key_skips_non_significant_first_param(self):
        task = _mock_task_with_sig(
            "id1",
            params=[("git_hash", "abc", False), ("channel", "B2Kee", True)],
        )
        app = ProgressApp.__new__(ProgressApp)
        app._split_classes = {"MyTask"}
        self.assertEqual(app._group_key(task), "MyTask [B2Kee]")

    def test_group_key_falls_back_to_class_name_when_no_significant_params(self):
        task = _mock_task_with_sig(
            "id1",
            params=[("git_hash", "abc", False)],
        )
        app = ProgressApp.__new__(ProgressApp)
        app._split_classes = {"MyTask"}
        self.assertEqual(app._group_key(task), "MyTask")


# ── _poll_scheduler ───────────────────────────────────────────────────────────


class TestPollScheduler(unittest.TestCase):
    def _make_app(self, factory=None):
        app = ProgressApp.__new__(ProgressApp)
        app._scheduler_factory = factory
        app.groups = {}
        app.group_order = []
        app._task_id_map = {}
        app._split_classes = set()
        return app

    def _add_task_to_app(self, app, task):
        app._task_id_map[task.task_id] = task
        group_key = task.__class__.__name__
        if group_key not in app.groups:
            app.groups[group_key] = TaskGroup(group_key)
            app.group_order.append(group_key)
        app.groups[group_key].get_or_add(task)

    def _make_scheduler(self, task_list_result):
        factory = MagicMock()
        scheduler = MagicMock()
        factory.scheduler = scheduler
        scheduler.task_list.return_value = task_list_result
        return factory, scheduler

    def test_no_op_when_scheduler_factory_is_none(self):
        app = self._make_app(None)
        app._poll_scheduler()  # should not raise

    def test_no_op_when_factory_has_no_scheduler_attr(self):
        factory = MagicMock(spec=[])  # no 'scheduler' attribute
        app = self._make_app(factory)
        app._poll_scheduler()  # should not raise

    def test_no_op_when_scheduler_is_none(self):
        factory = MagicMock()
        factory.scheduler = None
        app = self._make_app(factory)
        app._poll_scheduler()  # should not raise

    def test_calls_task_list_with_empty_args(self):
        factory, scheduler = self._make_scheduler({})
        app = self._make_app(factory)
        app._poll_scheduler()
        scheduler.task_list.assert_called_once_with("", "")

    def test_task_list_called_exactly_once(self):
        """A single call covers all statuses — not one call per status."""
        factory, scheduler = self._make_scheduler({})
        app = self._make_app(factory)
        app._poll_scheduler()
        self.assertEqual(scheduler.task_list.call_count, 1)

    def test_status_read_from_task_info_field(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {"status": "RUNNING"}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "RUNNING")

    def test_done_status_propagates(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {"status": "DONE"}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "DONE")

    def test_failed_status_propagates(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {"status": "FAILED"}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "FAILED")

    def test_disabled_maps_to_failed(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {"status": "DISABLED"}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "FAILED")

    def test_upstream_failed_maps_to_failed(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {"status": "UPSTREAM_FAILED"}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "FAILED")

    def test_upstream_disabled_maps_to_pending(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {"status": "UPSTREAM_DISABLED"}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "PENDING")

    def test_unknown_status_defaults_to_pending(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {"status": "SOME_NEW_STATUS"}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "PENDING")

    def test_missing_status_key_defaults_to_pending(self):
        task = _mock_task("task_1", x=1)
        factory, scheduler = self._make_scheduler({"task_1": {}})
        app = self._make_app(factory)
        self._add_task_to_app(app, task)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["task_1"].status, "PENDING")

    def test_task_not_in_map_is_skipped(self):
        factory, scheduler = self._make_scheduler({"unknown_task": {"status": "RUNNING"}})
        app = self._make_app(factory)
        app._poll_scheduler()  # should not raise

    def test_exception_in_task_list_is_swallowed(self):
        factory = MagicMock()
        scheduler = MagicMock()
        factory.scheduler = scheduler
        scheduler.task_list.side_effect = RuntimeError("scheduler died")
        app = self._make_app(factory)
        app._poll_scheduler()  # should not raise

    def test_multiple_tasks_updated_in_one_call(self):
        task_a = _mock_task("id_a", x=1)
        task_b = _mock_task("id_b", x=2)
        factory, scheduler = self._make_scheduler(
            {
                "id_a": {"status": "DONE"},
                "id_b": {"status": "RUNNING"},
            }
        )
        app = self._make_app(factory)
        self._add_task_to_app(app, task_a)
        self._add_task_to_app(app, task_b)

        app._poll_scheduler()

        self.assertEqual(app.groups["MyTask"].instances["id_a"].status, "DONE")
        self.assertEqual(app.groups["MyTask"].instances["id_b"].status, "RUNNING")
        self.assertEqual(scheduler.task_list.call_count, 1)

    def test_luigi_to_tui_status_map_is_complete(self):
        expected = {
            "PENDING": "PENDING",
            "RUNNING": "RUNNING",
            "DONE": "DONE",
            "FAILED": "FAILED",
            "DISABLED": "FAILED",
            "UPSTREAM_FAILED": "FAILED",
            "UPSTREAM_DISABLED": "PENDING",
            "UNKNOWN": "PENDING",
        }
        self.assertEqual(ProgressApp._LUIGI_TO_TUI_STATUS, expected)


# ── ProgressApp.check_action ──────────────────────────────────────────────────


class TestProgressAppCheckAction(unittest.TestCase):
    def _make_app(self):
        app = ProgressApp.__new__(ProgressApp)
        app._log_view = None
        return app

    def test_close_log_view_hidden_outside_log_view(self):
        app = self._make_app()
        result = app.check_action("close_log_view", ())
        self.assertFalse(result)

    def test_close_log_view_shown_inside_log_view(self):
        app = self._make_app()
        app._log_view = {"title": "stdout", "content": "hello"}
        result = app.check_action("close_log_view", ())
        self.assertTrue(result)

    def test_navigation_hidden_inside_log_view(self):
        app = self._make_app()
        app._log_view = {"title": "stdout", "content": "hello"}
        for action in ("cursor_up", "cursor_down", "toggle_fold", "open_stdout", "open_stderr"):
            with self.subTest(action=action):
                self.assertIsNone(app.check_action(action, ()))

    def test_navigation_shown_outside_log_view(self):
        app = self._make_app()
        for action in ("cursor_up", "cursor_down", "toggle_fold", "open_stdout", "open_stderr"):
            with self.subTest(action=action):
                self.assertTrue(app.check_action(action, ()))


# ── pre_populate ──────────────────────────────────────────────────────────────


class TestPrePopulate(unittest.TestCase):
    def _make_app(self, tasks):
        app = ProgressApp.__new__(ProgressApp)
        app._task_list = tasks
        app.groups = {}
        app.group_order = []
        app._task_id_map = {}
        app._split_classes = set()
        return app

    def test_already_complete_task_shown_as_done(self):
        task = _mock_task("id1", x=1)
        task.complete.return_value = True

        with patch("b2luigi.core.utils.task_iterator", return_value=[task]):
            app = self._make_app([task])
            app._pre_populate()

        self.assertIn("MyTask", app.groups)
        inst = list(app.groups["MyTask"].instances.values())[0]
        self.assertEqual(inst.status, "DONE")

    def test_incomplete_task_shown_as_pending(self):
        task = _mock_task("id1", x=1)
        task.complete.return_value = False

        with patch("b2luigi.core.utils.task_iterator", return_value=[task]):
            app = self._make_app([task])
            app._pre_populate()

        inst = list(app.groups["MyTask"].instances.values())[0]
        self.assertEqual(inst.status, "PENDING")

    def test_complete_raising_exception_shown_as_pending(self):
        task = _mock_task("id1", x=1)
        task.complete.side_effect = OSError("network error")

        with patch("b2luigi.core.utils.task_iterator", return_value=[task]):
            app = self._make_app([task])
            app._pre_populate()

        inst = list(app.groups["MyTask"].instances.values())[0]
        self.assertEqual(inst.status, "PENDING")

    def test_task_id_map_populated(self):
        task = _mock_task("id1", x=1)
        task.complete.return_value = False

        with patch("b2luigi.core.utils.task_iterator", return_value=[task]):
            app = self._make_app([task])
            app._pre_populate()

        self.assertIn("id1", app._task_id_map)
        self.assertIs(app._task_id_map["id1"], task)


# ── import guard ──────────────────────────────────────────────────────────────


class TestImportGuard(unittest.TestCase):
    def test_run_with_tui_raises_helpful_importerror(self):
        import sys
        from b2luigi.cli import runner

        with patch.dict(sys.modules, {"textual": None, "b2luigi.cli.tui": None}):
            with self.assertRaises(ImportError) as ctx:
                runner.run_with_tui([], MagicMock(), {})
        self.assertIn("b2luigi[tui]", str(ctx.exception))


class TestResourceTrackerStartsBeforeStderrRedirect(unittest.TestCase):
    """The multiprocessing resource tracker must be running before Textual owns stderr.

    Luigi's ``Worker.__init__`` creates a ``multiprocessing.Queue`` from inside the TUI
    worker thread. Under the ``spawn`` start method (the macOS default) the first
    semaphore starts the resource tracker, which passes ``sys.stderr.fileno()`` to the
    child. Textual's redirector answers ``-1`` there, so the spawn failed with
    ``ValueError: bad value(s) in fds_to_keep``. Starting the tracker while stderr is
    still the real terminal sidesteps this; a fork context never registers semaphores,
    which is why Linux never showed it.
    """

    _SCRIPT = textwrap.dedent(
        """
        import io, multiprocessing, sys
        from b2luigi.cli.runner import _ensure_multiprocessing_resource_tracker

        _ensure_multiprocessing_resource_tracker()

        class _TextualLikeRedirect(io.TextIOBase):
            def fileno(self):
                return -1

            def write(self, s):
                return len(s)

        sys.stderr = _TextualLikeRedirect()
        multiprocessing.get_context("spawn").Queue()
        print("OK")
        """
    )

    def test_queue_after_redirect_does_not_raise(self):
        proc = subprocess.run([sys.executable, "-c", self._SCRIPT], capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout)
