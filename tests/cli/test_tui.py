"""
Tests for the optional TUI progress interface.

The entire module is skipped when the 'textual' package is not installed so it
can live in the main test suite without breaking environments that have not
installed b2luigi[tui].
"""

import unittest
from unittest.mock import MagicMock, patch

import luigi
import pytest

pytest.importorskip("textual")

from b2luigi.cli.tui import (  # noqa: E402  (after importorskip)
    TaskGroup,
    TaskInstance,
    _TUIProcessWrapper,
    ProgressApp,
)


# ── helpers ───────────────────────────────────────────────────────────────────


def _mock_task(task_id="MyTask_x_1", class_name="MyTask", **params):
    task = MagicMock()
    task.__class__.__name__ = class_name
    task.param_kwargs = params
    task.task_id = task_id
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


# ── _TUIProcessWrapper ────────────────────────────────────────────────────────


class TestTUIProcessWrapper(unittest.TestCase):
    def _make_wrapper(self, is_batch=False, **kwargs):
        wrapped = MagicMock()
        task = MagicMock()
        wrapper = _TUIProcessWrapper(wrapped, task)
        wrapper._is_batch = is_batch
        return wrapper, wrapped, task

    def test_use_multiprocessing_always_false(self):
        wrapper, _, _ = self._make_wrapper()
        self.assertFalse(wrapper.use_multiprocessing)

    # ── local (non-batch) task ────────────────────────────────────────────────

    def test_local_run_delegates_and_does_not_fire_events(self):
        wrapper, wrapped, task = self._make_wrapper(is_batch=False)
        wrapper.run()
        wrapped.run.assert_called_once()
        task.trigger_event.assert_not_called()

    # ── batch task ────────────────────────────────────────────────────────────

    def test_batch_run_fires_start_event(self):
        wrapper, wrapped, task = self._make_wrapper(is_batch=True)
        wrapper.run()
        first_call_event = task.trigger_event.call_args_list[0][0][0]
        self.assertEqual(first_call_event, luigi.Event.START)

    def test_batch_run_fires_failure_when_start_job_raises(self):
        wrapper, wrapped, task = self._make_wrapper(is_batch=True)
        wrapped.run.side_effect = RuntimeError("condor_submit not found")
        wrapper.run()
        self.assertTrue(wrapper._start_failed)
        fired_events = [c[0][0] for c in task.trigger_event.call_args_list]
        self.assertIn(luigi.Event.FAILURE, fired_events)

    def test_start_failed_makes_is_alive_return_false(self):
        wrapper, wrapped, task = self._make_wrapper(is_batch=True)
        wrapped.run.side_effect = RuntimeError("submit error")
        wrapper.run()
        self.assertFalse(wrapper.is_alive())

    def test_batch_is_alive_fires_success_when_complete(self):
        wrapper, wrapped, task = self._make_wrapper(is_batch=True)
        wrapped.is_alive.return_value = False
        wrapper.task.complete.return_value = True
        wrapper.is_alive()
        fired_events = [c[0][0] for c in task.trigger_event.call_args_list]
        self.assertIn(luigi.Event.SUCCESS, fired_events)

    def test_batch_is_alive_fires_failure_when_not_complete(self):
        wrapper, wrapped, task = self._make_wrapper(is_batch=True)
        wrapped.is_alive.return_value = False
        wrapper.task.complete.return_value = False
        wrapper.is_alive()
        fired_events = [c[0][0] for c in task.trigger_event.call_args_list]
        self.assertIn(luigi.Event.FAILURE, fired_events)

    def test_batch_done_event_fires_only_once(self):
        wrapper, wrapped, task = self._make_wrapper(is_batch=True)
        wrapped.is_alive.return_value = False
        wrapper.task.complete.return_value = True
        wrapper.is_alive()
        wrapper.is_alive()
        success_calls = [c for c in task.trigger_event.call_args_list if c[0][0] == luigi.Event.SUCCESS]
        self.assertEqual(len(success_calls), 1)


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


# ── import guard ──────────────────────────────────────────────────────────────


class TestImportGuard(unittest.TestCase):
    def test_run_with_tui_raises_helpful_importerror(self):
        import sys
        from b2luigi.cli import runner

        with patch.dict(sys.modules, {"textual": None, "b2luigi.cli.tui": None}):
            with self.assertRaises(ImportError) as ctx:
                runner.run_with_tui([], MagicMock(), {})
        self.assertIn("b2luigi[tui]", str(ctx.exception))
