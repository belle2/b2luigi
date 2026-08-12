"""Tests for provenance-based parameter partitioning.

:Description: A ``parameters.py`` key that no task declares is filtered with a
    warning; a ``--param`` override that applies to nothing is an error. These
    tests cover the pure helper; command wiring is tested per command.
"""

import io
from unittest import TestCase
from unittest.mock import patch

from rich.console import Console

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    partition_params,
    unknown_param_error,
    warn_ignored_params,
)


class TestPartitionParams(TestCase):
    """The pure partition helper."""

    def test_keeps_accepted_keys(self) -> None:
        kept, dropped_config, dropped_override = partition_params(
            {"number": 1, "label": "a"}, {"number", "label"}, frozenset()
        )
        self.assertEqual(kept, {"number": 1, "label": "a"})
        self.assertEqual(dropped_config, [])
        self.assertEqual(dropped_override, [])

    def test_drops_unaccepted_config_key(self) -> None:
        kept, dropped_config, dropped_override = partition_params({"number": 1, "stray": 9}, {"number"}, frozenset())
        self.assertEqual(kept, {"number": 1})
        self.assertEqual(dropped_config, ["stray"])
        self.assertEqual(dropped_override, [])

    def test_classifies_dropped_key_as_override_when_it_came_from_param(self) -> None:
        """Provenance decides the bucket, not the key itself."""
        kept, dropped_config, dropped_override = partition_params(
            {"number": 1, "stray": 9}, {"number"}, frozenset({"stray"})
        )
        self.assertEqual(kept, {"number": 1})
        self.assertEqual(dropped_config, [])
        self.assertEqual(dropped_override, ["stray"])

    def test_an_override_key_that_is_accepted_is_not_dropped(self) -> None:
        kept, dropped_config, dropped_override = partition_params({"number": 5}, {"number"}, frozenset({"number"}))
        self.assertEqual(kept, {"number": 5})
        self.assertEqual(dropped_config, [])
        self.assertEqual(dropped_override, [])

    def test_dropped_lists_are_sorted(self) -> None:
        _, dropped_config, _ = partition_params({"zulu": 1, "alpha": 2, "mike": 3}, set(), frozenset())
        self.assertEqual(dropped_config, ["alpha", "mike", "zulu"])


class TestUnknownParamError(TestCase):
    """The error carries a did-you-mean when one is close enough."""

    def test_suggests_a_near_match(self) -> None:
        error = unknown_param_error(["numbr"], {"number", "label"}, "TaskA")
        message = str(error)
        self.assertIn("TaskA", message)
        self.assertIn("numbr", message)
        self.assertIn("Did you mean 'number'?", message)

    def test_omits_suggestion_when_nothing_is_close(self) -> None:
        error = unknown_param_error(["zzzzzz"], {"number", "label"}, "TaskA")
        message = str(error)
        self.assertNotIn("Did you mean", message)
        self.assertIn("number", message)  # lists the real parameters instead

    def test_is_a_cli_user_error(self) -> None:
        self.assertIsInstance(unknown_param_error(["x"], {"y"}, "TaskA"), CliUserError)


class TestWarnIgnoredParams(TestCase):
    """The warning names the target and every dropped key, on one line."""

    def test_emits_one_line_naming_keys_and_target(self) -> None:
        buffer = io.StringIO()
        with patch.object(runner, "console", Console(file=buffer, width=200)):
            warn_ignored_params(["alpha", "beta"], "TaskB")
        output = buffer.getvalue()

        self.assertEqual(len(output.strip().splitlines()), 1)
        self.assertIn("TaskB", output)
        self.assertIn("alpha", output)
        self.assertIn("beta", output)


class TestTaskContextOverrideKeys(TestCase):
    """resolve_task_context records which keys came from --param."""

    def test_override_keys_records_param_flags(self) -> None:
        import os
        import tempfile
        from b2luigi.cli.utils import resolve_task_context

        project = tempfile.mkdtemp()
        with open(os.path.join(project, "tasks.py"), "w") as handle:
            handle.write(
                "import b2luigi\n\n\nclass OnlyTask(b2luigi.Task):\n    number = b2luigi.IntParameter(default=1)\n"
            )
        with open(os.path.join(project, "parameters.py"), "w") as handle:
            handle.write("config = {'number': 7}\n")

        cwd = os.getcwd()
        os.chdir(project)
        try:
            ctx = resolve_task_context("tasks.py", "parameters.py", ["extra=3"])
        finally:
            os.chdir(cwd)

        self.assertEqual(ctx.override_keys, frozenset({"extra"}))
        self.assertIn("number", ctx.merged_params)
