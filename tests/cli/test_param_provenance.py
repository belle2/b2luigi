"""Tests for provenance-based parameter partitioning.

:Description: A ``parameters.py`` key that no task declares is filtered with a
    warning; a ``--param`` override that applies to nothing is an error. These
    tests cover the pure helper; command wiring is tested per command.
"""

import io
import os
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

from .helpers import CLITestCase


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


class ProvenanceProjectTestCase(CLITestCase):
    """A project where TaskA declares `number` and TaskB does not."""

    def setUp(self) -> None:
        super().setUp()
        with open(os.path.join(self.tmp_dir, "tasks.py"), "w") as handle:
            handle.write(
                "import b2luigi\n"
                "\n"
                "\n"
                "class TaskA(b2luigi.Task):\n"
                "    number = b2luigi.IntParameter(default=1)\n"
                "\n"
                "    def output(self):\n"
                "        yield self.add_to_output('a.txt')\n"
                "\n"
                "    def run(self):\n"
                "        with open(self.get_output_file_name('a.txt'), 'w') as handle:\n"
                "            handle.write('a')\n"
                "\n"
                "    def remove_output(self):\n"
                "        self._remove_output()\n"
                "\n"
                "\n"
                "class TaskB(b2luigi.Task):\n"
                "    label = b2luigi.Parameter(default='plain')\n"
                "\n"
                "    def output(self):\n"
                "        yield self.add_to_output('b.txt')\n"
                "\n"
                "    def run(self):\n"
                "        with open(self.get_output_file_name('b.txt'), 'w') as handle:\n"
                "            handle.write('b')\n"
                "\n"
                "    def remove_output(self):\n"
                "        self._remove_output()\n"
            )
        with open(os.path.join(self.tmp_dir, "parameters.py"), "w") as handle:
            handle.write("config = {'number': 7}\n")
        with open(os.path.join(self.tmp_dir, "settings.json"), "w") as handle:
            handle.write('{"result_dir": "results"}\n')


class TestRunProvenance(ProvenanceProjectTestCase):
    """run filters config keys with a warning and rejects unknown overrides."""

    def test_config_key_not_declared_is_filtered_with_a_warning(self) -> None:
        """Case 2: a shared parameters.py must work across differing tasks."""
        returncode, stdout, stderr = self._run_cli("run", ["TaskB"])
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"run failed: {combined}")
        self.assertIn("number", combined)
        self.assertIn("TaskB", combined)
        self.assertNotIn("UnknownParameterException", combined)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "results", "label=plain", "b.txt")))

    def test_unknown_override_is_a_clean_error(self) -> None:
        """Case 4: an explicit --param that applies to nothing is fatal."""
        returncode, stdout, stderr = self._run_cli("run", ["TaskA", "--param", "numbr=99"])
        combined = stdout + stderr

        self.assertEqual(returncode, 2, f"expected exit 2, got {returncode}: {combined}")
        self.assertIn("numbr", combined)
        self.assertIn("number", combined)  # did-you-mean
        self.assertNotIn("Traceback", combined)
        self.assertNotIn("UnknownParameterException", combined)

    def test_declared_override_still_applies(self) -> None:
        """Control: a correctly spelled override reaches the task."""
        returncode, stdout, stderr = self._run_cli("run", ["TaskA", "--param", "number=99"])
        self.assertEqual(returncode, 0, f"run failed: {stdout + stderr}")
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "results", "number=99", "a.txt")))


class TestRunSweepInteraction(CLITestCase):
    """The two constraints that a plausible implementation breaks."""

    def setUp(self) -> None:
        super().setUp()
        with open(os.path.join(self.tmp_dir, "tasks.py"), "w") as handle:
            handle.write(
                "import b2luigi\n"
                "\n"
                "\n"
                "class SweepTask(b2luigi.Task):\n"
                "    number = b2luigi.IntParameter(default=1)\n"
                "    label = b2luigi.Parameter(default='x')\n"
                "\n"
                "    def output(self):\n"
                "        yield self.add_to_output('s.txt')\n"
                "\n"
                "    def run(self):\n"
                "        with open(self.get_output_file_name('s.txt'), 'w') as handle:\n"
                "            handle.write('s')\n"
                "\n"
                "    def remove_output(self):\n"
                "        self._remove_output()\n"
            )
        with open(os.path.join(self.tmp_dir, "settings.json"), "w") as handle:
            handle.write('{"result_dir": "results"}\n')

    def test_zipped_generator_sweep_survives_filtering(self) -> None:
        """The sentinel key must not be mistaken for a parameter name.

        Filtering the raw config would drop the whole ZippedParameterGenerator
        and produce ONE combination instead of two.
        """
        with open(os.path.join(self.tmp_dir, "parameters.py"), "w") as handle:
            handle.write(
                "import b2luigi\n"
                "config = {'zipped': b2luigi.ZippedParameterGenerator("
                "number=[1, 2], label=['a', 'b'])}\n"
            )

        returncode, stdout, stderr = self._run_cli("run", ["SweepTask"])
        self.assertEqual(returncode, 0, f"run failed: {stdout + stderr}")

        results = os.path.join(self.tmp_dir, "results")
        self.assertTrue(os.path.exists(os.path.join(results, "number=1", "label=a", "s.txt")))
        self.assertTrue(os.path.exists(os.path.join(results, "number=2", "label=b", "s.txt")))
        self.assertNotIn("zipped", stdout + stderr)

    def test_large_sweep_warns_exactly_once(self) -> None:
        """The warning is per invocation, not per task instance."""
        with open(os.path.join(self.tmp_dir, "parameters.py"), "w") as handle:
            handle.write(
                "import b2luigi\n"
                "config = {\n"
                "    'number': b2luigi.ParameterGenerator(list(range(50))),\n"
                "    'stray': 1,\n"
                "}\n"
            )

        returncode, stdout, stderr = self._run_cli("run", ["SweepTask"])
        combined = stdout + stderr
        self.assertEqual(returncode, 0, f"run failed: {combined}")

        warnings = [line for line in combined.splitlines() if "ignoring parameters" in line.lower()]
        self.assertEqual(len(warnings), 1, f"expected exactly one warning, got {len(warnings)}: {warnings}")


class TestRemoveProvenance(ProvenanceProjectTestCase):
    """remove is as strict as run, because it deletes files."""

    def test_unknown_override_removes_nothing(self) -> None:
        returncode, stdout, stderr = self._run_cli("run", ["TaskA"])
        self.assertEqual(returncode, 0, f"setup run failed: {stdout + stderr}")
        produced = os.path.join(self.tmp_dir, "results", "number=7", "a.txt")
        self.assertTrue(os.path.exists(produced), "setup did not produce the output")

        returncode, stdout, stderr = self._run_cli("remove", ["TaskA", "--param", "numbr=99", "-y"])
        combined = stdout + stderr

        self.assertEqual(returncode, 2, f"expected exit 2, got {returncode}: {combined}")
        self.assertIn("numbr", combined)
        self.assertTrue(os.path.exists(produced), "remove deleted the output despite erroring")
