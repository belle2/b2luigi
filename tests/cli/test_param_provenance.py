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
        with patch.object(runner, "stderr_console", Console(file=buffer, width=200)):
            warn_ignored_params(["alpha", "beta"], "TaskB")
        output = buffer.getvalue()

        self.assertEqual(len(output.strip().splitlines()), 1)
        self.assertIn("TaskB", output)
        self.assertIn("alpha", output)
        self.assertIn("beta", output)

    def test_writes_to_the_stderr_console_not_stdout(self) -> None:
        """Machine-readable stdout (``show --paths``, ``graph --format dot``) must stay clean."""
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        with (
            patch.object(runner, "console", Console(file=stdout_buffer, width=200)),
            patch.object(runner, "stderr_console", Console(file=stderr_buffer, width=200)),
        ):
            warn_ignored_params(["alpha"], "TaskB")

        self.assertEqual(stdout_buffer.getvalue(), "")
        self.assertIn("alpha", stderr_buffer.getvalue())

    def test_markup_like_keys_are_neither_fatal_nor_swallowed(self) -> None:
        """Keys are user-controlled; a markup string would raise or delete them."""
        buffer = io.StringIO()
        with patch.object(runner, "stderr_console", Console(file=buffer, width=200)):
            warn_ignored_params(["[/foo]", "[bold]"], "TaskB")
        output = buffer.getvalue()

        self.assertIn("[/foo]", output)
        self.assertIn("[bold]", output)


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


class TestShowProvenance(ProvenanceProjectTestCase):
    """Named show is strict; whole-tree show warns."""

    def test_named_task_with_unknown_override_errors(self) -> None:
        """Case 3: naming a task makes an inapplicable override unambiguous."""
        returncode, stdout, stderr = self._run_cli("show", ["TaskA", "--param", "numbr=99"])
        combined = stdout + stderr

        self.assertEqual(returncode, 2, f"expected exit 2, got {returncode}: {combined}")
        self.assertIn("numbr", combined)
        self.assertIn("number", combined)

    def test_whole_tree_with_unknown_override_warns_and_renders(self) -> None:
        """With no task named, an unmatched override is not fatal."""
        returncode, stdout, stderr = self._run_cli("show", ["--param", "numbr=99"])
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertIn("numbr", combined)
        self.assertIn("TaskA", combined)  # still rendered

    def test_whole_tree_with_matching_override_does_not_warn(self) -> None:
        """A key some class accepts is not reported, even though others reject it."""
        returncode, stdout, stderr = self._run_cli("show", ["--param", "number=99"])
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertNotIn("ignoring parameters", combined.lower())
        self.assertNotIn("no task matched", combined.lower())


class TestShowWithRequirementsConsideredSet(CLITestCase):
    """A config key declared only by a requirement must not be reported."""

    def setUp(self) -> None:
        super().setUp()
        with open(os.path.join(self.tmp_dir, "tasks.py"), "w") as handle:
            handle.write(
                "import b2luigi\n"
                "\n"
                "\n"
                "class ChildTask(b2luigi.Task):\n"
                "    depth = b2luigi.IntParameter(default=1)\n"
                "\n"
                "    def output(self):\n"
                "        yield self.add_to_output('c.txt')\n"
                "\n"
                "\n"
                "class ParentTask(b2luigi.Task):\n"
                "    width = b2luigi.IntParameter(default=1)\n"
                "\n"
                "    def requires(self):\n"
                "        yield ChildTask(depth=self.width)\n"
                "\n"
                "    def output(self):\n"
                "        yield self.add_to_output('p.txt')\n"
            )
        with open(os.path.join(self.tmp_dir, "parameters.py"), "w") as handle:
            handle.write("config = {'width': 2, 'depth': 3}\n")
        with open(os.path.join(self.tmp_dir, "settings.json"), "w") as handle:
            handle.write('{"result_dir": "results"}\n')

    def test_requirement_only_key_is_not_reported(self) -> None:
        """`depth` belongs to ChildTask, reached only via --with-requirements."""
        returncode, stdout, stderr = self._run_cli("show", ["ParentTask", "--with-requirements"])
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertNotIn("ignoring parameters", combined.lower())


class TestShowNamedConfigWarn(ProvenanceProjectTestCase):
    """A named task warns (not errors) on a config key it does not declare."""

    def test_named_task_warns_on_undeclared_config_key(self) -> None:
        """TaskB doesn't declare `number`, which the shared config sets to 7."""
        returncode, stdout, stderr = self._run_cli("show", ["TaskB"])
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertIn("ignoring parameters", combined.lower())
        self.assertIn("number", combined)
        self.assertIn("TaskB", combined)


class TestGraphProvenance(ProvenanceProjectTestCase):
    """graph's traversal is unconditional, unlike show's --with-requirements gate."""

    def test_named_task_with_unknown_override_errors(self) -> None:
        """Naming a task makes an inapplicable override unambiguous."""
        returncode, stdout, stderr = self._run_cli("graph", ["TaskA", "--param", "numbr=99"])
        combined = stdout + stderr

        self.assertEqual(returncode, 2, f"expected exit 2, got {returncode}: {combined}")
        self.assertIn("numbr", combined)
        self.assertIn("number", combined)  # did-you-mean

    def test_whole_tree_with_unknown_override_warns_and_renders(self) -> None:
        """With no task named, an unmatched override is not fatal."""
        returncode, stdout, stderr = self._run_cli("graph", ["--param", "numbr=99"])
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertIn("numbr", combined)
        self.assertIn("TaskA", combined)  # still rendered


class RequirementProjectTestCase(CLITestCase):
    """ParentTask declares `width`; its requirement ChildTask declares `depth`."""

    def setUp(self) -> None:
        super().setUp()
        with open(os.path.join(self.tmp_dir, "tasks.py"), "w") as handle:
            handle.write(
                "import b2luigi\n"
                "\n"
                "\n"
                "class ChildTask(b2luigi.Task):\n"
                "    depth = b2luigi.IntParameter(default=1)\n"
                "\n"
                "    def output(self):\n"
                "        yield self.add_to_output('c.txt')\n"
                "\n"
                "    def run(self):\n"
                "        with open(self.get_output_file_name('c.txt'), 'w') as handle:\n"
                "            handle.write('c')\n"
                "\n"
                "    def remove_output(self):\n"
                "        self._remove_output()\n"
                "\n"
                "\n"
                "class ParentTask(b2luigi.Task):\n"
                "    width = b2luigi.IntParameter(default=1)\n"
                "\n"
                "    def requires(self):\n"
                "        yield ChildTask(depth=self.width)\n"
                "\n"
                "    def output(self):\n"
                "        yield self.add_to_output('p.txt')\n"
                "\n"
                "    def run(self):\n"
                "        with open(self.get_output_file_name('p.txt'), 'w') as handle:\n"
                "            handle.write('p')\n"
                "\n"
                "    def remove_output(self):\n"
                "        self._remove_output()\n"
            )
        with open(os.path.join(self.tmp_dir, "parameters.py"), "w") as handle:
            handle.write("config = {'width': 2, 'depth': 3}\n")
        with open(os.path.join(self.tmp_dir, "settings.json"), "w") as handle:
            handle.write('{"result_dir": "results"}\n')


class TestRemoveWithRequirementsConsideredSet(RequirementProjectTestCase):
    """`remove --with-requirements` deletes the tree, so it must consider the tree."""

    def test_requirement_only_config_key_is_not_reported(self) -> None:
        """`depth` belongs to ChildTask, which --with-requirements will delete."""
        returncode, stdout, stderr = self._run_cli("remove", ["ParentTask", "--with-requirements", "-y"])
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertNotIn("ignoring parameters", combined.lower())

    def test_requirement_only_override_is_accepted(self) -> None:
        """A --param aimed at a requirement is applicable, since it is in scope."""
        returncode, stdout, stderr = self._run_cli(
            "remove", ["ParentTask", "--with-requirements", "--param", "depth=3", "-y"]
        )
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertNotIn("has no parameter", combined)

    def test_show_and_remove_agree_on_the_same_project(self) -> None:
        """The divergence that motivated the fix: both must be silent here."""
        show_rc, show_out, show_err = self._run_cli("show", ["ParentTask", "--with-requirements"])
        remove_rc, remove_out, remove_err = self._run_cli("remove", ["ParentTask", "--with-requirements", "-y"])

        self.assertEqual(show_rc, 0, show_out + show_err)
        self.assertEqual(remove_rc, 0, remove_out + remove_err)
        self.assertNotIn("ignoring parameters", (show_out + show_err).lower())
        self.assertNotIn("ignoring parameters", (remove_out + remove_err).lower())

    def test_an_override_no_task_in_the_tree_declares_still_errors(self) -> None:
        """Control: broadening the considered set must not disable strictness."""
        returncode, stdout, stderr = self._run_cli(
            "remove", ["ParentTask", "--with-requirements", "--param", "numbr=99", "-y"]
        )
        combined = stdout + stderr

        self.assertEqual(returncode, 2, f"expected exit 2, got {returncode}: {combined}")
        self.assertIn("numbr", combined)


class TestRemoveWithoutNamesProvenance(ProvenanceProjectTestCase):
    """`remove` with no names still errors, but must name its target readably."""

    def test_unknown_override_errors_and_removes_nothing(self) -> None:
        returncode, stdout, stderr = self._run_cli("run", ["TaskA"])
        self.assertEqual(returncode, 0, f"setup run failed: {stdout + stderr}")
        produced = os.path.join(self.tmp_dir, "results", "number=7", "a.txt")
        self.assertTrue(os.path.exists(produced), "setup did not produce the output")

        returncode, stdout, stderr = self._run_cli("remove", ["--param", "bogus=1", "-y"])
        combined = stdout + stderr

        self.assertEqual(returncode, 2, f"expected exit 2, got {returncode}: {combined}")
        self.assertIn("bogus", combined)
        self.assertTrue(os.path.exists(produced), "remove deleted the output despite erroring")

    def test_target_is_named_any_task_not_every_class(self) -> None:
        """Enumerating every project class is unbounded and reads as a broken sentence."""
        returncode, stdout, stderr = self._run_cli("remove", ["--param", "bogus=1", "-y"])
        combined = stdout + stderr

        self.assertEqual(returncode, 2, f"expected exit 2, got {returncode}: {combined}")
        self.assertIn("any task", combined)
        self.assertNotIn("TaskA, TaskB", combined)


class TestWarningStreamSeparation(ProvenanceProjectTestCase):
    """Warnings must never reach the machine-readable stdout of --paths / --format dot."""

    def test_show_paths_stdout_is_only_paths(self) -> None:
        returncode, stdout, stderr = self._run_cli("show", ["TaskB", "--paths"], extra_env={"COLUMNS": "200"})

        self.assertEqual(returncode, 0, f"expected exit 0: {stdout + stderr}")
        self.assertIn("ignoring parameters", stderr.lower())
        self.assertNotIn("ignoring parameters", stdout.lower())
        for line in stdout.splitlines():
            if line.strip():
                self.assertTrue(os.path.isabs(line.strip()), f"non-path line on stdout: {line!r}")

    def test_graph_dot_stdout_is_valid_dot(self) -> None:
        returncode, stdout, stderr = self._run_cli("graph", ["TaskB", "--format", "dot"], extra_env={"COLUMNS": "200"})

        self.assertEqual(returncode, 0, f"expected exit 0: {stdout + stderr}")
        self.assertIn("ignoring parameters", stderr.lower())
        self.assertNotIn("ignoring parameters", stdout.lower())
        self.assertTrue(stdout.lstrip().startswith("digraph"), f"stdout does not start with digraph: {stdout[:80]!r}")


class TestMarkupLikeOverrideKey(ProvenanceProjectTestCase):
    """A --param key is arbitrary text and must survive the never-fatal warn branch."""

    def test_closing_tag_key_does_not_crash(self) -> None:
        returncode, stdout, stderr = self._run_cli("show", ["--param", "[/foo]=1"], extra_env={"COLUMNS": "200"})
        combined = stdout + stderr

        self.assertEqual(returncode, 0, f"expected exit 0, got {returncode}: {combined}")
        self.assertNotIn("MarkupError", combined)
        self.assertNotIn("Traceback", combined)
        self.assertIn("[/foo]", stderr)

    def test_style_tag_key_is_not_swallowed(self) -> None:
        returncode, stdout, stderr = self._run_cli("show", ["--param", "[bold]=1"], extra_env={"COLUMNS": "200"})

        self.assertEqual(returncode, 0, f"expected exit 0: {stdout + stderr}")
        self.assertIn("[bold]", stderr)
