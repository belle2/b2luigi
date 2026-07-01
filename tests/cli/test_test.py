"""Integration tests for the b2luigi test subcommand."""

import os
import pathlib
import shutil
from unittest import TestCase

from b2luigi.cli.runner import _build_fast_task
from tests.cli.helpers import CLITestCase


FIXTURE_DIR = os.path.dirname(__file__)


class TestTestNormalMode(CLITestCase):
    """Tests for normal (non-force, non-batch) subprocess execution."""

    def setUp(self) -> None:
        super().setUp()
        for fixture in ("cli_test_script.py", "cli_test_script_failing.py", "cli_test_script_silent.py"):
            shutil.copy(os.path.join(FIXTURE_DIR, fixture), os.path.join(self.tmp_dir, fixture))

    def test_normal_run_succeeds(self) -> None:
        """Script that writes its output file should exit 0."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)

    def test_nonzero_exit_propagated(self) -> None:
        """Script that exits non-zero should make the CLI exit non-zero."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script_failing.py", "-o", "result.txt"])
        self.assertNotEqual(rc, 0)

    def test_missing_output_detected(self) -> None:
        """Script that exits 0 without writing output should make the CLI exit non-zero."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script_silent.py", "-o", "result.txt"])
        self.assertNotEqual(rc, 0)


class TestTestForceFlag(CLITestCase):
    """Tests for --force flag behaviour."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_counter.py"),
            os.path.join(self.tmp_dir, "cli_test_script_counter.py"),
        )

    def _counter(self) -> int:
        """Read the run counter written by cli_test_script_counter.py.

        :returns: The integer value written to counter.txt, or 0 if the file
            does not exist yet.
        :rtype: int
        """
        path = os.path.join(self.tmp_dir, "counter.txt")
        return int(pathlib.Path(path).read_text()) if os.path.exists(path) else 0

    def test_normal_mode_skips_when_output_exists(self) -> None:
        """Second run without --force should be skipped by Luigi (counter stays at 1)."""
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 1)
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 1)

    def test_force_reruns_when_output_exists(self) -> None:
        """Second run with --force should re-run the script (counter reaches 2)."""
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 1)
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt", "--force"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 2)


class TestTestBatchFlag(CLITestCase):
    """Tests for --batch flag behaviour."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )

    def test_batch_flag_runs_successfully(self) -> None:
        """--batch should still run the task locally via the local scheduler."""
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script.py", "-o", "result.txt", "--batch"])
        self.assertEqual(rc, 0, stderr)


class TestTestExtraArgs(CLITestCase):
    """Tests for extra-args forwarding."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_echo_args.py"),
            os.path.join(self.tmp_dir, "cli_test_script_echo_args.py"),
        )

    def _read_output(self) -> str:
        """Read the output file written by the echo-args fixture.

        The ``-o`` argument is always ``result.txt``; b2luigi's ``result_dir``
        defaults to ``"."`` which ``map_folder`` resolves to ``cwd`` when
        invoked via the CLI binary (fallback 3 in ``get_filename()``), so
        the file lands directly in ``self.tmp_dir``.

        :returns: The text content of the output file, or an empty string if
            the file does not exist.
        :rtype: str
        """
        path = os.path.join(self.tmp_dir, "result.txt")
        if not os.path.exists(path):
            return ""
        with open(path) as fh:
            return fh.read()

    def test_extra_args_forwarded_to_script(self) -> None:
        """Args after -- should appear in the script's sys.argv."""
        rc, _, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script_echo_args.py", "-o", "result.txt", "--", "--lr", "0.01"],
        )
        self.assertEqual(rc, 0, stderr)
        argv_written = self._read_output()
        self.assertIn("--lr", argv_written)
        self.assertIn("0.01", argv_written)


class TestFastTaskCmdGeneration(TestCase):
    """Unit tests for task_cmd_additional_args injected by _build_fast_task."""

    def test_basic_flags_present(self) -> None:
        """Script and output file are encoded as --script / --output-file."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertEqual(
            FastTask.task_cmd_additional_args,
            ["--script", "script.py", "--output-file", "out.txt"],
        )

    def test_input_file_included(self) -> None:
        """--input-file is appended when input_file is not None."""
        FastTask = _build_fast_task("script.py", "out.txt", "in.txt", False, False, [])
        args = FastTask.task_cmd_additional_args
        self.assertIn("--input-file", args)
        idx = args.index("--input-file")
        self.assertEqual(args[idx + 1], "in.txt")

    def test_force_flag_included_when_true(self) -> None:
        """--force is appended when force=True."""
        FastTask = _build_fast_task("script.py", "out.txt", None, True, False, [])
        self.assertIn("--force", FastTask.task_cmd_additional_args)

    def test_force_flag_absent_when_false(self) -> None:
        """--force is not appended when force=False."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertNotIn("--force", FastTask.task_cmd_additional_args)

    def test_extra_args_encoded_as_repeated_option(self) -> None:
        """Each extra_arg becomes --extra-arg <value> in task_cmd_additional_args."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, ["--lr", "0.01"])
        args = FastTask.task_cmd_additional_args
        values = [args[i + 1] for i, a in enumerate(args) if a == "--extra-arg"]
        self.assertIn("--lr", values)
        self.assertIn("0.01", values)
