"""Integration tests for the b2luigi batch-runner subcommand in test mode."""

import os
import pathlib
import shutil

from tests.cli.helpers import CLITestCase

FIXTURE_DIR = os.path.dirname(__file__)


class TestBatchRunnerTestMode(CLITestCase):
    """Tests for batch_runner --script test-mode reconstruction."""

    def setUp(self) -> None:
        super().setUp()
        for fixture in (
            "cli_test_script.py",
            "cli_test_script_counter.py",
            "cli_test_script_echo_args.py",
        ):
            shutil.copy(os.path.join(FIXTURE_DIR, fixture), os.path.join(self.tmp_dir, fixture))

    def test_script_and_output_runs_successfully(self) -> None:
        """--script + --output-file should run the script and exit 0."""
        rc, _, stderr = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script.py", "--output-file", "result.txt"],
        )
        self.assertEqual(rc, 0, stderr)

    def test_force_reruns_when_output_exists(self) -> None:
        """--force should re-run the script even when the output already exists."""
        rc, _, stderr = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script_counter.py", "--output-file", "result.txt"],
        )
        self.assertEqual(rc, 0, stderr)
        rc, _, stderr = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script_counter.py", "--output-file", "result.txt", "--force"],
        )
        self.assertEqual(rc, 0, stderr)
        counter = int(pathlib.Path(os.path.join(self.tmp_dir, "counter.txt")).read_text())
        self.assertEqual(counter, 2)

    def test_extra_arg_forwarded_to_script(self) -> None:
        """--extra-arg values should reach the script's sys.argv."""
        rc, _, stderr = self._run_cli(
            "batch-runner",
            [
                "--script",
                "cli_test_script_echo_args.py",
                "--output-file",
                "result.txt",
                "--extra-arg",
                "--lr",
                "--extra-arg",
                "0.01",
            ],
        )
        self.assertEqual(rc, 0, stderr)
        content = pathlib.Path(os.path.join(self.tmp_dir, "result.txt")).read_text()
        self.assertIn("--lr", content)
        self.assertIn("0.01", content)

    def test_missing_output_file_is_an_error(self) -> None:
        """--script without --output-file should exit non-zero."""
        rc, _, _ = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script.py"],
        )
        self.assertNotEqual(rc, 0)

    def test_neither_classname_nor_script_is_an_error(self) -> None:
        """Invoking batch-runner with no flags should exit non-zero."""
        rc, _, _ = self._run_cli("batch-runner", [])
        self.assertNotEqual(rc, 0)
