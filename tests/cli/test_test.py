"""Integration tests for the b2luigi test subcommand."""

import os
import shutil

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
        return int(open(path).read()) if os.path.exists(path) else 0

    def test_normal_mode_skips_when_output_exists(self) -> None:
        """Second run without --force should be skipped by Luigi (counter stays at 1)."""
        self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(self._counter(), 1)
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 1)

    def test_force_reruns_when_output_exists(self) -> None:
        """Second run with --force should re-run the script (counter reaches 2)."""
        self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(self._counter(), 1)
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt", "--force"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 2)
