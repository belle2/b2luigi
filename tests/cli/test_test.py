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
