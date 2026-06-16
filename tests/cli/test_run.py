"""Integration tests for b2luigi run CLI command.

:Description: Tests the ``b2luigi run`` command end-to-end, covering task
    execution, listing, help display, and error handling.
"""

import os

from .helpers import CLITestCase


class TestRun(CLITestCase):
    """Integration tests for the run command."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_run_list(self) -> None:
        """Verify that ``b2luigi run list`` exits 0 and shows available tasks."""
        returncode, stdout, stderr = self._run_cli("run", ["list"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_run_help_leaftask(self) -> None:
        """Verify that ``b2luigi run help LeafTask`` shows the task docstring."""
        returncode, stdout, stderr = self._run_cli("run", ["help", "LeafTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertTrue("leaf" in stdout.lower() or "simple" in stdout.lower())

    def test_run_help_shows_params(self) -> None:
        """Verify that task help output contains parameter names."""
        returncode, stdout, stderr = self._run_cli("run", ["help", "RootTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("RootTask", stdout)
        self.assertTrue("split" in stdout or "parameter" in stdout.lower())

    def test_run_help_unknown_task_error(self) -> None:
        """Verify that ``b2luigi run help Nonexistent`` exits with error."""
        returncode, stdout, stderr = self._run_cli("run", ["help", "Nonexistent"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_run_dry_run(self) -> None:
        """Verify that ``b2luigi run -t LeafTask --dry`` exits 0 or 256."""
        returncode, stdout, stderr = self._run_cli("run", ["-t", "LeafTask", "--dry"])
        self.assertIn(returncode, (0, 256), f"Unexpected exit code: {returncode}, stderr: {stderr}")

    def test_run_unknown_task_error(self) -> None:
        """Verify that ``b2luigi run -t Nonexistent`` exits with error."""
        returncode, stdout, stderr = self._run_cli("run", ["-t", "Nonexistent"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_run_with_param_override(self) -> None:
        """Verify that ``--param split=99 --dry`` works without parameter errors."""
        returncode, stdout, stderr = self._run_cli("run", ["-t", "LeafTask", "--param", "split=99", "--dry"])
        self.assertIn(returncode, (0, 256), f"Unexpected exit code: {returncode}")

    def test_run_with_custom_task_file(self) -> None:
        """Verify that ``--task-file`` flag works."""
        returncode, stdout, stderr = self._run_cli("run", ["list", "-f", "tasks.py"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)

    def test_run_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("run", ["list"])
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))
