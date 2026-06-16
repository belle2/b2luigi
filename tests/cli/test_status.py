"""Integration tests for b2luigi status CLI command.

:Description: Tests the ``b2luigi status`` command end-to-end. Status is
    equivalent to ``b2luigi show`` with no ``-t`` flag — it shows the full
    dependency tree with output existence markers.
"""

import os

from .helpers import CLITestCase


class TestStatus(CLITestCase):
    """Integration tests for the status command."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_status_shows_all_tasks(self) -> None:
        """Verify that ``b2luigi status`` exits 0 and lists all tasks."""
        returncode, stdout, stderr = self._run_cli("status")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_status_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py exits non-zero with a helpful message."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("status")
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))

    def test_status_with_task_file_flag(self) -> None:
        """Verify that ``--task-file`` flag is accepted."""
        returncode, stdout, stderr = self._run_cli("status", ["--task-file", "tasks.py"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)

    def test_status_with_params_file_flag(self) -> None:
        """Verify that ``--params-file`` flag is accepted."""
        returncode, stdout, stderr = self._run_cli("status", ["--params-file", "parameters.py"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertTrue("LeafTask" in stdout or "RootTask" in stdout)
