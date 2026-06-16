"""Integration tests for b2luigi tasks CLI command.

:Description: Tests ``b2luigi tasks`` (list) and ``b2luigi tasks info``
    end-to-end, verifying exit codes, output content, and error handling.
"""

import os

from .helpers import CLITestCase


class TestTasks(CLITestCase):
    """Integration tests for the tasks command."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_tasks_exits_zero(self) -> None:
        """Verify that ``b2luigi tasks`` exits with code 0."""
        returncode, stdout, stderr = self._run_cli("tasks")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_tasks_lists_task_names(self) -> None:
        """Verify that ``b2luigi tasks`` shows all available task class names."""
        _, stdout, _ = self._run_cli("tasks")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_tasks_info_specific_task_exits_zero(self) -> None:
        """Verify that ``b2luigi tasks info LeafTask`` exits with code 0."""
        returncode, stdout, stderr = self._run_cli("tasks", ["info", "LeafTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_tasks_info_specific_task_shows_name(self) -> None:
        """Verify that ``b2luigi tasks info LeafTask`` includes the task name in output."""
        _, stdout, _ = self._run_cli("tasks", ["info", "LeafTask"])
        self.assertIn("LeafTask", stdout)

    def test_tasks_info_specific_task_shows_params(self) -> None:
        """Verify that ``b2luigi tasks info RootTask`` includes the parameter name."""
        _, stdout, _ = self._run_cli("tasks", ["info", "RootTask"])
        self.assertIn("split", stdout)

    def test_tasks_info_all_tasks_exits_zero(self) -> None:
        """Verify that ``b2luigi tasks info`` with no argument exits with code 0."""
        returncode, stdout, stderr = self._run_cli("tasks", ["info"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_tasks_info_all_tasks_shows_all_names(self) -> None:
        """Verify that ``b2luigi tasks info`` with no argument shows every task name."""
        _, stdout, _ = self._run_cli("tasks", ["info"])
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_tasks_info_unknown_task_exits_nonzero(self) -> None:
        """Verify that ``b2luigi tasks info Nonexistent`` exits with a nonzero code."""
        returncode, stdout, stderr = self._run_cli("tasks", ["info", "Nonexistent"])
        self.assertNotEqual(returncode, 0)

    def test_tasks_info_unknown_task_shows_error(self) -> None:
        """Verify that ``b2luigi tasks info Nonexistent`` prints an error message."""
        _, stdout, stderr = self._run_cli("tasks", ["info", "Nonexistent"])
        self.assertIn("Unknown task", stdout + stderr)

    def test_tasks_missing_tasks_file(self) -> None:
        """Verify that a missing tasks.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("tasks")
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))

    def test_tasks_empty_tasks_file_exits_nonzero(self) -> None:
        """Verify that a tasks.py with no b2luigi task classes produces a helpful error."""
        empty_tasks = os.path.join(self.tmp_dir, "tasks.py")
        with open(empty_tasks, "w", encoding="utf-8") as f:
            f.write("# no tasks here\n")
        returncode, stdout, stderr = self._run_cli("tasks")
        self.assertNotEqual(returncode, 0)
        self.assertTrue("task" in (stdout + stderr).lower())

    def test_tasks_info_empty_tasks_file_exits_nonzero(self) -> None:
        """Verify that tasks info with no b2luigi task classes produces a helpful error."""
        empty_tasks = os.path.join(self.tmp_dir, "tasks.py")
        with open(empty_tasks, "w", encoding="utf-8") as f:
            f.write("# no tasks here\n")
        returncode, stdout, stderr = self._run_cli("tasks", ["info"])
        self.assertNotEqual(returncode, 0)
        self.assertTrue("task" in (stdout + stderr).lower())

    def test_tasks_custom_task_file(self) -> None:
        """Verify that --task-file points tasks to a non-default filename."""
        import shutil

        alt_path = os.path.join(self.tmp_dir, "my_tasks.py")
        shutil.copy(os.path.join(self.tmp_dir, "tasks.py"), alt_path)
        returncode, stdout, stderr = self._run_cli("tasks", ["-f", alt_path])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
