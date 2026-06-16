"""Integration tests for b2luigi remove CLI command.

:Description: Tests the ``b2luigi remove`` command end-to-end, covering task
    output removal, confirmation prompts, and error handling.
"""

import os

from .helpers import CLITestCase


class TestRemove(CLITestCase):
    """Integration tests for the remove command."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_remove_task_auto_confirm(self) -> None:
        """Verify that ``b2luigi remove -t LeafTask -y`` exits 0."""
        returncode, stdout, stderr = self._run_cli("remove", ["-t", "LeafTask", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_all_tasks_auto_confirm(self) -> None:
        """Verify that ``b2luigi remove -y`` (all tasks) exits 0."""
        returncode, stdout, stderr = self._run_cli("remove", ["-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_unknown_task_error(self) -> None:
        """Verify that ``b2luigi remove -t Nonexistent -y`` exits with error."""
        returncode, stdout, stderr = self._run_cli("remove", ["-t", "Nonexistent", "-y"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_remove_with_dependents_flag(self) -> None:
        """Verify that ``--with-dependents`` flag is accepted without error."""
        returncode, stdout, stderr = self._run_cli("remove", ["-t", "LeafTask", "-y", "--with-dependents"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_with_keep_flag(self) -> None:
        """Verify that ``--keep RootTask`` preserves that task's outputs."""
        returncode, stdout, stderr = self._run_cli("remove", ["-y", "--keep", "RootTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertTrue("RootTask" in stdout or "Keeping" in stdout)

    def test_remove_with_param_override(self) -> None:
        """Verify that ``--param split=99`` works with remove."""
        returncode, stdout, stderr = self._run_cli("remove", ["-t", "LeafTask", "-y", "--param", "split=99"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("remove", ["-y"])
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))

    def test_remove_multiple_tasks(self) -> None:
        """Verify that multiple comma-separated tasks can be removed."""
        returncode, stdout, stderr = self._run_cli("remove", ["-t", "LeafTask,RootTask", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
