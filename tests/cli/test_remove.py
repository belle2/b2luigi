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
        """Verify that ``b2luigi remove LeafTask -y`` exits 0."""
        returncode, stdout, stderr = self._run_cli("remove", ["LeafTask", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_all_tasks_auto_confirm(self) -> None:
        """Verify that ``b2luigi remove -y`` (all tasks) exits 0."""
        returncode, stdout, stderr = self._run_cli("remove", ["-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_unknown_task_error(self) -> None:
        """Verify that ``b2luigi remove Nonexistent -y`` exits with error."""
        returncode, stdout, stderr = self._run_cli("remove", ["Nonexistent", "-y"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_remove_with_keep_flag(self) -> None:
        """Verify that ``--keep RootTask`` preserves that task's outputs."""
        returncode, stdout, stderr = self._run_cli("remove", ["-y", "--keep", "RootTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertTrue("RootTask" in stdout or "Keeping" in stdout)

    def test_remove_with_param_override(self) -> None:
        """Verify that ``--param split=99`` works with remove."""
        returncode, stdout, stderr = self._run_cli("remove", ["LeafTask", "-y", "--param", "split=99"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("remove", ["-y"])
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))

    def test_remove_multiple_tasks(self) -> None:
        """Verify that multiple tasks can be removed as separate positional args."""
        returncode, stdout, stderr = self._run_cli("remove", ["LeafTask", "RootTask", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_with_requirements_leaf_task(self) -> None:
        """remove LeafTask --with-requirements -y removes only LeafTask (leaf, no requirements)."""
        returncode, stdout, stderr = self._run_cli("remove", ["LeafTask", "--with-requirements", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_remove_with_requirements_root_task(self) -> None:
        """remove RootTask --with-requirements -y removes RootTask AND LeafTask."""
        returncode, stdout, stderr = self._run_cli("remove", ["RootTask", "--with-requirements", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        combined = stdout + stderr
        self.assertIn("RootTask", combined)
        self.assertIn("LeafTask", combined)


class TestRemoveMultiParam(CLITestCase):
    """Integration tests for remove with tasks that have different parameters."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_multi_project_files()

    def test_remove_root_task_does_not_crash(self) -> None:
        """remove ParentTask must not raise UnknownParameterException."""
        returncode, stdout, stderr = self._run_cli("remove", ["ParentTask", "-y"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")

    def test_remove_non_root_task_via_discovery(self) -> None:
        """remove ChildTask (no --param) discovers it via graph traversal."""
        returncode, stdout, stderr = self._run_cli("remove", ["ChildTask", "-y"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")

    def test_remove_non_root_task_with_explicit_param(self) -> None:
        """remove ChildTask --param child_param=6 uses direct path."""
        returncode, stdout, stderr = self._run_cli("remove", ["ChildTask", "-y", "--param", "child_param=6"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")

    def test_remove_direct_flag_errors_when_params_missing(self) -> None:
        """remove ChildTask --direct fails with a clear error when params absent."""
        returncode, stdout, stderr = self._run_cli("remove", ["ChildTask", "-y", "--direct"])
        self.assertNotEqual(returncode, 0)
        combined = stdout + stderr
        self.assertIn("ChildTask", combined)
        self.assertIn("child_param", combined)
