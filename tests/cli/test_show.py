"""Integration tests for b2luigi show CLI command.

:Description: Tests the ``b2luigi show`` command end-to-end, covering task
    listing, dependency tree display, parameter overrides, and error handling.
"""

import os

from .helpers import CLITestCase


class TestShow(CLITestCase):
    """Integration tests for the show command."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_show_all_tasks(self) -> None:
        """Verify that ``b2luigi show`` with no args renders all tasks."""
        returncode, stdout, stderr = self._run_cli("show")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_show_named_task(self) -> None:
        """Verify that ``b2luigi show LeafTask`` renders that task."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertTrue("split" in stdout.lower() or "output" in stdout.lower())

    def test_show_multiple_named_tasks(self) -> None:
        """Verify that ``b2luigi show LeafTask RootTask`` renders both tasks."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "RootTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_show_unknown_task_error(self) -> None:
        """Verify that ``b2luigi show Nonexistent`` exits with error."""
        returncode, stdout, stderr = self._run_cli("show", ["Nonexistent"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_show_unknown_task_with_typo_suggestion(self) -> None:
        """Verify that error message suggests a close task name."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTsk"])
        self.assertNotEqual(returncode, 0)
        error_msg = stdout + stderr
        self.assertIn("Unknown task", error_msg)
        self.assertTrue("LeafTask" in error_msg or "Did you mean" in error_msg)

    def test_show_with_param_override(self) -> None:
        """Verify that ``--param split=99`` is accepted without error."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "--param", "split=99"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)

    def test_show_with_dependents(self) -> None:
        """Verify that ``--with-dependents`` includes the dependent task."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "--with-dependents"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_show_with_custom_task_file(self) -> None:
        """Verify that ``--task-file`` overrides the default tasks.py."""
        returncode, stdout, stderr = self._run_cli("show", ["-f", "tasks.py"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)

    def test_show_with_custom_params_file(self) -> None:
        """Verify that ``--params-file`` overrides the default parameters.py."""
        returncode, stdout, stderr = self._run_cli("show", ["-p", "parameters.py"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertTrue("LeafTask" in stdout or "RootTask" in stdout)

    def test_show_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("show")
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))

    def test_show_missing_parameters_file(self) -> None:
        """Verify that missing parameters.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "parameters.py"))
        returncode, stdout, stderr = self._run_cli("show")
        self.assertNotEqual(returncode, 0)
        self.assertTrue("parameters.py" in (stdout + stderr) or "not found" in (stdout + stderr))


class TestShowMultiParam(CLITestCase):
    """Integration tests for show with tasks that have different parameters."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_multi_project_files()

    def test_show_root_task_does_not_crash(self) -> None:
        """show ParentTask must not raise UnknownParameterException."""
        returncode, stdout, stderr = self._run_cli("show", ["ParentTask"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ParentTask", stdout)

    def test_show_all_tasks_does_not_crash(self) -> None:
        """show (no args) must not raise UnknownParameterException."""
        returncode, stdout, stderr = self._run_cli("show")
        self.assertEqual(returncode, 0, f"stderr: {stderr}")

    def test_show_non_root_task_via_discovery(self) -> None:
        """show ChildTask (no --param) discovers it via graph traversal."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ChildTask", stdout)

    def test_show_non_root_task_with_explicit_param(self) -> None:
        """show ChildTask --param child_param=6 uses direct path."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask", "--param", "child_param=6"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ChildTask", stdout)

    def test_show_direct_flag_errors_when_params_missing(self) -> None:
        """show ChildTask --direct fails with a clear error when params absent."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask", "--direct"])
        self.assertNotEqual(returncode, 0)
        combined = stdout + stderr
        self.assertIn("ChildTask", combined)
        self.assertIn("child_param", combined)

    def test_show_with_dependents_for_root_task(self) -> None:
        """show ParentTask --with-dependents works for a directly resolvable task."""
        returncode, stdout, stderr = self._run_cli("show", ["ParentTask", "--with-dependents"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
