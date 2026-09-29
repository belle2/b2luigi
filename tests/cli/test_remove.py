"""Integration tests for b2luigi remove CLI command.

:Description: Tests the ``b2luigi remove`` command end-to-end, covering task
    output removal, confirmation prompts, and error handling.
"""

import json
import os
import shutil

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

    def test_remove_with_requirements_and_keep(self) -> None:
        """remove RootTask --with-requirements --keep LeafTask -y filters LeafTask out before removal.

        ``cli_show_tasks.py``'s ``LeafTask``/``RootTask`` define ``output()`` directly, so the default
        ``remove_output()`` finds no ``add_to_output`` targets and no file is removed either way. This
        test's point is narrower: verify ``--keep`` is honored (the "Keeping" message appears and
        LeafTask is never attempted) rather than silently ignored, which was the bug (``--keep``
        combined with ``--with-requirements`` was previously a silent no-op). The on-disk effect is
        pinned by ``TestRemoveDefaultImplementation``.
        """
        returncode, stdout, stderr = self._run_cli(
            "remove", ["RootTask", "--with-requirements", "--keep", "LeafTask", "-y"]
        )
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        combined = stdout + stderr
        self.assertIn("Keeping LeafTask outputs", combined)
        self.assertIn("RootTask", combined)
        self.assertNotIn("Removing... LeafTask", combined)


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


class TestRemoveNonRootTaskCorrectness(CLITestCase):
    """Verify that remove only targets the named non-root task, not its parent."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_multi_project_files()

    def test_remove_non_root_task_does_not_mention_parent(self) -> None:
        """remove ChildTask -y should reference ChildTask but not ParentTask."""
        returncode, stdout, stderr = self._run_cli("remove", ["ChildTask", "-y"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        combined = stdout + stderr
        self.assertIn("ChildTask", combined)
        self.assertNotIn("ParentTask", combined)

    def test_remove_non_root_task_with_requirements_does_not_mention_parent(self) -> None:
        """remove ChildTask --with-requirements -y should not reference ParentTask."""
        returncode, stdout, stderr = self._run_cli("remove", ["ChildTask", "--with-requirements", "-y"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        combined = stdout + stderr
        self.assertIn("ChildTask", combined)
        self.assertNotIn("ParentTask", combined)


class TestRemoveDefaultImplementation(CLITestCase):
    """``b2luigi remove`` deletes ``add_to_output`` targets of tasks that override nothing.

    Pins the default :meth:`b2luigi.Task.remove_output`: the fixture tasks in
    ``cli_default_remove_tasks.py`` declare their outputs via ``add_to_output``
    and do **not** define ``remove_output`` themselves.
    """

    def setUp(self) -> None:
        super().setUp()
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_default_remove_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )
        shutil.copy(
            os.path.join(test_dir, "cli_show_parameters.py"),
            os.path.join(self.tmp_dir, "parameters.py"),
        )
        with open(os.path.join(self.tmp_dir, "settings.json"), "w") as f:
            json.dump({"result_dir": "results"}, f)
        # Output layout is result_dir/param=value/filename, no task-class component.
        output_dir = os.path.join(self.tmp_dir, "results", "split=1")
        os.makedirs(output_dir)
        self.root_file = os.path.join(output_dir, "root.txt")
        self.leaf_file = os.path.join(output_dir, "leaf.txt")
        for path in (self.root_file, self.leaf_file):
            with open(path, "w") as f:
                f.write("x")

    def test_remove_deletes_outputs_without_user_override(self) -> None:
        """remove RootTask --with-requirements -y deletes both files via the default remove_output."""
        returncode, stdout, stderr = self._run_cli("remove", ["RootTask", "--with-requirements", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertNotIn("Skipping", stdout + stderr)
        self.assertFalse(os.path.exists(self.root_file))
        self.assertFalse(os.path.exists(self.leaf_file))

    def test_remove_keep_preserves_kept_task_files(self) -> None:
        """--keep LeafTask leaves LeafTask's file on disk while RootTask's is deleted."""
        returncode, stdout, stderr = self._run_cli(
            "remove", ["RootTask", "--with-requirements", "--keep", "LeafTask", "-y"]
        )
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("Keeping LeafTask outputs", stdout + stderr)
        self.assertFalse(os.path.exists(self.root_file))
        self.assertTrue(os.path.exists(self.leaf_file))
