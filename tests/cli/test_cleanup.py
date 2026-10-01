"""Integration tests for the ``b2luigi cleanup`` CLI command.

:Description: ``cleanup`` removes the per-task log and task-file directories via
    :meth:`b2luigi.Task.remove_log_files` / :meth:`b2luigi.Task.remove_task_files`,
    with the same task selection as ``remove``. The fixture tasks in
    ``cli_default_remove_tasks.py`` override nothing, so the defaults are exercised.
"""

import json
import os
import shutil

from .helpers import CLITestCase


class TestCleanup(CLITestCase):
    """End-to-end tests for ``b2luigi cleanup``."""

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
            json.dump({"result_dir": "results", "log_dir": "logs", "task_file_dir": "task_files"}, f)

        # Layout: <base>/split=1/<Family>/ for logs and task files; results have no family component.
        self.dirs = {}
        for base in ("logs", "task_files"):
            for family in ("RootTask", "LeafTask"):
                path = os.path.join(self.tmp_dir, base, "split=1", family)
                os.makedirs(path)
                with open(os.path.join(path, "stdout"), "w") as f:
                    f.write("x")
                self.dirs[(base, family)] = path
        self.result_file = os.path.join(self.tmp_dir, "results", "split=1", "root.txt")
        os.makedirs(os.path.dirname(self.result_file))
        with open(self.result_file, "w") as f:
            f.write("x")

    def _exists(self, base: str, family: str) -> bool:
        return os.path.exists(self.dirs[(base, family)])

    def test_cleanup_removes_logs_and_task_files_of_tree(self) -> None:
        """cleanup RootTask --with-requirements -y removes both directories of both tasks."""
        returncode, stdout, stderr = self._run_cli("cleanup", ["RootTask", "--with-requirements", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        for key in self.dirs:
            self.assertFalse(self._exists(*key), f"{key} should have been removed")
        self.assertTrue(os.path.exists(self.result_file), "results must never be touched")
        self.assertIn("log and task files", stdout + stderr)

    def test_cleanup_named_task_only(self) -> None:
        """cleanup RootTask -y (no --with-requirements) leaves LeafTask's directories alone."""
        returncode, stdout, stderr = self._run_cli("cleanup", ["RootTask", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertFalse(self._exists("logs", "RootTask"))
        self.assertFalse(self._exists("task_files", "RootTask"))
        self.assertTrue(self._exists("logs", "LeafTask"))
        self.assertTrue(self._exists("task_files", "LeafTask"))

    def test_logs_only_keeps_task_files(self) -> None:
        """--logs-only removes the log directories and leaves the task-file directories."""
        returncode, stdout, stderr = self._run_cli("cleanup", ["RootTask", "--with-requirements", "--logs-only", "-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertFalse(self._exists("logs", "RootTask"))
        self.assertFalse(self._exists("logs", "LeafTask"))
        self.assertTrue(self._exists("task_files", "RootTask"))
        self.assertTrue(self._exists("task_files", "LeafTask"))

    def test_task_files_only_keeps_logs(self) -> None:
        """--task-files-only removes the task-file directories and leaves the logs."""
        returncode, stdout, stderr = self._run_cli(
            "cleanup", ["RootTask", "--with-requirements", "--task-files-only", "-y"]
        )
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertTrue(self._exists("logs", "RootTask"))
        self.assertTrue(self._exists("logs", "LeafTask"))
        self.assertFalse(self._exists("task_files", "RootTask"))
        self.assertFalse(self._exists("task_files", "LeafTask"))

    def test_both_only_flags_is_an_error(self) -> None:
        """--logs-only together with --task-files-only exits non-zero and removes nothing."""
        returncode, stdout, stderr = self._run_cli("cleanup", ["RootTask", "--logs-only", "--task-files-only", "-y"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("--logs-only", stdout + stderr)
        for key in self.dirs:
            self.assertTrue(self._exists(*key))

    def test_keep_preserves_kept_task(self) -> None:
        """--keep LeafTask leaves LeafTask's directories while RootTask's are removed."""
        returncode, stdout, stderr = self._run_cli(
            "cleanup", ["RootTask", "--with-requirements", "--keep", "LeafTask", "-y"]
        )
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("Keeping LeafTask", stdout + stderr)
        self.assertFalse(self._exists("logs", "RootTask"))
        self.assertFalse(self._exists("task_files", "RootTask"))
        self.assertTrue(self._exists("logs", "LeafTask"))
        self.assertTrue(self._exists("task_files", "LeafTask"))

    def test_cleanup_all_tasks(self) -> None:
        """cleanup -y without names covers every project task."""
        returncode, stdout, stderr = self._run_cli("cleanup", ["-y"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        for key in self.dirs:
            self.assertFalse(self._exists(*key), f"{key} should have been removed")
