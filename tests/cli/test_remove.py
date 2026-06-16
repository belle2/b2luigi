"""Integration tests for b2luigi remove CLI command.

:Description: Tests the `b2luigi remove` command end-to-end, covering task output
    removal, confirmation prompts, and error handling.
"""

import os
import subprocess
import sys
import tempfile
import shutil
from typing import Tuple

import pytest


class TestRemove:
    """Integration tests for the remove command.

    :Description: Tests the remove command by running it as a subprocess with
        minimal task files and verifying exit codes and output.
    """

    @pytest.fixture
    def tmp_project(self) -> Tuple[str, str, str]:
        """Create a temporary directory with tasks.py and parameters.py.

        :returns: Tuple of (temp_dir, tasks_file_path, parameters_file_path).
        :rtype: Tuple[str, str, str]
        """
        tmp_dir = tempfile.mkdtemp()
        tasks_file = os.path.join(tmp_dir, "tasks.py")
        params_file = os.path.join(tmp_dir, "parameters.py")

        # Copy test task files
        test_dir = os.path.dirname(__file__)
        shutil.copy(os.path.join(test_dir, "cli_show_tasks.py"), tasks_file)
        shutil.copy(os.path.join(test_dir, "cli_show_parameters.py"), params_file)

        yield tmp_dir, tasks_file, params_file

        # Cleanup
        shutil.rmtree(tmp_dir)

    def _run_remove(
        self,
        tmp_dir: str,
        args: list | None = None,
    ) -> Tuple[int, str, str]:
        """Run `b2luigi remove` command as a subprocess.

        :param tmp_dir: Working directory for the command.
        :type tmp_dir: str
        :param args: Additional command-line arguments.
        :type args: list | None
        :returns: Tuple of (return code, stdout, stderr).
        :rtype: Tuple[int, str, str]
        """
        if args is None:
            args = []

        cmd = [sys.executable, "-m", "b2luigi", "remove"] + args
        env = {**os.environ, "NO_COLOR": "1"}
        result = subprocess.run(
            cmd,
            cwd=tmp_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        return result.returncode, result.stdout, result.stderr

    def test_remove_task_auto_confirm(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi remove -t LeafTask -y` exits 0."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_remove(tmp_dir, ["-t", "LeafTask", "-y"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"

    def test_remove_all_tasks_auto_confirm(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi remove -y` (all tasks) exits 0."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_remove(tmp_dir, ["-y"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"

    def test_remove_unknown_task_error(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi remove -t Nonexistent -y` exits with error."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_remove(tmp_dir, ["-t", "Nonexistent", "-y"])

        assert returncode != 0, "Command should fail for unknown task"
        error_msg = stderr + stdout
        assert "Unknown task" in error_msg

    def test_remove_with_dependents_flag(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that --with-dependents flag is accepted without error."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_remove(tmp_dir, ["-t", "LeafTask", "-y", "--with-dependents"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"

    def test_remove_with_keep_flag(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that --keep flag preserves specified task outputs."""
        tmp_dir, _, _ = tmp_project
        # Remove all but keep RootTask
        returncode, stdout, stderr = self._run_remove(tmp_dir, ["-y", "--keep", "RootTask"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        # Should indicate that RootTask is being kept
        assert "RootTask" in stdout or "Keeping" in stdout

    def test_remove_with_param_override(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that --param overrides work with remove."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_remove(tmp_dir, ["-t", "LeafTask", "-y", "--param", "split=99"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"

    def test_remove_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            returncode, stdout, stderr = self._run_remove(tmp_dir, ["-y"])

            assert returncode != 0
            error_msg = stderr + stdout
            assert "tasks.py" in error_msg or "not found" in error_msg

    def test_remove_multiple_tasks(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that multiple comma-separated tasks can be removed."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_remove(tmp_dir, ["-t", "LeafTask,RootTask", "-y"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
