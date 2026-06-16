"""Integration tests for b2luigi run CLI command.

:Description: Tests the `b2luigi run` command end-to-end, covering task execution,
    listing, help display, and error handling.
"""

import os
import subprocess
import sys
import tempfile
import shutil
from typing import Tuple

import pytest


class TestRun:
    """Integration tests for the run command.

    :Description: Tests the run command by running it as a subprocess with
        minimal task files and verifying output and exit codes.
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

    def _run_command(
        self,
        tmp_dir: str,
        args: list | None = None,
    ) -> Tuple[int, str, str]:
        """Run `b2luigi run` command as a subprocess.

        :param tmp_dir: Working directory for the command.
        :type tmp_dir: str
        :param args: Additional command-line arguments.
        :type args: list | None
        :returns: Tuple of (return code, stdout, stderr).
        :rtype: Tuple[int, str, str]
        """
        if args is None:
            args = []

        cmd = [sys.executable, "-m", "b2luigi", "run"] + args
        env = {**os.environ, "NO_COLOR": "1"}
        result = subprocess.run(
            cmd,
            cwd=tmp_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        return result.returncode, result.stdout, result.stderr

    def test_run_list(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi run list` exits 0 and shows available tasks."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_command(tmp_dir, ["list"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout, "LeafTask should be listed"
        assert "RootTask" in stdout, "RootTask should be listed"

    def test_run_help_leaftask(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi run help LeafTask` shows task docstring."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_command(tmp_dir, ["help", "LeafTask"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout
        # Docstring contains "simple leaf task"
        assert "leaf" in stdout.lower() or "simple" in stdout.lower()

    def test_run_help_shows_params(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that task help output contains parameter names."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_command(tmp_dir, ["help", "RootTask"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        # RootTask has a "split" parameter; check it appears in output
        assert "RootTask" in stdout
        assert "split" in stdout or "parameter" in stdout.lower()

    def test_run_help_unknown_task_error(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi run help Nonexistent` exits with error."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_help(tmp_dir, ["help", "Nonexistent"])

        assert returncode != 0, "Command should fail for unknown task"
        error_msg = stderr + stdout
        assert "Unknown task" in error_msg

    def _run_help(self, tmp_dir: str, args: list | None = None) -> Tuple[int, str, str]:
        """Helper to run help command."""
        if args is None:
            args = []
        cmd = [sys.executable, "-m", "b2luigi", "run"] + args
        env = {**os.environ, "NO_COLOR": "1"}
        result = subprocess.run(
            cmd,
            cwd=tmp_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        return result.returncode, result.stdout, result.stderr

    def test_run_dry_run(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi run -t LeafTask --dry` exits 0."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_command(tmp_dir, ["-t", "LeafTask", "--dry"])

        # dry_run exits with 0 if tasks are not finished; may also show unfinished tasks
        assert returncode == 0 or returncode == 256, f"Unexpected exit code: {returncode}, stderr: {stderr}"

    def test_run_unknown_task_error(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi run -t Nonexistent` exits with error."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_command(tmp_dir, ["-t", "Nonexistent"])

        assert returncode != 0, "Command should fail for unknown task"
        error_msg = stderr + stdout
        assert "Unknown task" in error_msg

    def test_run_with_param_override(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi run -t LeafTask --param split=99 --dry` works."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_command(tmp_dir, ["-t", "LeafTask", "--param", "split=99", "--dry"])

        # Should not fail due to parameter parsing
        assert returncode == 0 or returncode == 256, f"Unexpected exit code: {returncode}"

    def test_run_with_custom_task_file(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that --task-file flag works."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_command(tmp_dir, ["list", "-f", "tasks.py"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout

    def test_run_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            returncode, stdout, stderr = self._run_command(tmp_dir, ["list"])

            assert returncode != 0
            error_msg = stderr + stdout
            assert "tasks.py" in error_msg or "not found" in error_msg
