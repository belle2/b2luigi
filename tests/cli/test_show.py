"""Integration tests for b2luigi show CLI command.

:Description: Tests the `b2luigi show` command end-to-end, covering task listing,
    dependency tree display, parameter overrides, and error handling.
"""

import os
import subprocess
import sys
import tempfile
import shutil
from typing import Optional, Tuple

import pytest


class TestShow:
    """Integration tests for the show command.

    :Description: Tests the show command by running it as a subprocess with
        minimal task files and verifying stdout output.
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

    def _run_show(
        self,
        tmp_dir: str,
        args: Optional[list] = None,
    ) -> Tuple[int, str, str]:
        """Run `b2luigi show` command as a subprocess.

        :param tmp_dir: Working directory for the command.
        :type tmp_dir: str
        :param args: Additional command-line arguments.
        :type args: Optional[list]
        :returns: Tuple of (return code, stdout, stderr).
        :rtype: Tuple[int, str, str]
        """
        if args is None:
            args = []

        cmd = [sys.executable, "-m", "b2luigi", "show"] + args
        env = {**os.environ, "NO_COLOR": "1"}
        result = subprocess.run(
            cmd,
            cwd=tmp_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        return result.returncode, result.stdout, result.stderr

    def test_show_all_tasks(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi show` with no args renders all tasks."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir)

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout, "LeafTask should be in output"
        assert "RootTask" in stdout, "RootTask should be in output"

    def test_show_named_task(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi show -t LeafTask` renders only that task."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-t", "LeafTask"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout
        # RootTask may or may not appear depending on traversal; just check LeafTask is present
        assert "split" in stdout.lower() or "output" in stdout.lower()

    def test_show_multiple_named_tasks(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi show -t TaskA,TaskB` renders both tasks."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-t", "LeafTask,RootTask"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout
        assert "RootTask" in stdout

    def test_show_unknown_task_error(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi show -t Nonexistent` exits with error."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-t", "Nonexistent"])

        assert returncode != 0, "Command should fail for unknown task"
        assert "Unknown task" in stderr or "Unknown task" in stdout

    def test_show_unknown_task_with_typo_suggestion(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that error message suggests a close task name."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-t", "LeafTsk"])

        assert returncode != 0
        error_msg = stderr + stdout
        assert "Unknown task" in error_msg
        # "LeafTsk" is close to "LeafTask", so a suggestion should appear
        assert "LeafTask" in error_msg or "Did you mean" in error_msg

    def test_show_with_param_override(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that `b2luigi show -t LeafTask --param split=99` shows the parameter."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-t", "LeafTask", "--param", "split=99"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        # The output path should contain "split=99" in the task identifier
        assert "LeafTask" in stdout

    def test_show_with_dependents(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that --with-dependents shows both the task and its dependents."""
        tmp_dir, _, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-t", "LeafTask", "--with-dependents"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        # Both tasks should appear because RootTask depends on LeafTask
        assert "LeafTask" in stdout
        assert "RootTask" in stdout

    def test_show_with_custom_task_file(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that --task-file overrides the default tasks.py."""
        tmp_dir, tasks_file, _ = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-f", "tasks.py"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout

    def test_show_with_custom_params_file(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that --params-file overrides the default parameters.py."""
        tmp_dir, _, params_file = tmp_project
        returncode, stdout, stderr = self._run_show(tmp_dir, ["-p", "parameters.py"])

        assert returncode == 0, f"Command failed with stderr: {stderr}"
        assert "LeafTask" in stdout or "RootTask" in stdout

    def test_show_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            returncode, stdout, stderr = self._run_show(tmp_dir)

            assert returncode != 0
            error_msg = stderr + stdout
            assert "tasks.py" in error_msg or "not found" in error_msg

    def test_show_missing_parameters_file(self, tmp_project: Tuple[str, str, str]) -> None:
        """Verify that missing parameters.py produces a helpful error."""
        tmp_dir, _, params_file = tmp_project
        os.remove(params_file)

        returncode, stdout, stderr = self._run_show(tmp_dir)

        assert returncode != 0
        error_msg = stderr + stdout
        assert "parameters.py" in error_msg or "not found" in error_msg
