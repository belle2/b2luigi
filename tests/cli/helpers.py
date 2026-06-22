"""Shared base class for b2luigi CLI integration tests."""

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
from unittest import TestCase


class CLITestCase(TestCase):
    """Base class for CLI integration tests.

    Provides a temporary working directory and a subprocess runner that
    invokes the installed ``b2luigi`` binary directly (resolved from
    ``sys.executable`` — no PATH dependency).
    """

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir)

    def _setup_project_files(self) -> None:
        """Copy standard task/parameter fixtures into the temp directory.

        Call this from ``setUp`` in test classes that need ``tasks.py`` and
        ``parameters.py`` to be present (e.g. run, show, remove, status).
        """
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_show_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )
        shutil.copy(
            os.path.join(test_dir, "cli_show_parameters.py"),
            os.path.join(self.tmp_dir, "parameters.py"),
        )

    def _setup_multi_project_files(self) -> None:
        """Copy multi-param task/parameter fixtures into the temp directory.

        Use in tests that need a root task (ParentTask with ``parent_param``)
        and a non-root task (ChildTask with ``child_param``) whose parameter
        is not present in parameters.py.
        """
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_multi_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )
        shutil.copy(
            os.path.join(test_dir, "cli_multi_parameters.py"),
            os.path.join(self.tmp_dir, "parameters.py"),
        )

    def _run_cli(
        self,
        subcmd: str,
        args: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
        exclude_env: set[str] | None = None,
    ) -> tuple[int, str, str]:
        """Run ``b2luigi <subcmd>`` and return ``(returncode, stdout, stderr)``.

        :param subcmd: The b2luigi subcommand to run (e.g. ``"status"``).
        :type subcmd: str
        :param args: Additional command-line arguments.
        :type args: list[str] | None
        :param extra_env: Environment variables to add or override.
        :type extra_env: dict[str, str] | None
        :param exclude_env: Environment variable names to strip from the
            subprocess environment (useful for testing ``(unset)`` fallbacks).
        :type exclude_env: set[str] | None
        :returns: Tuple of (return code, stdout, stderr).
        :rtype: tuple[int, str, str]
        """
        b2luigi_bin = str(pathlib.Path(sys.executable).parent / "b2luigi")
        cmd = [b2luigi_bin, subcmd] + (args or [])
        env = {k: v for k, v in os.environ.items() if k not in (exclude_env or set())}
        env["NO_COLOR"] = "1"
        if extra_env:
            env.update(extra_env)
        result = subprocess.run(cmd, cwd=self.tmp_dir, capture_output=True, text=True, env=env)
        return result.returncode, result.stdout, result.stderr
