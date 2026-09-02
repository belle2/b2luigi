"""Integration tests for b2luigi run CLI command.

:Description: Tests the ``b2luigi run`` command end-to-end, covering task
    execution and error handling via a positional task name argument.
"""

import json
import os
import pathlib
import shutil
from unittest import TestCase
from unittest.mock import patch

from b2luigi.cli.runner import run_luigi
from b2luigi.core.settings import set_setting, with_new_settings

from .helpers import CLITestCase


class TestRun(CLITestCase):
    """Integration tests for the run command."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_run_dry_run(self) -> None:
        """Verify that ``b2luigi run LeafTask --dry`` exits 0 or 256."""
        returncode, stdout, stderr = self._run_cli("run", ["LeafTask", "--dry"])
        self.assertIn(returncode, (0, 256), f"Unexpected exit code: {returncode}, stderr: {stderr}")

    def test_run_unknown_task_error(self) -> None:
        """Verify that ``b2luigi run Nonexistent`` exits with a nonzero code."""
        returncode, stdout, stderr = self._run_cli("run", ["Nonexistent"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_run_no_argument_exits_nonzero(self) -> None:
        """Verify that ``b2luigi run`` with no task name exits nonzero."""
        returncode, stdout, stderr = self._run_cli("run", [])
        self.assertNotEqual(returncode, 0)

    def test_run_with_param_override(self) -> None:
        """Verify that ``--param split=99 --dry`` works without parameter errors."""
        returncode, stdout, stderr = self._run_cli("run", ["LeafTask", "--param", "split=99", "--dry"])
        self.assertIn(returncode, (0, 256), f"Unexpected exit code: {returncode}")

    def test_run_missing_tasks_file(self) -> None:
        """Verify that a missing tasks.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("run", ["LeafTask"])
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))


class TestRunWithoutParametersFile(CLITestCase):
    """Tests for running tasks when parameters.py is absent."""

    def setUp(self) -> None:
        super().setUp()
        # Copy only tasks.py — deliberately NO parameters.py
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_show_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )

    def test_run_with_param_flag_no_parameters_file(self) -> None:
        """--param split=5 --dry should succeed without parameters.py."""
        returncode, stdout, stderr = self._run_cli("run", ["LeafTask", "--param", "split=5", "--dry"])
        self.assertIn(returncode, (0, 256), f"Expected 0 or 256, got {returncode}. stderr: {stderr}")

    def test_run_dry_no_parameters_file_no_params(self) -> None:
        """--dry alone without parameters.py should not crash with a file-not-found error."""
        returncode, stdout, stderr = self._run_cli("run", ["LeafTask", "--dry"])
        # May fail due to missing parameter (split is required) but must NOT
        # fail with "parameters.py not found"
        self.assertNotIn("parameters.py' not found", stdout + stderr)


class TestRunLuigiWorkersSetting(TestCase):
    """Unit tests for run_luigi's workers-setting fallback."""

    def test_workers_setting_used_as_default(self) -> None:
        """A globally-set 'workers' setting is forwarded to luigi.build when no explicit kwarg is given."""
        with with_new_settings():
            set_setting("workers", 5)
            with patch("b2luigi.cli.runner.luigi.build", return_value=True) as mock_build:
                run_luigi([], {})
            self.assertEqual(mock_build.call_args.kwargs["workers"], 5)

    def test_explicit_workers_kwarg_overrides_setting(self) -> None:
        """An explicit workers= kwarg takes precedence over the 'workers' setting."""
        with with_new_settings():
            set_setting("workers", 5)
            with patch("b2luigi.cli.runner.luigi.build", return_value=True) as mock_build:
                run_luigi([], {"workers": 9})
            self.assertEqual(mock_build.call_args.kwargs["workers"], 9)

    def test_default_workers_is_one_without_setting(self) -> None:
        """With no setting and no explicit kwarg, luigi.build still receives workers=1 explicitly."""
        with with_new_settings():
            with patch("b2luigi.cli.runner.luigi.build", return_value=True) as mock_build:
                run_luigi([], {})
            self.assertEqual(mock_build.call_args.kwargs["workers"], 1)


class TestRunWorkersFlag(CLITestCase):
    """Integration test for the --workers CLI flag."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_run_with_workers_flag(self) -> None:
        """Verify that `b2luigi run LeafTask --workers 2 --dry` is accepted without error."""
        returncode, stdout, stderr = self._run_cli("run", ["LeafTask", "--workers", "2", "--dry"])
        self.assertIn(returncode, (0, 256), f"Unexpected exit code: {returncode}, stderr: {stderr}")


class TestRunBatchWithParameterGenerator(CLITestCase):
    """Integration test for `run --batch` when parameters.py expands via ParameterGenerator.

    Multiple combinations make ``run_task`` wrap the real task in a dynamically-created
    ``WrapperTask`` (via ``_make_wrapper_task``). That wrapper is never importable by
    name from ``tasks.py``, so it must always execute in-process even under --batch,
    or the batch worker crashes trying to reconstruct it by classname.
    """

    def setUp(self) -> None:
        super().setUp()
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_generator_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )
        pathlib.Path(self.tmp_dir, "parameters.py").write_text(
            "from b2luigi import ParameterGenerator\n" "config = {'value': ParameterGenerator([1, 2])}\n"
        )
        pathlib.Path(self.tmp_dir, "settings.json").write_text('{"batch_system": "test"}')

    def test_wrapper_task_runs_in_process_not_as_batch_job(self) -> None:
        """The dynamic WrapperTask must not be submitted as its own batch job.

        Note: the CLI's exit code is not a reliable signal here — `run --batch`
        does not currently propagate luigi build failures to the process exit
        code (a separate, pre-existing gap, out of scope for this test). The
        bug under test instead surfaces as a "Failed task ...Wrapper" block in
        stdout and a missing output file.
        """
        returncode, stdout, stderr = self._run_cli("run", ["SimpleTask", "--batch"])
        combined = stdout + stderr
        self.assertNotIn("not found in", combined)
        self.assertNotIn("Failed task SimpleTaskWrapper", combined)
        self.assertIn("looks :)", combined)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "output_1.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "output_2.txt")))


class TestRunBatchWithApptainerImage(CLITestCase):
    """``apptainer_image`` must not drag the dynamic WrapperTask into a container job.

    ``_make_wrapper_task`` pins ``batch_system = "local"`` so the wrapper — which
    exists only as a ``type()``-created class and is importable from nowhere —
    always runs in-process. But ``workers.py`` diverts the *local* branch to
    ``ApptainerProcess`` whenever ``apptainer_image`` is set, which defeats that
    pin and submits the wrapper after all. Reconstruction then fails, because a
    ``type()``-created luigi task reports ``__module__ == "abc"`` (luigi's
    ``Register`` metaclass extends ``abc.ABCMeta``, so ``type.__new__`` reads the
    module from ``abc``'s frame) and the worker resolves ``abc.SimpleTaskWrapper``
    against the real :mod:`abc`.
    """

    def setUp(self) -> None:
        super().setUp()
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_generator_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )
        pathlib.Path(self.tmp_dir, "parameters.py").write_text(
            "from b2luigi import ParameterGenerator\nconfig = {'value': ParameterGenerator([1, 2])}\n"
        )
        # Stand in for the container runtime: log the argv, then run the payload
        # on the host. Enough to exercise the dispatch decision without Apptainer,
        # which is Linux-only and cannot be installed on every dev machine.
        fake_bin = pathlib.Path(self.tmp_dir, "fake-apptainer")
        fake_bin.write_text('#!/bin/bash\nwhile [ "$1" != "/bin/bash" ]; do shift; done\nexec "$@"\n')
        fake_bin.chmod(0o755)
        pathlib.Path(self.tmp_dir, "env.sh").write_text("#!/bin/bash\n")
        pathlib.Path(self.tmp_dir, "settings.json").write_text(
            json.dumps(
                {
                    "batch_system": "local",
                    "apptainer_image": os.path.join(self.tmp_dir, "fake.sif"),
                    "apptainer_cmd": str(fake_bin),
                    "apptainer_mounts": [],
                    "env_script": os.path.join(self.tmp_dir, "env.sh"),
                }
            )
        )

    def test_wrapper_task_is_not_submitted_as_a_container_job(self) -> None:
        """The wrapper must stay in-process even with an Apptainer image configured."""
        _returncode, stdout, stderr = self._run_cli("run", ["SimpleTask", "--batch"])
        combined = stdout + stderr
        self.assertNotIn("abc.SimpleTaskWrapper", combined)
        self.assertNotIn("Failed task SimpleTaskWrapper", combined)
        self.assertIn("looks :)", combined)
