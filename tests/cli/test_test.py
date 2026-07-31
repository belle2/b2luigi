"""Integration tests for the b2luigi test subcommand."""

import os
import pathlib
import shutil
import tempfile
from unittest import TestCase

import b2luigi

from b2luigi.cli.runner import _build_fast_task, _build_fast_req_task, test_task
from b2luigi.core.executable import create_executable_wrapper
from b2luigi.core.settings import get_setting, set_setting, with_new_settings
from b2luigi.core.utils import create_cmd_from_task
from tests.cli.helpers import CLITestCase


FIXTURE_DIR = os.path.dirname(__file__)


class TestTestNormalMode(CLITestCase):
    """Tests for normal (non-force, non-batch) subprocess execution."""

    def setUp(self) -> None:
        super().setUp()
        for fixture in ("cli_test_script.py", "cli_test_script_failing.py", "cli_test_script_silent.py"):
            shutil.copy(os.path.join(FIXTURE_DIR, fixture), os.path.join(self.tmp_dir, fixture))

    def test_normal_run_succeeds(self) -> None:
        """Script that writes its output file should exit 0."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)

    def test_nonzero_exit_propagated(self) -> None:
        """Script that exits non-zero should make the CLI exit non-zero."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script_failing.py", "-o", "result.txt"])
        self.assertNotEqual(rc, 0)

    def test_missing_output_detected(self) -> None:
        """Script that exits 0 without writing output should make the CLI exit non-zero."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script_silent.py", "-o", "result.txt"])
        self.assertNotEqual(rc, 0)


class TestTestForceFlag(CLITestCase):
    """Tests for --force flag behaviour."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_counter.py"),
            os.path.join(self.tmp_dir, "cli_test_script_counter.py"),
        )

    def _counter(self) -> int:
        """Read the run counter written by cli_test_script_counter.py.

        :returns: The integer value written to counter.txt, or 0 if the file
            does not exist yet.
        :rtype: int
        """
        path = os.path.join(self.tmp_dir, "counter.txt")
        return int(pathlib.Path(path).read_text()) if os.path.exists(path) else 0

    def test_normal_mode_skips_when_output_exists(self) -> None:
        """Second run without --force should be skipped by Luigi (counter stays at 1)."""
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 1)
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 1)

    def test_force_reruns_when_output_exists(self) -> None:
        """Second run with --force should re-run the script (counter reaches 2)."""
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 1)
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script_counter.py", "-o", "result.txt", "--force"])
        self.assertEqual(rc, 0, stderr)
        self.assertEqual(self._counter(), 2)


class TestTestBatchFlag(CLITestCase):
    """Tests for --batch flag behaviour."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )

    def test_batch_flag_runs_successfully(self) -> None:
        """--batch should still run the task locally via the local scheduler."""
        rc, _, stderr = self._run_cli("test", ["-s", "cli_test_script.py", "-o", "result.txt", "--batch"])
        self.assertEqual(rc, 0, stderr)


class TestTestExtraArgs(CLITestCase):
    """Tests for extra-args forwarding."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_echo_args.py"),
            os.path.join(self.tmp_dir, "cli_test_script_echo_args.py"),
        )

    def _read_output(self) -> str:
        """Read the output file written by the echo-args fixture.

        The ``-o`` argument is always ``result.txt``; b2luigi's ``result_dir``
        defaults to ``"."`` which ``map_folder`` resolves to ``cwd`` when
        invoked via the CLI binary (fallback 3 in ``get_filename()``), so
        the file lands directly in ``self.tmp_dir``.

        :returns: The text content of the output file, or an empty string if
            the file does not exist.
        :rtype: str
        """
        path = os.path.join(self.tmp_dir, "result.txt")
        if not os.path.exists(path):
            return ""
        with open(path) as fh:
            return fh.read()

    def test_extra_args_forwarded_to_script(self) -> None:
        """Args after -- should appear in the script's sys.argv."""
        rc, _, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script_echo_args.py", "-o", "result.txt", "--", "--lr", "0.01"],
        )
        self.assertEqual(rc, 0, stderr)
        argv_written = self._read_output()
        self.assertIn("--lr", argv_written)
        self.assertIn("0.01", argv_written)


class TestFastTaskCmdGeneration(TestCase):
    """Unit tests for task_cmd_additional_args injected by _build_fast_task."""

    def test_basic_flags_present(self) -> None:
        """Script and output file are encoded as --script / --output-file."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertEqual(
            FastTask.task_cmd_additional_args,
            ["--script", os.path.abspath("script.py"), "--output-file", "out.txt"],
        )

    def test_input_file_included(self) -> None:
        """--input-file is appended when input_file is not None."""
        FastTask = _build_fast_task("script.py", "out.txt", "in.txt", False, False, [])
        args = FastTask.task_cmd_additional_args
        self.assertIn("--input-file", args)
        idx = args.index("--input-file")
        self.assertEqual(args[idx + 1], "in.txt")

    def test_force_flag_included_when_true(self) -> None:
        """--force is appended when force=True."""
        FastTask = _build_fast_task("script.py", "out.txt", None, True, False, [])
        self.assertIn("--force", FastTask.task_cmd_additional_args)

    def test_force_flag_absent_when_false(self) -> None:
        """--force is not appended when force=False."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertNotIn("--force", FastTask.task_cmd_additional_args)

    def test_extra_args_encoded_as_repeated_option(self) -> None:
        """Each extra_arg becomes --extra-arg <value> in task_cmd_additional_args."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, ["--lr", "0.01"])
        args = FastTask.task_cmd_additional_args
        values = [args[i + 1] for i, a in enumerate(args) if a == "--extra-arg"]
        self.assertIn("--lr", values)
        self.assertIn("0.01", values)

    def test_script_path_resolved_to_absolute(self) -> None:
        """A relative exec_script is resolved to an absolute path in task_cmd_additional_args."""
        FastTask = _build_fast_task("relscript.py", "out.txt", None, False, False, [])
        args = FastTask.task_cmd_additional_args
        idx = args.index("--script")
        self.assertTrue(os.path.isabs(args[idx + 1]))
        self.assertEqual(args[idx + 1], os.path.abspath("relscript.py"))

    def test_env_script_set_as_class_attribute(self) -> None:
        """env_script is resolved to an absolute path and set as a class attribute."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], env_script="env.sh")
        self.assertEqual(FastTask.env_script, os.path.abspath("env.sh"))

    def test_env_script_absent_when_not_given(self) -> None:
        """No env_script attribute is set when env_script is None (default)."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertNotIn("env_script", FastTask.__dict__)

    def test_env_script_not_in_task_cmd_additional_args(self) -> None:
        """env_script is a submission-time-only concern; it must never be forwarded to the worker."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], env_script="env.sh")
        self.assertNotIn("--env-script", FastTask.task_cmd_additional_args)

    def test_literal_path_flag_included_when_true(self) -> None:
        """--literal-path is appended to task_cmd_additional_args when literal_path=True."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], literal_path=True)
        self.assertIn("--literal-path", FastTask.task_cmd_additional_args)

    def test_literal_path_flag_absent_when_false(self) -> None:
        """--literal-path is not appended when literal_path=False (default)."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertNotIn("--literal-path", FastTask.task_cmd_additional_args)


class TestTestBatchArmsCliModeSubmission(TestCase):
    """Unit tests verifying test_task(batch=True) routes through the new CLI batch-runner path."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )
        self._old_cwd = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        shutil.rmtree(self.tmp_dir)

    def test_batch_true_arms_use_cli_setting(self) -> None:
        """test_task(batch=True) sets __batch_runner_use_cli so create_cmd_from_task uses the new CLI branch."""
        with with_new_settings():
            test_task("cli_test_script.py", "result.txt", None, False, True, [])
            self.assertTrue(get_setting("__batch_runner_use_cli", default=False))

    def test_batch_submission_command_uses_script_reconstruction(self) -> None:
        """create_cmd_from_task on the FastTask built by --batch matches the batch-runner --script contract."""
        with with_new_settings():
            FastTask = _build_fast_task("cli_test_script.py", "result.txt", None, False, True, [])
            set_setting("__batch_runner_use_cli", True)
            cmd = create_cmd_from_task(FastTask())
            self.assertIn("batch-runner", cmd)
            self.assertIn("--script", cmd)
            script_idx = cmd.index("--script")
            self.assertEqual(cmd[script_idx + 1], os.path.abspath("cli_test_script.py"))
            self.assertIn("--output-file", cmd)
            output_idx = cmd.index("--output-file")
            self.assertEqual(cmd[output_idx + 1], "result.txt")


class TestTestTaskSettingsAndEnvScript(TestCase):
    """Unit tests for test_task's --setting and --env-script plumbing."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )
        self._old_cwd = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        shutil.rmtree(self.tmp_dir)

    def test_settings_applied_before_run(self) -> None:
        """Each --setting key=value is applied via set_setting() before the task runs."""
        with with_new_settings():
            test_task(
                "cli_test_script.py",
                "result.txt",
                None,
                False,
                False,
                [],
                settings=["result_dir=.", 'env={"MY_VAR": "1"}'],
            )
            self.assertEqual(get_setting("result_dir", default=None), ".")
            self.assertEqual(get_setting("env", default=None), {"MY_VAR": "1"})

    def test_env_script_forwarded_to_fast_task(self) -> None:
        """env_script is forwarded to _build_fast_task and lands on the FastTask class."""
        with with_new_settings():
            FastTask = _build_fast_task("cli_test_script.py", "result.txt", None, False, False, [], env_script="env.sh")
            self.assertEqual(FastTask.env_script, os.path.abspath("env.sh"))

    def test_no_settings_no_env_script_is_unaffected(self) -> None:
        """Omitting both settings and env_script behaves exactly as before (no regression)."""
        with with_new_settings():
            test_task("cli_test_script.py", "result.txt", None, False, False, [])
            self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))

    def test_env_script_sourced_in_executable_wrapper(self) -> None:
        """create_executable_wrapper() actually sources env_script in the generated wrapper."""
        with with_new_settings():
            env_script_path = os.path.join(self.tmp_dir, "env.sh")
            pathlib.Path(env_script_path).write_text("#!/bin/bash\n")
            FastTask = _build_fast_task(
                "cli_test_script.py", "result.txt", None, False, True, [], env_script=env_script_path
            )
            task_instance = FastTask()
            wrapper_path = create_executable_wrapper(task_instance)
            wrapper_content = pathlib.Path(wrapper_path).read_text()
            self.assertIn(f"source {os.path.abspath(env_script_path)}", wrapper_content)


class TestTestSettingFlag(CLITestCase):
    """Tests for --setting flag behaviour."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )

    def test_setting_flag_accepted(self) -> None:
        """--setting key=value is accepted and does not break a normal run."""
        rc, _, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script.py", "-o", "result.txt", "--setting", "working_dir=" + self.tmp_dir],
        )
        self.assertEqual(rc, 0, stderr)

    def test_multiple_setting_flags_accepted(self) -> None:
        """--setting can be repeated."""
        rc, _, stderr = self._run_cli(
            "test",
            [
                "-s",
                "cli_test_script.py",
                "-o",
                "result.txt",
                "--setting",
                "working_dir=" + self.tmp_dir,
                "--setting",
                "log_dir=" + self.tmp_dir,
            ],
        )
        self.assertEqual(rc, 0, stderr)

    def test_setting_result_dir_moves_output_location(self) -> None:
        """--setting result_dir=custom_out actually relocates the output file, not just accepted."""
        rc, _, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script.py", "-o", "result.txt", "--setting", "result_dir=custom_out"],
        )
        self.assertEqual(rc, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "custom_out", "result.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))


class TestTestEnvScriptFlag(CLITestCase):
    """Tests for --env-script flag behaviour."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )

    def test_env_script_without_batch_is_noop(self) -> None:
        """--env-script without --batch is accepted and has no effect on a local run."""
        rc, _, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script.py", "-o", "result.txt", "--env-script", "/nonexistent/env.sh"],
        )
        self.assertEqual(rc, 0, stderr)


class TestFastReqTaskTargetResolution(TestCase):
    """Unit tests for _build_fast_req_task's output target resolution."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self._old_cwd = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        shutil.rmtree(self.tmp_dir)

    def test_output_target_is_literal_path_not_result_dir_nested(self) -> None:
        """The FastReqTask output target must point at the literal input path, not result_dir/input.txt."""
        with with_new_settings():
            set_setting("result_dir", "results")
            FastReqTask = _build_fast_req_task("input.txt")
            outputs = list(FastReqTask().output())
            self.assertEqual(len(outputs), 1)
            target = outputs[0]["input.txt"]
            self.assertEqual(target.path, os.path.abspath("input.txt"))

    def test_is_external_task_subclass(self) -> None:
        """FastReqTask must be an ExternalTask (no-op run(), completeness from output().exists() alone)."""
        FastReqTask = _build_fast_req_task("input.txt")
        self.assertTrue(issubclass(FastReqTask, b2luigi.ExternalTask))


class TestFastTaskLiteralPathTargetResolution(TestCase):
    """Unit tests for _build_fast_task's output target resolution with literal_path."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self._old_cwd = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        shutil.rmtree(self.tmp_dir)

    def test_literal_path_bypasses_result_dir(self) -> None:
        """With literal_path=True, the output target must point at the literal path, not result_dir/out.txt."""
        with with_new_settings():
            set_setting("result_dir", "results")
            FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], literal_path=True)
            outputs = list(FastTask().output())
            self.assertEqual(len(outputs), 1)
            target = outputs[0]["out.txt"]
            self.assertEqual(target.path, os.path.abspath("out.txt"))

    def test_default_still_nests_under_result_dir(self) -> None:
        """Without literal_path (default), behavior is unchanged: output nests under result_dir."""
        with with_new_settings():
            set_setting("result_dir", "results")
            FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
            outputs = list(FastTask().output())
            target = outputs[0]["out.txt"]
            self.assertTrue(target.path.endswith(os.path.join("results", "out.txt")))
            self.assertNotEqual(target.path, os.path.abspath("out.txt"))


class TestTestInputFlag(CLITestCase):
    """Integration tests for the -i/--input flag."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_echo_args.py"),
            os.path.join(self.tmp_dir, "cli_test_script_echo_args.py"),
        )
        pathlib.Path(self.tmp_dir, "input.txt").write_text("hello from input")

    def test_input_flag_with_preexisting_file_succeeds(self) -> None:
        """A real pre-existing input file at a literal (non-result_dir) path is found and used."""
        rc, _, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script_echo_args.py", "-o", "result.txt", "-i", "input.txt"],
        )
        self.assertEqual(rc, 0, stderr)
        result_path = os.path.join(self.tmp_dir, "result.txt")
        self.assertTrue(os.path.exists(result_path))
        written = pathlib.Path(result_path).read_text()
        self.assertIn("input.txt", written)


class TestTestLiteralPathFlag(CLITestCase):
    """Integration tests for the --literal-path flag."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )

    def test_literal_path_writes_to_given_path_not_result_dir(self) -> None:
        """--literal-path with a custom result_dir must still write to the literal -o path."""
        rc, _, stderr = self._run_cli(
            "test",
            [
                "-s",
                "cli_test_script.py",
                "-o",
                "result.txt",
                "--setting",
                "result_dir=custom_out",
                "--literal-path",
            ],
        )
        self.assertEqual(rc, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.tmp_dir, "custom_out", "result.txt")))

    def test_default_without_flag_still_nests_under_result_dir(self) -> None:
        """Without --literal-path, --setting result_dir=... still relocates the output (no regression)."""
        rc, _, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script.py", "-o", "result.txt", "--setting", "result_dir=custom_out"],
        )
        self.assertEqual(rc, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "custom_out", "result.txt")))
