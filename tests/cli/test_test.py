"""Integration tests for the b2luigi test subcommand."""

import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile
from unittest import TestCase, mock

import b2luigi

from b2luigi.cli.errors import CliUserError
from b2luigi.cli.runner import _build_fast_task, _build_fast_req_task, _resolve_batch_system, test_task
from b2luigi.core.executable import create_executable_wrapper
from b2luigi.core.settings import get_setting, set_setting, with_new_settings
from b2luigi.core.utils import create_cmd_from_task
from tests.cli.helpers import CLITestCase

# test_task is a production function (not a test), but its name matches pytest's
# default test_* collection pattern; mark it non-collectible to avoid pytest trying
# to run it as a test and inject fixtures for its parameters.
test_task.__test__ = False


FIXTURE_DIR = os.path.dirname(__file__)


class TestTestNormalMode(CLITestCase):
    """Tests for normal (non-force, non-batch) subprocess execution."""

    def setUp(self) -> None:
        super().setUp()
        for fixture in (
            "cli_test_script.py",
            "cli_test_script_failing.py",
            "cli_test_script_silent.py",
            "cli_test_script_failing_after_output.py",
        ):
            shutil.copy(os.path.join(FIXTURE_DIR, fixture), os.path.join(self.tmp_dir, fixture))

    def test_normal_run_succeeds(self) -> None:
        """Script that writes its output file should exit 0."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script.py", "-o", "result.txt"])
        self.assertEqual(rc, 0, stderr)

    def test_nonzero_exit_propagated(self) -> None:
        """Script that exits non-zero should make the CLI exit non-zero."""
        rc, stdout, stderr = self._run_cli("test", ["-s", "cli_test_script_failing.py", "-o", "result.txt"])
        self.assertNotEqual(rc, 0)

    def test_nonzero_exit_with_output_written_still_fails(self) -> None:
        """A script that writes its output and THEN exits non-zero must still fail the CLI.

        This is the case the returncode guard exists for specifically: the missing-output
        guard cannot catch it, since the output is present. Mirrors a basf2 steering file
        that errors after RootOutput has already flushed to disk (the motivating case for
        --executable).
        """
        rc, stdout, stderr = self._run_cli(
            "test", ["-s", "cli_test_script_failing_after_output.py", "-o", "result.txt"]
        )
        self.assertNotEqual(rc, 0)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))

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
        """Script, output file and the always-explicit literal-path flag are encoded."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertEqual(
            FastTask.task_cmd_additional_args,
            [
                "--script",
                os.path.abspath("script.py"),
                "--output-file",
                os.path.abspath("out.txt"),
                "--literal-path",
            ],
        )

    def test_input_file_included(self) -> None:
        """--input-file is appended when input_file is not None."""
        FastTask = _build_fast_task("script.py", "out.txt", "in.txt", False, False, [])
        args = FastTask.task_cmd_additional_args
        self.assertIn("--input-file", args)
        idx = args.index("--input-file")
        self.assertEqual(args[idx + 1], os.path.abspath("in.txt"))

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

    def test_literal_path_flag_included_by_default(self) -> None:
        """--literal-path is appended to task_cmd_additional_args by default (literal_path=True)."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        self.assertIn("--literal-path", FastTask.task_cmd_additional_args)

    def test_no_literal_path_flag_encoded_when_false(self) -> None:
        """literal_path=False is encoded explicitly as --no-literal-path so the worker cannot disagree."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], literal_path=False)
        self.assertIn("--no-literal-path", FastTask.task_cmd_additional_args)
        self.assertNotIn("--literal-path", FastTask.task_cmd_additional_args)


class TestFastTaskCmdQuoting(TestCase):
    """The generated batch tokens must survive the wrapper's shell-join round trip.

    ``create_executable_wrapper`` flattens ``create_cmd_from_task``'s output with
    ``" ".join(...)`` into a shell script, so any generated token containing a
    space is re-split by the shell on the worker unless it is quoted here.
    """

    def _round_trip(self, FastTask: type) -> list[str]:
        """Encode the task, flatten it exactly as the wrapper does, and re-tokenise."""
        with with_new_settings():
            set_setting("__batch_runner_use_cli", True)
            cmd = create_cmd_from_task(FastTask())
        return shlex.split(" ".join(cmd))

    def test_script_path_with_space_survives_the_join(self) -> None:
        """A script path containing a space reaches the worker as one token."""
        FastTask = _build_fast_task("my script.py", "out.txt", None, False, True, [])
        rebuilt = self._round_trip(FastTask)
        idx = rebuilt.index("--script")
        self.assertEqual(rebuilt[idx + 1], os.path.abspath("my script.py"))

    def test_extra_arg_with_space_survives_the_join(self) -> None:
        """An extra arg containing a space reaches the worker as one token."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, True, ["--label", "two words"])
        rebuilt = self._round_trip(FastTask)
        values = [rebuilt[i + 1] for i, a in enumerate(rebuilt) if a == "--extra-arg"]
        self.assertIn("two words", values)

    def test_output_and_input_keys_with_spaces_survive_the_join(self) -> None:
        """Output and input filename keys containing spaces reach the worker intact."""
        FastTask = _build_fast_task("script.py", "my out.txt", "my in.txt", False, True, [])
        rebuilt = self._round_trip(FastTask)
        self.assertEqual(rebuilt[rebuilt.index("--output-file") + 1], os.path.abspath("my out.txt"))
        self.assertEqual(rebuilt[rebuilt.index("--input-file") + 1], os.path.abspath("my in.txt"))


class TestFastTaskExecutable(TestCase):
    """Unit tests for the --executable swap and the resulting argv layout."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self._old_cwd = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        shutil.rmtree(self.tmp_dir)

    def _capture_cmd(self, FastTask) -> list[str]:
        """Run the task with subprocess.run patched out and return the argv it built.

        The fake writes the output file so ``_run``'s post-run existence check passes;
        no real interpreter is ever launched.
        """
        captured: dict[str, list[str]] = {}

        def fake_run(cmd, *args, **kwargs):
            captured["cmd"] = list(cmd)
            out = pathlib.Path(cmd[cmd.index("-o") + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text("done")
            return subprocess.CompletedProcess(cmd, 0)

        with mock.patch("b2luigi.cli.runner.subprocess.run", fake_run):
            FastTask().run()
        return captured["cmd"]

    def test_default_uses_sys_executable(self) -> None:
        """With no executable given, the current interpreter runs the script."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
        cmd = self._capture_cmd(FastTask)
        self.assertEqual(cmd[0], sys.executable)
        self.assertEqual(cmd[1], os.path.abspath("script.py"))

    def test_output_flag_precedes_extra_args(self) -> None:
        """-o is emitted before the user's extra args, for the default executable too."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, ["--lr", "0.01"])
        cmd = self._capture_cmd(FastTask)
        self.assertLess(cmd.index("-o"), cmd.index("--lr"))

    def test_no_separator_for_default_executable(self) -> None:
        """No -- is inserted when the executable was not overridden."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, ["--lr", "0.01"])
        self.assertNotIn("--", self._capture_cmd(FastTask))

    def test_executable_replaces_the_interpreter(self) -> None:
        """A given executable becomes the leading token instead of sys.executable."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], executable="basf2")
        cmd = self._capture_cmd(FastTask)
        self.assertEqual(cmd[0], "basf2")
        self.assertEqual(cmd[1], os.path.abspath("script.py"))

    def test_multi_token_executable_is_shlex_split(self) -> None:
        """A multi-token executable string is split on shell rules."""
        FastTask = _build_fast_task(
            "script.py", "out.txt", None, False, False, [], executable="apptainer exec img.sif basf2"
        )
        cmd = self._capture_cmd(FastTask)
        self.assertEqual(cmd[:4], ["apptainer", "exec", "img.sif", "basf2"])

    def test_separator_precedes_extra_args_when_executable_given(self) -> None:
        """-- separates b2luigi's flags from the script's own args under a custom executable."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, ["--lr", "0.01"], executable="basf2")
        cmd = self._capture_cmd(FastTask)
        self.assertEqual(cmd.count("--"), 1)
        self.assertEqual(cmd[cmd.index("--") + 1 :], ["--lr", "0.01"])

    def test_no_trailing_separator_without_extra_args(self) -> None:
        """A lone -- is never appended when there are no extra args."""
        FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], executable="basf2")
        self.assertNotIn("--", self._capture_cmd(FastTask))

    def test_input_flag_precedes_the_separator(self) -> None:
        """-i belongs to b2luigi's half of the command, before --."""
        pathlib.Path("in.txt").write_text("x")
        FastTask = _build_fast_task("script.py", "out.txt", "in.txt", False, False, ["--lr"], executable="basf2")
        FastReqTask = _build_fast_req_task("in.txt")
        FastTask = b2luigi.requires(FastReqTask)(FastTask)
        cmd = self._capture_cmd(FastTask)
        self.assertLess(cmd.index("-i"), cmd.index("--"))

    def test_executable_encoded_for_the_worker_when_given(self) -> None:
        """--executable is forwarded to the batch worker, quoted for the wrapper's shell join."""
        FastTask = _build_fast_task(
            "script.py", "out.txt", None, False, True, [], executable="apptainer exec img.sif basf2"
        )
        with with_new_settings():
            set_setting("__batch_runner_use_cli", True)
            rebuilt = shlex.split(" ".join(create_cmd_from_task(FastTask())))
        self.assertEqual(rebuilt[rebuilt.index("--executable") + 1], "apptainer exec img.sif basf2")

    def test_executable_absent_from_encoding_when_not_given(self) -> None:
        """The unset default must NOT be encoded.

        Unlike --literal-path (a policy both hosts must agree on), the executable is an
        environment-dependent path. Emitting the resolved default would bake the submission
        host's sys.executable into the worker command — the container-universe failure that
        executable_is_entrypoint exists to fix. The worker must fall back to its own.
        """
        FastTask = _build_fast_task("script.py", "out.txt", None, False, True, [])
        self.assertNotIn("--executable", FastTask.task_cmd_additional_args)


class TestFastTaskExecutableValidation(TestCase):
    """Unit tests for _build_fast_task's --executable validation (unparsable/blank values)."""

    def test_unparsable_executable_raises_cli_user_error(self) -> None:
        """A shlex.split ValueError is surfaced as a CliUserError, not a raw traceback."""
        with self.assertRaises(CliUserError):
            _build_fast_task("script.py", "out.txt", None, False, False, [], executable="basf2 'oops")

    def test_whitespace_only_executable_raises_cli_user_error(self) -> None:
        """An executable that is whitespace-only splits to zero tokens and must be rejected."""
        with self.assertRaises(CliUserError):
            _build_fast_task("script.py", "out.txt", None, False, False, [], executable="   ")

    def test_empty_string_executable_raises_cli_user_error(self) -> None:
        """An empty-string executable must be rejected rather than silently falling back to sys.executable."""
        with self.assertRaises(CliUserError):
            _build_fast_task("script.py", "out.txt", None, False, False, [], executable="")


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
            self.assertEqual(cmd[output_idx + 1], os.path.abspath("result.txt"))


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
        """--setting result_dir=custom_out actually relocates the output file, not just accepted.

        Needs --no-literal-path since the literal-path default would bypass result_dir
        entirely and hide whether the setting was applied.
        """
        rc, _, stderr = self._run_cli(
            "test",
            [
                "-s",
                "cli_test_script.py",
                "-o",
                "result.txt",
                "--setting",
                "result_dir=custom_out",
                "--no-literal-path",
            ],
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

    def test_default_bypasses_result_dir(self) -> None:
        """By default (literal_path=True), the output target must point at the literal path, not result_dir/out.txt."""
        with with_new_settings():
            set_setting("result_dir", "results")
            FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
            outputs = list(FastTask().output())
            self.assertEqual(len(outputs), 1)
            target = outputs[0]["out.txt"]
            self.assertEqual(target.path, os.path.abspath("out.txt"))

    def test_no_literal_path_nests_under_result_dir(self) -> None:
        """With literal_path=False, the output nests under result_dir as before the default flip."""
        with with_new_settings():
            set_setting("result_dir", "results")
            FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [], literal_path=False)
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

    def test_default_writes_to_given_path_not_result_dir(self) -> None:
        """By default, even with a custom result_dir, -o is written at the literal path given."""
        rc, _, stderr = self._run_cli(
            "test",
            [
                "-s",
                "cli_test_script.py",
                "-o",
                "result.txt",
                "--setting",
                "result_dir=custom_out",
            ],
        )
        self.assertEqual(rc, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.tmp_dir, "custom_out", "result.txt")))

    def test_no_literal_path_nests_under_result_dir(self) -> None:
        """--no-literal-path restores the old behavior: --setting result_dir=... relocates the output."""
        rc, _, stderr = self._run_cli(
            "test",
            [
                "-s",
                "cli_test_script.py",
                "-o",
                "result.txt",
                "--setting",
                "result_dir=custom_out",
                "--no-literal-path",
            ],
        )
        self.assertEqual(rc, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "custom_out", "result.txt")))


class TestBatchRunnerInputFile(CLITestCase):
    """The worker-side reconstruction must wire up the -i prerequisite task.

    ``test_task`` wraps ``FastTask`` with ``b2luigi.requires(FastReqTask)`` when
    ``-i`` is given; the ``batch-runner`` path built the task without it, so
    ``self.input()`` was empty on the worker and ``get_input_file_name`` raised
    ``KeyError: '<input file>'``. That made ``b2luigi test -i ... --batch`` fail on
    every real batch system while passing locally, because only the worker takes
    this path.
    """

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_reading_input.py"),
            os.path.join(self.tmp_dir, "cli_test_script_reading_input.py"),
        )
        with open(os.path.join(self.tmp_dir, "input.txt"), "w") as input_file:
            input_file.write("seed content")

    def test_batch_runner_resolves_the_input_file(self) -> None:
        """batch-runner --input-file resolves the path and hands it to the script.

        The script reads the input and copies it into its output, so a passing run
        proves the worker resolved a real, readable path — not merely that the
        command exited 0.
        """
        rc, stdout, stderr = self._run_cli(
            "batch-runner",
            [
                "--script",
                os.path.join(self.tmp_dir, "cli_test_script_reading_input.py"),
                "--output-file",
                "result.txt",
                "--input-file",
                "input.txt",
                "--literal-path",
            ],
        )
        self.assertEqual(rc, 0, stdout + stderr)
        output_path = os.path.join(self.tmp_dir, "result.txt")
        self.assertTrue(os.path.exists(output_path))
        with open(output_path) as output_file:
            self.assertEqual(output_file.read(), "input was: seed content")


class TestBatchSystemFlag(CLITestCase):
    """End-to-end tests for `test --batch-system`."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )

    def test_batch_system_local_implies_batch_and_runs(self) -> None:
        """--batch-system implies --batch and is honoured instead of PATH probing."""
        rc, stdout, stderr = self._run_cli(
            "test", ["-s", "cli_test_script.py", "-o", "result.txt", "--batch-system", "local"]
        )

        self.assertEqual(rc, 0, stdout + stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "result.txt")), stdout + stderr)

    def test_unknown_batch_system_is_rejected(self) -> None:
        """A typo fails loudly and names the nearest valid system."""
        rc, stdout, stderr = self._run_cli(
            "test", ["-s", "cli_test_script.py", "-o", "result.txt", "--batch-system", "slrum"]
        )

        self.assertNotEqual(rc, 0)
        self.assertIn("slurm", stdout + stderr)
        self.assertFalse(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))


class TestBatchSystemSelection(TestCase):
    """`--batch` must not stop the user choosing a batch system.

    ``_build_fast_task`` set ``batch_system`` as a *class attribute*, and
    ``get_setting`` ranks a task property above ``set_setting`` and ``settings.json``
    (``core/settings.py``). So ``--setting batch_system=slurm`` and a ``settings.json``
    entry were both silently ignored, and ``"auto"`` was resolved by probing PATH in a
    fixed order — which can only ever reach lsf/htcondor/slurm/local.
    """

    def test_class_attribute_is_auto_without_an_explicit_setting(self) -> None:
        """With nothing configured, --batch still means 'detect it for me'."""
        with with_new_settings():
            FastTask = _build_fast_task("script.py", "out.txt", None, False, True, [])
            self.assertEqual(FastTask.batch_system, "auto")

    def test_explicit_setting_wins_over_the_class_attribute(self) -> None:
        """An explicitly chosen batch system reaches get_setting instead of "auto"."""
        with with_new_settings():
            set_setting("batch_system", "htcondor")
            FastTask = _build_fast_task("script.py", "out.txt", None, False, True, [])
            self.assertNotIn("batch_system", FastTask.__dict__)
            self.assertEqual(get_setting("batch_system", default="auto", task=FastTask()), "htcondor")

    def test_local_run_ignores_an_explicit_batch_system(self) -> None:
        """Without --batch the task must stay local, whatever the settings say.

        Guard, not a change: a plain ``b2luigi test`` must never submit anywhere
        because a settings file happens to name a scheduler.
        """
        with with_new_settings():
            set_setting("batch_system", "slurm")
            FastTask = _build_fast_task("script.py", "out.txt", None, False, False, [])
            self.assertEqual(FastTask.batch_system, "local")

    def test_known_batch_system_is_accepted(self) -> None:
        """A valid name resolves to itself."""
        self.assertEqual(_resolve_batch_system("slurm"), "slurm")

    def test_gbasf2_is_selectable(self) -> None:
        """gbasf2 is a valid choice even though PATH probing can never detect it."""
        self.assertEqual(_resolve_batch_system("gbasf2"), "gbasf2")

    def test_unknown_batch_system_is_rejected_with_a_suggestion(self) -> None:
        """A typo is a hard error naming the nearest valid system, not a silent no-op."""
        with self.assertRaises(CliUserError) as caught:
            _resolve_batch_system("slrum")
        self.assertIn("slurm", str(caught.exception))


class TestTestInputInSubdirectory(CLITestCase):
    """``-i`` must accept a path, not only a bare filename in the current directory.

    ``self.input()`` is keyed by ``os.path.basename`` (``flatten_to_file_paths`` in
    ``core/utils.py``), but ``FastTask._run`` looked the key up by the raw ``-i``
    value. That matched only while the value happened to be a bare filename, so
    ``b2luigi test -i data/input.txt`` failed with ``KeyError: 'data/input.txt'``.
    """

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_reading_input.py"),
            os.path.join(self.tmp_dir, "cli_test_script_reading_input.py"),
        )
        os.mkdir(os.path.join(self.tmp_dir, "data"))
        with open(os.path.join(self.tmp_dir, "data", "input.txt"), "w") as input_file:
            input_file.write("seed content")

    def test_input_file_in_a_subdirectory_is_resolved(self) -> None:
        """A -i path with a directory component reaches the script as a readable file."""
        rc, stdout, stderr = self._run_cli(
            "test",
            ["-s", "cli_test_script_reading_input.py", "-o", "result.txt", "-i", os.path.join("data", "input.txt")],
        )

        self.assertEqual(rc, 0, stdout + stderr)
        output_path = os.path.join(self.tmp_dir, "result.txt")
        self.assertTrue(os.path.exists(output_path), stdout + stderr)
        with open(output_path) as output_file:
            self.assertEqual(output_file.read(), "input was: seed content")


class TestFastTaskWorkerPathResolution(CLITestCase):
    """The encoded -o/-i must resolve to the same files the submission host declared.

    ``_build_fast_task`` resolves ``exec_script`` to an absolute path before encoding
    it, but encoded ``output``/``input_file`` verbatim. The worker rebuilds the task
    with the very same function, so its ``os.path.abspath`` ran against the wrapper's
    ``cd <working_dir>`` instead of the directory the job was submitted from. With
    ``-i`` that failed loudly (``FileNotFoundError``); with only ``-o`` the job exited
    0 and wrote the output to the wrong directory, so the scheduler reported success
    while luigi never saw the target appear.
    """

    def setUp(self) -> None:
        super().setUp()
        # On macOS the temp dir is reached through the /var -> /private/var symlink,
        # which os.getcwd() (and therefore os.path.abspath) resolves but mkdtemp does
        # not report. Pin the resolved form so the encoded paths can be compared.
        self.tmp_dir = os.path.realpath(self.tmp_dir)
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script_reading_input.py"),
            os.path.join(self.tmp_dir, "cli_test_script_reading_input.py"),
        )
        with open(os.path.join(self.tmp_dir, "input.txt"), "w") as input_file:
            input_file.write("seed content")
        self.worker_dir = os.path.join(self.tmp_dir, "worker_working_dir")
        os.mkdir(self.worker_dir)

    def _encode_from_tmp_dir(self, **kwargs) -> list[str]:
        """Build FastTask with the tmp dir as cwd and return its decoded worker args."""
        previous_dir = os.getcwd()
        os.chdir(self.tmp_dir)
        try:
            FastTask = _build_fast_task(**kwargs)
        finally:
            os.chdir(previous_dir)
        return shlex.split(" ".join(FastTask.task_cmd_additional_args))

    def test_output_file_encoded_absolute_under_literal_path(self) -> None:
        """A relative -o is resolved against the submission cwd, like --script already is."""
        args = self._encode_from_tmp_dir(
            exec_script="script.py",
            output="out.txt",
            input_file=None,
            force=False,
            batch=True,
            extra_args=[],
        )
        self.assertEqual(args[args.index("--output-file") + 1], os.path.join(self.tmp_dir, "out.txt"))

    def test_input_file_encoded_absolute(self) -> None:
        """A relative -i is resolved against the submission cwd."""
        args = self._encode_from_tmp_dir(
            exec_script="script.py",
            output="out.txt",
            input_file="in.txt",
            force=False,
            batch=True,
            extra_args=[],
        )
        self.assertEqual(args[args.index("--input-file") + 1], os.path.join(self.tmp_dir, "in.txt"))

    def test_output_file_left_alone_without_literal_path(self) -> None:
        """Without --literal-path, -o is a filename key for add_to_output, not a path.

        ``os.path.join(result_dir, "/abs/out.txt")`` collapses to ``/abs/out.txt``, so
        absolutizing here would silently discard the ``result_dir`` nesting the flag
        exists to provide.
        """
        args = self._encode_from_tmp_dir(
            exec_script="script.py",
            output="out.txt",
            input_file=None,
            force=False,
            batch=True,
            extra_args=[],
            literal_path=False,
        )
        self.assertEqual(args[args.index("--output-file") + 1], "out.txt")

    def test_worker_in_a_different_directory_uses_the_submission_paths(self) -> None:
        """End-to-end: the worker resolves -o/-i where the job was submitted from.

        The command is the one the wrapper would run, executed from a directory that
        is not the submission directory. The script copies its input into its output,
        so passing proves both paths resolved to the real files rather than merely
        that the command exited 0.
        """
        args = self._encode_from_tmp_dir(
            exec_script="cli_test_script_reading_input.py",
            output="result.txt",
            input_file="input.txt",
            force=False,
            batch=True,
            extra_args=[],
        )
        rc, stdout, stderr = self._run_cli("batch-runner", args, cwd=self.worker_dir)

        self.assertEqual(rc, 0, stdout + stderr)
        submitted_output = os.path.join(self.tmp_dir, "result.txt")
        self.assertTrue(os.path.exists(submitted_output), stdout + stderr)
        with open(submitted_output) as output_file:
            self.assertEqual(output_file.read(), "input was: seed content")
        self.assertFalse(os.path.exists(os.path.join(self.worker_dir, "result.txt")))


class TestTestExecutableFlag(CLITestCase):
    """End-to-end tests for --executable through the real CLI binary."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_test_script.py"),
            os.path.join(self.tmp_dir, "cli_test_script.py"),
        )

    def test_executable_runs_the_script(self) -> None:
        """--executable with the current interpreter behaves like the default path."""
        rc, stdout, stderr = self._run_cli(
            "test", ["-s", "cli_test_script.py", "-o", "result.txt", "--executable", sys.executable]
        )
        self.assertEqual(rc, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))

    def test_missing_executable_fails_loudly(self) -> None:
        """A non-existent executable makes the run fail rather than silently succeed.

        A bare non-zero exit code does not discriminate here: a bug that silently
        no-opped the whole task would produce one too. So this also pins that the
        failure names the missing binary, and that no output file was produced.
        """
        rc, stdout, stderr = self._run_cli(
            "test", ["-s", "cli_test_script.py", "-o", "result.txt", "--executable", "definitely-not-a-real-binary"]
        )
        self.assertNotEqual(rc, 0)
        self.assertIn("definitely-not-a-real-binary", stdout + stderr)
        self.assertFalse(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))

    def test_batch_runner_accepts_executable(self) -> None:
        """The worker-side reconstruction accepts and uses --executable (batch round trip)."""
        rc, stdout, stderr = self._run_cli(
            "batch-runner",
            [
                "--script",
                os.path.join(self.tmp_dir, "cli_test_script.py"),
                "--output-file",
                "result.txt",
                "--literal-path",
                "--executable",
                sys.executable,
            ],
        )
        self.assertEqual(rc, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir, "result.txt")))
