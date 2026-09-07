"""Integration tests for the b2luigi batch-runner subcommand in test mode."""

import os
import pathlib
import shutil

from tests.cli.helpers import CLITestCase

FIXTURE_DIR = os.path.dirname(__file__)


class TestBatchRunnerTestMode(CLITestCase):
    """Tests for batch_runner --script test-mode reconstruction."""

    def setUp(self) -> None:
        super().setUp()
        for fixture in (
            "cli_test_script.py",
            "cli_test_script_counter.py",
            "cli_test_script_echo_args.py",
        ):
            shutil.copy(os.path.join(FIXTURE_DIR, fixture), os.path.join(self.tmp_dir, fixture))

    def test_script_and_output_runs_successfully(self) -> None:
        """--script + --output-file should run the script and exit 0."""
        rc, _, stderr = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script.py", "--output-file", "result.txt"],
        )
        self.assertEqual(rc, 0, stderr)

    def test_force_reruns_when_output_exists(self) -> None:
        """--force should re-run the script even when the output already exists."""
        rc, _, stderr = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script_counter.py", "--output-file", "result.txt"],
        )
        self.assertEqual(rc, 0, stderr)
        rc, _, stderr = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script_counter.py", "--output-file", "result.txt", "--force"],
        )
        self.assertEqual(rc, 0, stderr)
        counter = int(pathlib.Path(os.path.join(self.tmp_dir, "counter.txt")).read_text())
        self.assertEqual(counter, 2)

    def test_extra_arg_forwarded_to_script(self) -> None:
        """--extra-arg values should reach the script's sys.argv."""
        rc, _, stderr = self._run_cli(
            "batch-runner",
            [
                "--script",
                "cli_test_script_echo_args.py",
                "--output-file",
                "result.txt",
                "--extra-arg",
                "--lr",
                "--extra-arg",
                "0.01",
            ],
        )
        self.assertEqual(rc, 0, stderr)
        content = pathlib.Path(os.path.join(self.tmp_dir, "result.txt")).read_text()
        self.assertIn("--lr", content)
        self.assertIn("0.01", content)

    def test_missing_output_file_is_an_error(self) -> None:
        """--script without --output-file should exit non-zero."""
        rc, _, _ = self._run_cli(
            "batch-runner",
            ["--script", "cli_test_script.py"],
        )
        self.assertNotEqual(rc, 0)

    def test_neither_classname_nor_script_is_an_error(self) -> None:
        """Invoking batch-runner with no flags should exit non-zero."""
        rc, _, _ = self._run_cli("batch-runner", [])
        self.assertNotEqual(rc, 0)

    def test_classname_and_script_together_takes_script_path(self) -> None:
        """When --classname and --script are both present (the real batch-system path), script-mode wins."""
        rc, _, stderr = self._run_cli(
            "batch-runner",
            [
                "--classname",
                "FastTask",
                "--script",
                "cli_test_script.py",
                "--output-file",
                "result.txt",
            ],
        )
        self.assertEqual(rc, 0, stderr)


class TestBatchRunnerParamRoundTrip(CLITestCase):
    """batch-runner must reconstruct parameters byte-identically to the submitter."""

    def setUp(self) -> None:
        super().setUp()
        shutil.copy(
            os.path.join(FIXTURE_DIR, "cli_roundtrip_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )

    def _dirs_created(self) -> set:
        """Names of every directory created anywhere under the temp project."""
        found = set()
        for _root, dirnames, _files in os.walk(self.tmp_dir):
            found.update(dirnames)
        return found

    def _assert_value_preserved(self, value: str) -> None:
        rc, _, stderr = self._run_cli(
            "batch-runner",
            ["--classname", "RoundTripTask", "--param", f"text={value}"],
        )
        self.assertEqual(rc, 0, stderr)
        self.assertIn(f"text={value}", self._dirs_created())

    def test_trailing_zero_float_string_is_preserved(self) -> None:
        """'1.50' must not be normalised to '1.5'."""
        self._assert_value_preserved("1.50")

    def test_boolean_shaped_string_is_preserved(self) -> None:
        """'true' must not become 'True'."""
        self._assert_value_preserved("true")

    def test_json_shaped_string_is_preserved(self) -> None:
        """A Parameter holding '[1,2]' must not gain a space."""
        self._assert_value_preserved("[1,2]")

    def test_plain_string_is_unaffected(self) -> None:
        """Control: a non-JSON value already round-trips correctly today."""
        self._assert_value_preserved("plain")


class TestBatchRunnerLoadsParametersFile(CLITestCase):
    """The worker imports ``parameters.py`` so its side effects apply on the node.

    ``b2luigi run`` imports the parameters file on the submission host, so a
    ``set_setting`` call in it takes effect there. The worker rebuilds the task
    from ``--classname``/``--param`` and used to import only the task file, so
    the same ``set_setting`` never ran on the node: ``get_setting`` raised and
    ``add_to_output`` silently fell back to the worker's cwd.
    """

    TASKS = (
        "import b2luigi\n"
        "\n"
        "\n"
        "class SettingTask(b2luigi.Task):\n"
        "    number = b2luigi.IntParameter()\n"
        "\n"
        "    def output(self):\n"
        "        yield self.add_to_output('out.txt')\n"
        "\n"
        "    def run(self):\n"
        "        marker = b2luigi.get_setting('marker', task=self)\n"
        "        with open(self.get_output_file_name('out.txt'), 'w') as f:\n"
        "            f.write(marker)\n"
    )

    def _write(self, name: str, content: str) -> None:
        pathlib.Path(os.path.join(self.tmp_dir, name)).write_text(content)

    def setUp(self) -> None:
        super().setUp()
        self._write("tasks.py", self.TASKS)
        self._write("settings.json", '{"log_dir": "logs"}')

    def _assert_worker_wrote(self, extra_args: list[str], relative_result: str) -> None:
        rc, stdout, stderr = self._run_cli(
            "batch-runner",
            ["--classname", "tasks.SettingTask", "--param", "number=1", *extra_args],
        )
        self.assertEqual(rc, 0, stdout + stderr)
        out = pathlib.Path(self.tmp_dir, relative_result, "number=1", "out.txt")
        self.assertTrue(out.exists(), f"missing {out}\n{stdout}\n{stderr}")
        self.assertEqual(out.read_text(), "from-parameters-py")
        self.assertFalse(pathlib.Path(self.tmp_dir, "number=1").exists(), "output leaked into the worker cwd")

    def test_default_parameters_file_is_imported(self) -> None:
        """A ``set_setting`` in ``parameters.py`` is in effect when the task runs on the worker."""
        self._write(
            "parameters.py",
            "import b2luigi\n"
            "b2luigi.set_setting('result_dir', 'results')\n"
            "b2luigi.set_setting('marker', 'from-parameters-py')\n"
            "config = {'number': 1}\n",
        )
        self._assert_worker_wrote([], "results")

    def test_explicit_params_file_is_honoured(self) -> None:
        """``--params-file`` names the file the worker imports, as forwarded by ``run``."""
        self._write(
            "sweep.py",
            "import b2luigi\n"
            "b2luigi.set_setting('result_dir', 'elsewhere')\n"
            "b2luigi.set_setting('marker', 'from-parameters-py')\n"
            "config = {'number': 1}\n",
        )
        self._assert_worker_wrote(["--params-file", "sweep.py"], "elsewhere")

    def test_missing_parameters_file_is_not_an_error(self) -> None:
        """A project without ``parameters.py`` still runs; the file is optional on the worker too."""
        self._write("settings.json", '{"log_dir": "logs", "result_dir": "results", "marker": "from-parameters-py"}')
        self._assert_worker_wrote([], "results")
