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
