"""``working_dir`` names the project root as the worker sees it.

``docs/features/batch.rst``: "In case your script is accessible from a
different location on the worker than on the scheduling machine, you can give
the setting ``working_dir`` to specify where the job should run. Your script
needs to be in this folder."

So a batch worker must run the code found under ``working_dir`` — not the code
at the submission host's path. These tests use ``batch_system: "test"``, which
generates the real executable wrapper and runs it, so they exercise the same
wire format a real scheduler would.
"""

import json
import os
import shutil

from tests.cli.helpers import CLITestCase


TASKS_TEMPLATE = '''
import b2luigi


class MarkerTask(b2luigi.Task):
    """Writes which copy of the source tree actually executed."""

    number = b2luigi.IntParameter(default=1)

    def output(self):
        yield self.add_to_output("marker.txt")

    def run(self):
        with open(self.get_output_file_name("marker.txt"), "w") as handle:
            handle.write("{marker}\\n")

    def remove_output(self):
        self._remove_output()
'''


class TestWorkingDirRelocation(CLITestCase):
    """The worker must execute the copy of the project living in working_dir."""

    def setUp(self) -> None:
        super().setUp()
        self.dev_dir = os.path.join(self.tmp_dir, "dev")
        self.prod_dir = os.path.join(self.tmp_dir, "prod")
        self.results_dir = os.path.join(self.tmp_dir, "results")
        for path, marker in ((self.dev_dir, "DEV-CODE"), (self.prod_dir, "PROD-CODE")):
            os.makedirs(path)
            with open(os.path.join(path, "tasks.py"), "w") as handle:
                handle.write(TASKS_TEMPLATE.format(marker=marker))

    def _write_settings(self, directory: str) -> None:
        settings = {
            "result_dir": self.results_dir,
            "log_dir": os.path.join(self.tmp_dir, "logs"),
            "batch_system": "test",
            "working_dir": self.prod_dir,
        }
        with open(os.path.join(directory, "settings.json"), "w") as handle:
            json.dump(settings, handle)

    def test_worker_runs_the_code_in_working_dir_not_the_submission_copy(self):
        """Submitting from dev with working_dir=prod must execute the prod code.

        Reverting the relative ``--task-file`` encoding makes this fail with
        ``DEV-CODE``: the wrapper ``cd``s into prod but the absolute task-file
        path sends the worker back to the dev copy. No error is raised — the
        job succeeds and writes a plausible file — so only the marker's
        content discriminates.
        """
        self._write_settings(self.dev_dir)
        shutil.copy(os.path.join(self.dev_dir, "settings.json"), os.path.join(self.prod_dir, "settings.json"))

        returncode, stdout, stderr = self._run_cli("run", ["MarkerTask", "--batch"], cwd=self.dev_dir)

        marker_path = os.path.join(self.results_dir, "number=1", "marker.txt")
        self.assertTrue(
            os.path.exists(marker_path),
            f"no output produced (rc={returncode})\nstdout:\n{stdout}\nstderr:\n{stderr}",
        )
        with open(marker_path) as handle:
            self.assertEqual(handle.read().strip(), "PROD-CODE")

    def test_wrapper_encodes_task_file_relative_to_working_dir(self):
        """The generated wrapper must not carry the submission host's path."""
        self._write_settings(self.dev_dir)
        shutil.copy(os.path.join(self.dev_dir, "settings.json"), os.path.join(self.prod_dir, "settings.json"))

        self._run_cli("run", ["MarkerTask", "--batch"], cwd=self.dev_dir)

        wrapper = os.path.join(self.prod_dir, "task_files", "number=1", "MarkerTask", "executable_wrapper.sh")
        if not os.path.exists(wrapper):
            wrapper = os.path.join(self.dev_dir, "task_files", "number=1", "MarkerTask", "executable_wrapper.sh")
        with open(wrapper) as handle:
            exec_line = [line for line in handle if line.startswith("exec ")][0]

        self.assertIn("--task-file tasks.py", exec_line)
        self.assertNotIn(self.dev_dir, exec_line)
