"""Tests for b2luigi.core.executable — the batch executable wrapper generator."""

import shlex
from unittest.mock import patch

from b2luigi.core.executable import create_executable_wrapper

from ..helpers import B2LuigiTestCase


def _wrapper_settings(overrides=None):
    """Settings side_effect for create_executable_wrapper with an Apptainer image set."""
    values = {
        "shell": "bash",
        "apptainer_image": "/img.sif",
        "working_dir": "/work",
        "env_script": "/env.sh",
        "env": {},
    }
    if overrides:
        values.update(overrides)

    def _side_effect(key, task=None, default=None, deprecated_keys=None):
        return values.get(key, default)

    return _side_effect


class TestApptainerWrapperQuoting(B2LuigiTestCase):
    """The wrapper must nest the Apptainer payload as a single shell word."""

    def _generate(self, apptainer_list):
        with (
            patch("b2luigi.core.executable.get_setting", side_effect=_wrapper_settings()),
            patch("b2luigi.core.executable.get_task_file_dir", return_value=self.test_dir),
            patch("b2luigi.core.executable.create_cmd_from_task", return_value=["b2luigi", "batch-runner"]),
            patch("b2luigi.core.executable.create_apptainer_command", return_value=apptainer_list),
        ):
            path = create_executable_wrapper(task=None)
        with open(path) as f:
            return f.read()

    def test_payload_stays_one_argv_element(self):
        """shlex.split of the generated line must keep the payload intact after -c."""
        payload = "source /env.sh && b2luigi batch-runner --param 'mylist=[1, 2, 3]'"
        content = self._generate(["apptainer", "exec", "/img.sif", "/bin/bash", "-c", payload])

        line = next(ln for ln in content.splitlines() if ln.startswith("apptainer "))
        argv = shlex.split(line)
        self.assertEqual(argv[argv.index("-c") + 1], payload)
        self.assertEqual(len(argv), argv.index("-c") + 2)

    def test_ampersand_never_escapes_to_the_outer_shell(self):
        """Regression guard: a bare && token would mean the job runs outside the container."""
        payload = "source /env.sh && b2luigi batch-runner"
        content = self._generate(["apptainer", "exec", "/img.sif", "/bin/bash", "-c", payload])

        line = next(ln for ln in content.splitlines() if ln.startswith("apptainer "))
        self.assertNotIn("&&", shlex.split(line))
