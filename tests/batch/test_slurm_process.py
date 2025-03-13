"""
Test helper functions for :py:class:`SlurmProcess`.
"""

import json
import pathlib

import subprocess
import unittest
from unittest import mock

import b2luigi
from b2luigi.batch.processes.slurm import SlurmJobStatusCache, SlurmProcess

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask


class TestSlurmCreateSubmitFile(B2LuigiTestCase):
    def _get_slurm_submit_file_string(self, task):
        # the _create_slurm_submit_file is a method of the ``SlurmProcess`` class, but it only uses its
        # class to obtain ``self.task``, so it's sufficient to provide a mock class for ``self``, `
        slurm_mock_process = mock.Mock()
        task.get_task_file_dir = lambda: self.test_dir
        task.get_log_file_dir = lambda: self.test_dir
        slurm_mock_process.task = task
        #  create submit file
        SlurmProcess._create_slurm_submit_file(slurm_mock_process)
        # read submit file and return string
        submit_file_path = pathlib.Path(self.test_dir) / "submit_job.sh"
        with open(submit_file_path, "r") as submit_file:
            return submit_file.read()

    def test_minimal_submit_file(self):
        """
        Minimal submit file should have expected shape:
            #!/usr/bin/bash
            #SBATCH --output=...
            #SBATCH --error=...
            exec executable_wrapper.sh
        """
        submit_file_lines = self._get_slurm_submit_file_string(MyTask("some_parameter")).splitlines()
        self.assertIn("#!/usr/bin/bash", submit_file_lines[0])
        self.assertIn("#SBATCH --output=", submit_file_lines[1])
        self.assertIn("#SBATCH --error=", submit_file_lines[2])
        self.assertEqual(
            f"exec {(pathlib.Path(self.test_dir)/'executable_wrapper.sh').resolve()}", submit_file_lines[3]
        )

    def test_not_setting_job_name(self):
        submit_file_string = self._get_slurm_submit_file_string(MyTask("some_parameter"))
        self.assertNotIn("job-name", submit_file_string)

    def test_set_job_name_via_task_attribute(self):
        task = MyTask("some_parameter")
        task.job_name = "some_job_name"
        submit_file_lines = self._get_slurm_submit_file_string(task).splitlines()
        self.assertIn("#SBATCH --job-name=some_job_name", submit_file_lines)

        b2luigi.set_setting("job_name", "some_job_name")
        submit_file_lines = self._get_slurm_submit_file_string(MyTask("some_parameter")).splitlines()
        b2luigi.clear_setting("job_name")
        self.assertIn("#SBATCH --job-name=some_job_name", submit_file_lines)

    def test_set_job_name_is_overriden_by_slurm_settings(self):
        """
        ``job_name`` is a global setting, but if the ``job-name`` is set explicitly via the settings, we
        want that to override the global setting
        """
        task = MyTask("some_parameter")
        task.job_name = "job_name_global"
        slurm_settings = {"job-name": "job_name_slurm"}
        b2luigi.set_setting("slurm_settings", slurm_settings)
        submit_file_lines = self._get_slurm_submit_file_string(task).splitlines()
        b2luigi.clear_setting("slurm_settings")
        self.assertNotIn("#SBATCH --job-name=job_name_global", submit_file_lines)
        self.assertIn("#SBATCH --job-name=job_name_slurm", submit_file_lines)


class TestSlurmJobStatusCache(unittest.TestCase):
    def setUp(self):
        mock_status_dicts = {
            "jobs": [
                {
                    "job_state": ["COMPLETED"],  # completed
                    "job_id": 42,
                },
                {
                    "job_state": ["RUNNING", "RESIZING"],  # running
                    "job_id": 43,
                },
                {
                    "job_state": ["FAILED"],  # failed - squeue output
                    "job_id": 44,
                },
                {
                    "state": {"current": ["FAILED"]},  # failed - sacct output
                    "job_id": 45,
                },
            ]
        }
        self.mock_status_json = json.dumps(mock_status_dicts).encode()

        self.slurm_job_status_cache = SlurmJobStatusCache()

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_does_2_retries(self, mock_check_output):
        """Test the ``_ask_for_job_status`` recovers after two condor_q failures."""

        # make check_output fail 2 times before  return status dict
        n_fail = 2
        mock_check_output.side_effect = n_fail * [subprocess.CalledProcessError(1, ["mock", "command"])] + [
            self.mock_status_json
        ]
        self.slurm_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 3)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_no_retries_on_success(self, mock_check_output):
        """Test the ``_ask_for_job_status`` is only called once (no retries) when everything works."""

        # make check_output fail 2 times before  return status dict
        mock_check_output.return_value = self.mock_status_json
        self.slurm_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 1)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_fails_after_4_condor_q_failures(self, mock_check_output):
        """Test the ``_ask_for_job_status`` does not do more than 3 retries"""

        # make check_output fail 2 times before  return status dict
        n_fail = 4
        mock_check_output.side_effect = n_fail * [subprocess.CalledProcessError(1, ["mock", "command"])] + [
            self.mock_status_json
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            self.slurm_job_status_cache._ask_for_job_status()
