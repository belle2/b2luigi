"""
Test helper functions for :py:class:`SlurmProcess`.
"""

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
        submit_file_path = pathlib.Path(self.test_dir) / "slurm_parameters.sh"
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
        task = MyTask("some_parameter_a")
        submit_file_lines = self._get_slurm_submit_file_string(task).splitlines()
        self.assertIn("#!/usr/bin/bash", submit_file_lines[0])
        self.assertIn("#SBATCH --output=", submit_file_lines[1])
        self.assertIn("#SBATCH --error=", submit_file_lines[2])
        self.assertEqual(
            f"exec {(pathlib.Path(self.test_dir)/'executable_wrapper.sh').resolve()}", submit_file_lines[3]
        )

    def test_not_setting_job_name(self):
        task = MyTask("some_parameter_b")
        submit_file_string = self._get_slurm_submit_file_string(task)
        self.assertNotIn("job-name", submit_file_string)

    def test_set_job_name_via_task_attribute(self):
        task = MyTask("some_parameter_c")
        task.job_name = "some_job_name"
        submit_file_lines = self._get_slurm_submit_file_string(task).splitlines()
        self.assertIn("#SBATCH --job-name=some_job_name", submit_file_lines)

        b2luigi.set_setting("job_name", "some_job_name")
        submit_file_lines = self._get_slurm_submit_file_string(task).splitlines()
        b2luigi.clear_setting("job_name")
        self.assertIn("#SBATCH --job-name=some_job_name", submit_file_lines)

    def test_set_job_name_is_overriden_by_slurm_settings(self):
        """
        ``job_name`` is a global setting, but if the ``job-name`` is set explicitly via the settings, we
        want that to override the global setting
        """
        task = MyTask("some_parameter_d")
        task.job_name = "job_name_global"
        slurm_settings = {"job-name": "job_name_slurm"}
        b2luigi.set_setting("slurm_settings", slurm_settings)
        submit_file_lines = self._get_slurm_submit_file_string(task).splitlines()
        b2luigi.clear_setting("slurm_settings")
        self.assertNotIn("#SBATCH --job-name=job_name_global", submit_file_lines)
        self.assertIn("#SBATCH --job-name=job_name_slurm", submit_file_lines)


class TestSlurmJobStatusCache(unittest.TestCase):
    def setUp(self):
        # Slurm output formatted when using squeue and sacct
        mock_squeue_sacct_status_string = "12344 COMPLETED\n12356 RUNNING\n13253 FAILED\n"
        self.mock_status_string = mock_squeue_sacct_status_string.encode()
        # Incorrect sqeue status
        mock_incorrect_squeue_sacct_status_string = "IncorrectString"
        self.mock_incorrect_sqeue_sacct_status_string = mock_incorrect_squeue_sacct_status_string.encode()
        # Slurm output formatted when using scontrol
        mock_scontrol_status_string = """
            JobId=142889 JobName=slurm_parameters.sh
            UserId=test JobState=RUNNING
        """
        self.mock_scontrol_status_string = mock_scontrol_status_string.encode()
        # Empty encoded string for when squeue returns nothing
        self.mock_empty_squeue_status_string = "".encode()
        self.slurm_job_status_cache = SlurmJobStatusCache()

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_does_2_retries(self, mock_check_output):
        """Test the ``_ask_for_job_status`` recovers after two squeue failures."""

        # make check_output fail 2 times before  return status dict
        n_fail = 2
        mock_check_output.side_effect = n_fail * [subprocess.CalledProcessError(1, ["mock", "command"])] + [
            self.mock_status_string
        ]
        self.slurm_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 3)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_no_retries_on_success(self, mock_check_output):
        """Test the ``_ask_for_job_status`` is only called once (no retries) when everything works."""

        # make check_output fail 2 times before  return status dict
        mock_check_output.return_value = self.mock_status_string
        self.slurm_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 1)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_fails_after_4_condor_q_failures(self, mock_check_output):
        """Test the ``_ask_for_job_status`` does not do more than 3 retries"""

        # make check_output fail 2 times before  return status dict
        n_fail = 4
        mock_check_output.side_effect = n_fail * [subprocess.CalledProcessError(1, ["mock", "command"])] + [
            self.mock_status_string
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            self.slurm_job_status_cache._ask_for_job_status()

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_with_scontrol_no_retries_on_success(self, mock_check_output):
        """Test that when a system has no sacct, the scontrol method works on first try"""

        # make _check_sacct_is_active_on_server return False to enable scontrol command
        self.slurm_job_status_cache._check_if_sacct_is_disabled_on_server = lambda: True
        self.assertEqual(self.slurm_job_status_cache._check_if_sacct_is_disabled_on_server(), True)
        # Make the first subprocess.check_output call return an empty string to signify
        # there is no output from squeue. Since the _check_sacct_is_active_on_server returns
        # False the second call of subprocess.check_output will be using scontrol
        mock_check_output.side_effect = [self.mock_empty_squeue_status_string] + [self.mock_scontrol_status_string]
        # Run assertion test
        self.slurm_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 1)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_with_scontrol_does_2_retries(self, mock_check_output):
        """Test that when a system has no sacct, scontrol method works on third try"""
        # make check_output fail 2 times before  return status dict
        n_fail = 2
        # After 2 retries, make the first successful subprocess.check_output call return an
        # empty string to signify there is no output from squeue. Since the
        # _check_sacct_is_active_on_server returns False the second call of subprocess.check_output
        # will be using scontrol
        mock_check_output.side_effect = (
            n_fail * [subprocess.CalledProcessError(1, ["mock", "command"])]
            + [self.mock_empty_squeue_status_string]
            + [self.mock_scontrol_status_string]
        )
        # make _check_sacct_is_active_on_server return False to enable scontrol command
        self.slurm_job_status_cache._check_if_sacct_is_disabled_on_server = lambda: True
        self.assertEqual(self.slurm_job_status_cache._check_if_sacct_is_disabled_on_server(), True)
        # Run assertion test
        self.slurm_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 3)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_with_sqeue_sacct_raises_assertion_for_incorrect_formatted_output(self, mock_check_output):
        """Test that when a slurm system returns an unexpected return string, an assertion is raised"""
        mock_check_output.side_effect = [self.mock_incorrect_sqeue_sacct_status_string]
        # Run assertion test
        with self.assertRaises(AssertionError):
            self.slurm_job_status_cache._ask_for_job_status()
