"""
Test helper functions for :py:class:`HTCondorProcess`.
"""

import json
import os
import subprocess
import unittest
from unittest import mock

import luigi

import b2luigi
from b2luigi.batch.processes import JobStatus
from b2luigi.batch.processes.htcondor import (
    HTCondorJobStatus,
    HTCondorJobStatusCache,
    HTCondorProcess,
    _batch_job_status_cache,
)

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask
from .batch_task_grouped import MyGroupedTask


class TestHTCondorCreateSubmitFile(B2LuigiTestCase):
    def _get_htcondor_submit_file_string(self, task):
        # the _create_htcondor_submit_file is a method of the ``HTCondorProcess`` class, but it only uses its
        # class to obtain ``self.task``, to it's sufficient to provide a mock class for ``self``, `
        htcondor_mock_process = mock.Mock()
        task.get_task_file_dir = lambda: self.test_dir
        task.get_log_file_dir = lambda: self.test_dir
        htcondor_mock_process.task = task
        #  create submit file
        htcondor_mock_process._create_submit_file_content = lambda task: HTCondorProcess._create_submit_file_content(
            task
        )
        HTCondorProcess._create_htcondor_submit_file(htcondor_mock_process)
        # read submit file and return string
        submit_file_path = os.path.join(self.test_dir, "job.submit")
        with open(submit_file_path, "r") as submit_file:
            return submit_file.read()

    def test_minimal_submit_file(self):
        """
        Minimal  submit file should have expected shape:

            output = ...
            error = ..
            log = ..
            executable = executable_wrapper.sh
            queue 1
        """
        task = MyTask("some_parameter")
        submit_file_lines = self._get_htcondor_submit_file_string(task).splitlines()
        self.assertIn("output = ", submit_file_lines[0])
        self.assertIn("error = ", submit_file_lines[1])
        self.assertIn("log = ", submit_file_lines[2])
        self.assertEqual(f"executable = {task.get_task_file_dir()}/executable_wrapper.sh", submit_file_lines[3])
        self.assertEqual("queue 1", submit_file_lines[4])

    def test_not_setting_job_name(self):
        submit_file_string = self._get_htcondor_submit_file_string(MyTask("some_parameter"))
        self.assertNotIn("JobBatchName", submit_file_string)

    def test_set_job_name_via_task_attribute(self):
        task = MyTask("some_parameter")
        task.job_name = "some_job_name"
        submit_file_lines = self._get_htcondor_submit_file_string(task).splitlines()
        self.assertIn("JobBatchName = some_job_name", submit_file_lines)

        b2luigi.set_setting("job_name", "some_job_name")
        submit_file_lines = self._get_htcondor_submit_file_string(MyTask("some_parameter")).splitlines()
        b2luigi.clear_setting("job_name")
        self.assertIn("JobBatchName = some_job_name", submit_file_lines)

    def test_set_job_name_is_overriden_by_htcondor_settings(self):
        """
        ``job_name`` is a global setting, but if the ``JobBatchName`` is set explicitly via the settings, we
        want that to override the global setting
        """
        task = MyTask("some_parameter")
        task.job_name = "job_name_global"
        htcondor_settings = {"JobBatchName": "job_name_htcondor"}
        b2luigi.set_setting("htcondor_settings", htcondor_settings)
        submit_file_lines = self._get_htcondor_submit_file_string(task).splitlines()
        b2luigi.clear_setting("htcondor_settings")
        self.assertNotIn("JobBatchName = job_name_global", submit_file_lines)
        self.assertIn("JobBatchName = job_name_htcondor", submit_file_lines)


class TestHTCondorJobStatusCache(unittest.TestCase):
    def setUp(self):
        mock_status_dicts = [
            {
                "JobStatus": 4,  # completed
                "ExitCode": 0,  # success
                "ClusterId": 42,
                "UserLog": "/some/mock/log",
            },
            {
                "JobStatus": 2,  # running
                "ClusterId": 43,
                "UserLog": "/some/mock/log",
            },
            {
                "JobStatus": 999,  # failed
                "ClusterId": 44,
                "UserLog": "/some/mock/log",
            },
        ]
        self.mock_status_json = json.dumps(mock_status_dicts).encode()

        self.htcondor_job_status_cache = HTCondorJobStatusCache()

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_does_2_retries(self, mock_check_output):
        """Test the ``_ask_for_job_status`` recovers after two condor_q failures."""

        # make check_output fail 2 times before  return status dict
        n_fail = 2
        mock_check_output.side_effect = n_fail * [subprocess.CalledProcessError(1, ["mock", "command"])] + [
            self.mock_status_json
        ]
        self.htcondor_job_status_cache._ask_for_job_status()
        self.assertEqual(mock_check_output.call_count, 3)

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_no_retries_on_success(self, mock_check_output):
        """Test the ``_ask_for_job_status`` is only called once (no retries) when everything works."""

        # make check_output fail 2 times before  return status dict
        mock_check_output.return_value = self.mock_status_json
        self.htcondor_job_status_cache._ask_for_job_status()
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
            self.htcondor_job_status_cache._ask_for_job_status()


class TestHTCondorGroupedSubmitFile(B2LuigiTestCase):
    """
    Tests for the parameter-grouping branch of :obj:`HTCondorProcess._create_htcondor_submit_file`.

    A grouped task arrives from luigi's batching machinery with a *tuple* value for
    every grouped parameter. The submit file must then carry one ``queue 1`` block
    per element of that tuple, each describing a single scalar sub-task.
    """

    def setUp(self):
        super().setUp()
        # Sub-task wrappers and logs must land in the temp dir: the defaults resolve relative to
        # the "main script", which under pytest on CI is a read-only externals installation.
        b2luigi.set_setting("log_dir", os.path.join(self.test_dir, "logs"))
        b2luigi.set_setting("task_file_dir", os.path.join(self.test_dir, "task_files"))

    def tearDown(self):
        b2luigi.clear_setting("log_dir")
        b2luigi.clear_setting("task_file_dir")
        super().tearDown()

    def _make_process(self, task):
        process = mock.Mock()
        task.get_task_file_dir = lambda: self.test_dir
        task.get_log_file_dir = lambda: self.test_dir
        process.task = task
        process._terminated = False
        process._create_submit_file_content = lambda task: HTCondorProcess._create_submit_file_content(task)
        return process

    def _submit_file_string(self, task):
        process = self._make_process(task)
        HTCondorProcess._create_htcondor_submit_file(process)
        with open(os.path.join(self.test_dir, "job.submit"), "r") as submit_file:
            return submit_file.read()

    @staticmethod
    def _mark_complete(task):
        output_file_name = task.get_output_file_name("grouped.txt")
        os.makedirs(os.path.dirname(output_file_name), exist_ok=True)
        with open(output_file_name, "w") as f:
            f.write("already done")

    def test_grouped_task_gets_one_queue_block_per_value(self):
        submit_file_string = self._submit_file_string(MyGroupedTask(plain=0, grouped=(0, 1, 2)))
        self.assertEqual(submit_file_string.count("queue 1"), 3)

    def test_grouped_task_blocks_carry_the_individual_values(self):
        submit_file_string = self._submit_file_string(MyGroupedTask(plain=0, grouped=(0, 1, 2)))
        for value in (0, 1, 2):
            self.assertIn(f"grouped={value}{os.sep}", submit_file_string)
        self.assertNotIn("grouped=(0, 1, 2)", submit_file_string)

    def test_ungrouped_value_is_submitted_as_a_single_job(self):
        submit_file_string = self._submit_file_string(MyGroupedTask(plain=0, grouped=7))
        self.assertEqual(submit_file_string.count("queue 1"), 1)

    def test_complete_sub_tasks_are_not_resubmitted(self):
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        self._mark_complete(task.clone(None, grouped=1))

        submit_file_string = self._submit_file_string(task)

        self.assertEqual(submit_file_string.count("queue 1"), 2)
        self.assertNotIn(f"grouped=1{os.sep}", submit_file_string)

    def test_fully_complete_group_terminates_without_submitting(self):
        task = MyGroupedTask(plain=0, grouped=(0, 1))
        for value in (0, 1):
            self._mark_complete(task.clone(None, grouped=value))

        process = self._make_process(task)
        HTCondorProcess._create_htcondor_submit_file(process)

        self.assertTrue(process._terminated)
        process._put_to_result_queue.assert_called_once_with(status=luigi.scheduler.DONE, explanation="")


class TestHTCondorGroupedJobStatus(B2LuigiTestCase):
    def setUp(self):
        super().setUp()
        _batch_job_status_cache.clear()

    def tearDown(self):
        _batch_job_status_cache.clear()
        super().tearDown()

    def test_status_is_aggregated_over_all_jobs(self):
        process = HTCondorProcess(
            task=MyGroupedTask(plain=0, grouped=(0, 1, 2)),
            scheduler=mock.Mock(),
            result_queue=mock.Mock(),
            worker_timeout=None,
        )
        process._batch_job_ids = [101, 102, 103]
        _batch_job_status_cache.add_job_ids(process._batch_job_ids)

        _batch_job_status_cache[101] = (HTCondorJobStatus.completed, "log")
        _batch_job_status_cache[102] = (HTCondorJobStatus.running, "log")
        _batch_job_status_cache[103] = (HTCondorJobStatus.failed, "log")
        self.assertEqual(process.get_job_status(), JobStatus.running)

        _batch_job_status_cache[102] = (HTCondorJobStatus.completed, "log")
        self.assertEqual(process.get_job_status(), JobStatus.aborted)
