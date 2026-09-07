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
from b2luigi.batch.processes.htcondor import HTCondorJobStatusCache, HTCondorProcess
from b2luigi.batch.workers import BatchSystems, SendJobWorker
from b2luigi.core.utils import get_task_file_dir

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

    def _make_process(self, task):
        """
        Build a mock stand-in for :obj:`HTCondorProcess` around *task*.

        ``_create_htcondor_submit_file`` only reaches ``self.task``, ``self._terminated``
        and ``self._put_to_result_queue``, so a mock is sufficient and keeps the test
        free of a scheduler and a result queue.
        """
        process = mock.Mock()
        # Pin the submit file next to the test, but leave the *cloned* sub-tasks
        # untouched so their paths are derived from their real parameter values.
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

    def test_grouped_task_gets_one_queue_block_per_value(self):
        """A tuple of three grouped values must expand into three separate jobs."""
        submit_file_string = self._submit_file_string(MyGroupedTask(plain=0, grouped=(0, 1, 2)))
        self.assertEqual(submit_file_string.count("queue 1"), 3)

    def test_grouped_task_blocks_carry_the_individual_values(self):
        """
        Each queue block must describe one scalar sub-task, not the tuple.

        This is what makes the expansion meaningful: were the parent task submitted
        unchanged, every block would carry the same ``grouped=(0, 1, 2)`` path.
        """
        submit_file_string = self._submit_file_string(MyGroupedTask(plain=0, grouped=(0, 1, 2)))
        for value in (0, 1, 2):
            self.assertIn(f"grouped={value}{os.sep}", submit_file_string)
        self.assertNotIn("grouped=(0, 1, 2)", submit_file_string)

    def test_ungrouped_value_is_submitted_as_a_single_job(self):
        """
        A grouped parameter that luigi did not batch is a plain scalar and submits once.

        The task is then passed through untouched, so this asserts only on the block
        count -- the harness pins the parent task's directories to ``test_dir``, which
        keeps its parameter values out of the paths.
        """
        submit_file_string = self._submit_file_string(MyGroupedTask(plain=0, grouped=7))
        self.assertEqual(submit_file_string.count("queue 1"), 1)

    def test_complete_sub_tasks_are_not_resubmitted(self):
        """Only the sub-tasks whose output is still missing may reach the submit file."""
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        done = task.clone(None, grouped=1)
        output_file_name = done.get_output_file_name("grouped.txt")
        os.makedirs(os.path.dirname(output_file_name), exist_ok=True)
        with open(output_file_name, "w") as f:
            f.write("already done")
        self.assertTrue(done.complete())

        submit_file_string = self._submit_file_string(task)

        self.assertEqual(submit_file_string.count("queue 1"), 2)
        self.assertNotIn(f"grouped=1{os.sep}", submit_file_string)
        self.assertIn(f"grouped=0{os.sep}", submit_file_string)
        self.assertIn(f"grouped=2{os.sep}", submit_file_string)

    def test_fully_complete_group_terminates_without_submitting(self):
        """
        When every sub-task is already done the submit file is empty, so the process
        must report the task DONE instead of handing an empty file to ``condor_submit``.
        """
        task = MyGroupedTask(plain=0, grouped=(0, 1))
        for value in (0, 1):
            output_file_name = task.clone(None, grouped=value).get_output_file_name("grouped.txt")
            os.makedirs(os.path.dirname(output_file_name), exist_ok=True)
            with open(output_file_name, "w") as f:
                f.write("already done")

        process = self._make_process(task)
        HTCondorProcess._create_htcondor_submit_file(process)

        self.assertTrue(process._terminated)
        process._put_to_result_queue.assert_called_once_with(status=luigi.scheduler.DONE, explanation="")

    def test_each_sub_task_is_addressed_individually_on_the_worker(self):
        """
        The batch command of every block must reconstruct one scalar sub-task.

        Grouping expands the task at *submission* time, so the worker never learns
        that a group existed — it must receive a plain ``--param grouped=<value>``.
        """
        b2luigi.set_setting("__batch_runner_use_cli", True)
        b2luigi.set_setting("__batch_runner_task_file", "tasks.py")
        try:
            self._submit_file_string(MyGroupedTask(plain=0, grouped=(0, 1, 2)))
            commands = [
                open(
                    os.path.join(get_task_file_dir(MyGroupedTask(plain=0, grouped=value)), "executable_wrapper.sh"),
                    "r",
                ).read()
                for value in (0, 1, 2)
            ]
        finally:
            b2luigi.clear_setting("__batch_runner_use_cli")
            b2luigi.clear_setting("__batch_runner_task_file")

        for value, command in zip((0, 1, 2), commands):
            self.assertIn("--classname tests.batch.batch_task_grouped.MyGroupedTask", command)
            self.assertIn(f"--param grouped={value}", command)
            self.assertIn("--param plain=0", command)


class TestGroupingIsHTCondorOnly(B2LuigiTestCase):
    """Grouping is only implemented for HTCondor; every other batch system must refuse it."""

    def _create_process(self, task, batch_system):
        worker = mock.Mock()
        worker.detect_batch_system = lambda task: BatchSystems(batch_system)
        # BatchProcess.__init__ does arithmetic on the timeout, so it cannot be a Mock.
        worker._config.timeout = None
        return SendJobWorker._create_task_process(worker, task)

    def test_grouping_on_a_non_htcondor_system_raises(self):
        task = MyGroupedTask(plain=0, grouped=(0, 1))
        with self.assertRaises(RuntimeError) as context:
            self._create_process(task, "slurm")
        self.assertIn("only implemented for HTCondor", str(context.exception))

    def test_grouping_on_htcondor_is_accepted(self):
        """Negative control: the same task must pass the guard on HTCondor."""
        task = MyGroupedTask(plain=0, grouped=(0, 1))
        process = self._create_process(task, "htcondor")
        self.assertIsInstance(process, HTCondorProcess)
