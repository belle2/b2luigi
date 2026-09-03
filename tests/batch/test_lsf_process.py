"""
Tests for :py:class:`LSFProcess`, in particular parameter grouping.
"""

import os
from unittest import mock

import luigi

import b2luigi
from b2luigi.batch.processes import JobStatus
from b2luigi.batch.processes.lsf import LSFProcess, _batch_job_status_cache

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask
from .batch_task_grouped import MyGroupedTask


class TestLSFGroupedSubmission(B2LuigiTestCase):
    """
    Parameter grouping on LSF: one ``bsub`` per incomplete scalar sub-task,
    with the job status aggregated over all submitted ids.
    """

    def setUp(self):
        super().setUp()
        _batch_job_status_cache.clear()
        # Sub-task wrappers and logs must land in the temp dir: the defaults resolve relative to
        # the "main script", which under pytest on CI is a read-only externals installation.
        b2luigi.set_setting("log_dir", os.path.join(self.test_dir, "logs"))
        b2luigi.set_setting("task_file_dir", os.path.join(self.test_dir, "task_files"))

    def tearDown(self):
        b2luigi.clear_setting("log_dir")
        b2luigi.clear_setting("task_file_dir")
        _batch_job_status_cache.clear()
        super().tearDown()

    @staticmethod
    def _make_process(task):
        return LSFProcess(task=task, scheduler=mock.Mock(), result_queue=mock.Mock(), worker_timeout=None)

    @staticmethod
    def _mark_complete(task):
        output_file_name = task.get_output_file_name("grouped.txt")
        os.makedirs(os.path.dirname(output_file_name), exist_ok=True)
        with open(output_file_name, "w") as f:
            f.write("already done")

    @staticmethod
    def _bsub_output(job_id):
        return f"Job <{job_id}> is submitted to default queue <s>.".encode()

    @mock.patch("subprocess.check_output")
    def test_one_bsub_per_sub_task(self, check_output):
        check_output.side_effect = [self._bsub_output(101), self._bsub_output(102), self._bsub_output(103)]
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1, 2)))

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["101", "102", "103"])
        self.assertEqual(check_output.call_count, 3)
        for value, call in zip((0, 1, 2), check_output.call_args_list):
            command = call.args[0]
            self.assertEqual(command[0], "bsub")
            self.assertIn(f"grouped={value}", command[-1])
            self.assertTrue(command[-1].startswith(self.test_dir), command[-1])

    @mock.patch("subprocess.check_output")
    def test_ungrouped_task_is_submitted_once(self, check_output):
        check_output.return_value = self._bsub_output(7)
        process = self._make_process(MyTask("some_parameter"))

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["7"])
        self.assertEqual(check_output.call_count, 1)

    @mock.patch("subprocess.check_output")
    def test_complete_sub_tasks_are_not_submitted(self, check_output):
        check_output.side_effect = [self._bsub_output(101), self._bsub_output(103)]
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        self._mark_complete(task.clone(None, grouped=1))
        process = self._make_process(task)

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["101", "103"])
        executables = [call.args[0][-1] for call in check_output.call_args_list]
        self.assertFalse(any("grouped=1" in executable for executable in executables))

    @mock.patch("subprocess.check_output")
    def test_fully_complete_group_reports_done_without_submitting(self, check_output):
        task = MyGroupedTask(plain=0, grouped=(0, 1))
        for value in (0, 1):
            self._mark_complete(task.clone(None, grouped=value))
        process = self._make_process(task)

        process.start_job()

        check_output.assert_not_called()
        self.assertTrue(process._terminated)
        process._result_queue.put.assert_called_once()
        self.assertEqual(process._result_queue.put.call_args.args[0][1], luigi.scheduler.DONE)

    def test_status_is_aggregated_over_all_jobs(self):
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1, 2)))
        process._batch_job_ids = ["101", "102", "103"]

        _batch_job_status_cache["101"] = "DONE"
        _batch_job_status_cache["102"] = "RUN"
        _batch_job_status_cache["103"] = "EXIT"
        self.assertEqual(process.get_job_status(), JobStatus.running)

        _batch_job_status_cache["102"] = "DONE"
        self.assertEqual(process.get_job_status(), JobStatus.aborted)

        _batch_job_status_cache["103"] = "DONE"
        self.assertEqual(process.get_job_status(), JobStatus.successful)

    def test_status_without_job_ids_is_aborted(self):
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))
        self.assertEqual(process.get_job_status(), JobStatus.aborted)

    @mock.patch("subprocess.run")
    def test_terminate_kills_every_job(self, run):
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))
        process._batch_job_ids = ["101", "102"]

        process.terminate_job()

        run.assert_called_once()
        self.assertEqual(run.call_args.args[0], ["bkill", "101", "102"])
