"""
Test helper functions for :py:class:`SlurmProcess`.
"""

import os
import pathlib

import subprocess
import unittest
from unittest import mock

import luigi

import b2luigi
from b2luigi.batch.processes import JobStatus
from b2luigi.core.utils import get_log_file_dir
from b2luigi.batch.processes.slurm import (
    SlurmJobStatusCache,
    SlurmProcess,
    SlurmJobStatus,
    _batch_job_status_cache,
)

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask
from .batch_task_grouped import MyGroupedTask


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

    @mock.patch("subprocess.run")
    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_treats_invalid_job_id_as_not_found(self, mock_check_output, mock_run):
        """
        Test that a squeue failure with "Invalid job id specified" (raised when a job has already
        been purged from Slurm's live job table) is treated as "no jobs seen" instead of being
        retried/raised, falling through to the sacct lookup.
        """
        # _check_if_sacct_is_disabled_on_server calls subprocess.run(["sacct"]) directly; mock it
        # to report accounting as enabled (returncode 0) so the code takes the sacct history_cmd
        # path below, which goes through the mocked subprocess.check_output instead.
        mock_run.return_value = mock.Mock(returncode=0, stderr=b"")
        squeue_error = subprocess.CalledProcessError(1, ["mock", "squeue"])
        squeue_error.stderr = b"slurm_load_jobs error: Invalid job id specified"
        mock_check_output.side_effect = [squeue_error, self.mock_status_string]
        self.slurm_job_status_cache._ask_for_job_status(job_id="12344")
        self.assertEqual(mock_check_output.call_count, 2)
        self.assertEqual(self.slurm_job_status_cache["12344"].value, "COMPLETED")

    @mock.patch("subprocess.check_output")
    def test_ask_for_job_status_accepts_array_job_ids(self, mock_check_output):
        """
        squeue reports every job of the user, including job arrays b2luigi did not submit itself.
        Their ids (e.g. ``123456_0``) are not integers and must not crash the status query
        (https://github.com/belle2/b2luigi/issues/5).
        """
        mock_check_output.return_value = b"12344 COMPLETED\n123456_0 RUNNING\n123456_1 PENDING\n"
        self.slurm_job_status_cache._ask_for_job_status()
        self.assertEqual(self.slurm_job_status_cache["12344"], SlurmJobStatus.completed)
        self.assertEqual(self.slurm_job_status_cache["123456_0"], SlurmJobStatus.running)
        self.assertEqual(self.slurm_job_status_cache["123456_1"], SlurmJobStatus.pending)

    def test_fill_from_output_expands_pending_array_elements(self):
        """
        Array elements that have not started yet are summarised by Slurm in one line. Every element
        must still be found under its own id, otherwise a pending element counts as unknown (aborted).
        """
        output = "123_[0-2,5,8-12:2%3] PENDING\n123_3 RUNNING\n456 RUNNING\n"
        seen_ids = self.slurm_job_status_cache._fill_from_output(output)

        pending = ["123_0", "123_1", "123_2", "123_5", "123_8", "123_10", "123_12"]
        self.assertEqual(seen_ids, {*pending, "123_3", "456"})
        for job_id in pending:
            self.assertEqual(self.slurm_job_status_cache[job_id], SlurmJobStatus.pending)
        self.assertEqual(self.slurm_job_status_cache["123_3"], SlurmJobStatus.running)
        self.assertEqual(self.slurm_job_status_cache["456"], SlurmJobStatus.running)

    def test_expand_array_job_id_leaves_other_ids_alone(self):
        self.assertEqual(SlurmJobStatusCache._expand_array_job_id("456"), ["456"])
        self.assertEqual(SlurmJobStatusCache._expand_array_job_id("123_4"), ["123_4"])
        self.assertEqual(SlurmJobStatusCache._expand_array_job_id("123_[7]"), ["123_7"])


class TestSlurmJobStatus:
    def test_strenum_comparison_with_string(self):
        """Test that SlurmJobStatus members can be compared with strings directly."""
        assert SlurmJobStatus.configuring == "CONFIGURING"
        assert SlurmJobStatus.running == "RUNNING"
        assert SlurmJobStatus.completed == "COMPLETED"

    def test_strenum_value_is_string(self):
        """Test that SlurmJobStatus values are strings."""
        assert isinstance(SlurmJobStatus.configuring.value, str)
        assert isinstance(SlurmJobStatus.running.value, str)

    def test_strenum_creation_from_string(self):
        """Test that SlurmJobStatus can be created from a string value."""
        status = SlurmJobStatus("CONFIGURING")
        assert status == SlurmJobStatus.configuring


class TestSlurmGroupedSubmission(B2LuigiTestCase):
    """
    Parameter grouping on Slurm: one ``sbatch`` per incomplete scalar sub-task,
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
        return SlurmProcess(task=task, scheduler=mock.Mock(), result_queue=mock.Mock(), worker_timeout=None)

    @staticmethod
    def _mark_complete(task):
        output_file_name = task.get_output_file_name("grouped.txt")
        os.makedirs(os.path.dirname(output_file_name), exist_ok=True)
        with open(output_file_name, "w") as f:
            f.write("already done")

    @mock.patch("subprocess.check_output")
    def test_one_sbatch_per_sub_task(self, check_output):
        check_output.side_effect = [b"Submitted batch job 101", b"Submitted batch job 102", b"Submitted batch job 103"]
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1, 2)))

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["101", "102", "103"])
        self.assertEqual(check_output.call_count, 3)
        for value, call in zip((0, 1, 2), check_output.call_args_list):
            self.assertEqual(call.args[0][0], "sbatch")
            self.assertIn(f"grouped={value}", str(call.kwargs["cwd"]))
            self.assertTrue(str(call.kwargs["cwd"]).startswith(self.test_dir), call.kwargs["cwd"])

    @mock.patch("subprocess.check_output")
    def test_ungrouped_task_is_submitted_once(self, check_output):
        check_output.return_value = b"Submitted batch job 7"
        process = self._make_process(MyTask("some_parameter"))

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["7"])
        self.assertEqual(check_output.call_count, 1)

    @mock.patch("subprocess.check_output")
    def test_complete_sub_tasks_are_not_submitted(self, check_output):
        check_output.side_effect = [b"Submitted batch job 101", b"Submitted batch job 103"]
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        self._mark_complete(task.clone(None, grouped=1))
        process = self._make_process(task)

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["101", "103"])
        cwds = [str(call.kwargs["cwd"]) for call in check_output.call_args_list]
        self.assertFalse(any("grouped=1" in cwd for cwd in cwds))

    @mock.patch("subprocess.check_output")
    def test_new_job_status_is_served_from_cache(self, check_output):
        """A freshly submitted job is seeded as pending, so its first status check must not call ``squeue``."""
        check_output.return_value = b"Submitted batch job 7"
        process = self._make_process(MyGroupedTask(plain=0, grouped=7))

        process.start_job()

        self.assertEqual(process.get_job_status(), JobStatus.running)
        check_output.assert_called_once()
        self.assertEqual(check_output.call_args.args[0][0], "sbatch")

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

        _batch_job_status_cache["101"] = SlurmJobStatus.completed
        _batch_job_status_cache["102"] = SlurmJobStatus.running
        _batch_job_status_cache["103"] = SlurmJobStatus.failed
        self.assertEqual(process.get_job_status(), JobStatus.running)

        _batch_job_status_cache["102"] = SlurmJobStatus.completed
        self.assertEqual(process.get_job_status(), JobStatus.aborted)

        _batch_job_status_cache["103"] = SlurmJobStatus.completed
        self.assertEqual(process.get_job_status(), JobStatus.successful)

    def test_status_without_job_ids_is_aborted(self):
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))
        self.assertEqual(process.get_job_status(), JobStatus.aborted)

    @mock.patch("subprocess.check_output")
    def test_aborted_group_writes_failed_jobs_log_in_group_log_dir(self, check_output):
        """
        ``on_failure`` points the user at the *group* task's log directory, but every sub-task
        logs into its own scalar directory. The aborted branch must leave a ``failed_jobs.log``
        in the group directory that names the failed job ids and their log directories.
        """
        check_output.side_effect = [b"Submitted batch job 101", b"Submitted batch job 102", b"Submitted batch job 103"]
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        process = self._make_process(task)
        process.start_job()

        _batch_job_status_cache["101"] = SlurmJobStatus.completed
        _batch_job_status_cache["102"] = SlurmJobStatus.failed
        _batch_job_status_cache["103"] = SlurmJobStatus.completed
        self.assertEqual(process.get_job_status(), JobStatus.aborted)

        failed_jobs_log = os.path.join(get_log_file_dir(task), "failed_jobs.log")
        self.assertTrue(os.path.isfile(failed_jobs_log), failed_jobs_log)
        with open(failed_jobs_log) as f:
            lines = f.read().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("102: "), lines[0])
        self.assertEqual(lines[0], f"102: {get_log_file_dir(task.clone(None, grouped=1))}")

    @mock.patch("subprocess.run")
    def test_terminate_cancels_every_job(self, run):
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))
        process._batch_job_ids = ["101", "102"]

        process.terminate_job()

        run.assert_called_once()
        self.assertEqual(run.call_args.args[0], ["scancel", "101", "102"])


class TestSlurmArrayAndMpiSubmission(B2LuigiTestCase):
    """
    ``submission_type`` "array" and "mpi": a group is submitted as one Slurm job, dispatched to the
    sub-tasks by ``$SLURM_ARRAY_TASK_ID`` or ``$SLURM_PROCID``. Array elements are tracked one by one.
    """

    def setUp(self):
        super().setUp()
        _batch_job_status_cache.clear()
        b2luigi.set_setting("log_dir", os.path.join(self.test_dir, "logs"))
        b2luigi.set_setting("task_file_dir", os.path.join(self.test_dir, "task_files"))

    def tearDown(self):
        for setting in ("log_dir", "task_file_dir", "submission_type", "tasks_per_node", "slurm_settings"):
            b2luigi.clear_setting(setting)
        _batch_job_status_cache.clear()
        super().tearDown()

    _make_process = staticmethod(TestSlurmGroupedSubmission._make_process)
    _mark_complete = staticmethod(TestSlurmGroupedSubmission._mark_complete)

    @staticmethod
    def _read_submitted_files(check_output):
        """Return the lines of the submit file and of the dispatch file of the (only) ``sbatch`` call."""
        submit_dir = pathlib.Path(check_output.call_args.kwargs["cwd"])
        submit_lines = (submit_dir / check_output.call_args.args[0][1]).read_text().splitlines()
        dispatch_lines = (submit_dir / "slurm_dispatch.sh").read_text().splitlines()
        return submit_lines, dispatch_lines

    @staticmethod
    def _log_dir(task):
        return pathlib.Path(get_log_file_dir(task)).resolve()

    @mock.patch("subprocess.check_output")
    def test_array_is_submitted_as_one_job_array(self, check_output):
        b2luigi.set_setting("submission_type", "array")
        check_output.return_value = b"Submitted batch job 101"
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        process = self._make_process(task)

        process.start_job()

        check_output.assert_called_once()
        self.assertEqual(process._batch_job_ids, ["101_0", "101_1", "101_2"])
        submit_lines, dispatch_lines = self._read_submitted_files(check_output)
        self.assertIn("#SBATCH --array=0-2", submit_lines)
        dispatch_file = (pathlib.Path(check_output.call_args.kwargs["cwd"]) / "slurm_dispatch.sh").resolve()
        # Started by its absolute path, so that it does not depend on PATH (unset with export=NONE)
        self.assertEqual(submit_lines[-1], f"exec {dispatch_file}")
        self.assertTrue(os.access(dispatch_file, os.X_OK))
        self.assertIn("case $SLURM_ARRAY_TASK_ID in", dispatch_lines)
        for index, value in enumerate((0, 1, 2)):
            log_dir = self._log_dir(task.clone(None, grouped=value))
            case_line = dispatch_lines.index(f"  {index})")
            self.assertTrue(
                dispatch_lines[case_line + 1].endswith(f"> {log_dir / 'stdout'} 2> {log_dir / 'stderr'}"),
                dispatch_lines[case_line + 1],
            )
            self.assertEqual(process._job_log_dirs[f"101_{index}"], get_log_file_dir(task.clone(None, grouped=value)))

    @mock.patch("subprocess.check_output")
    def test_new_array_elements_are_served_from_cache(self, check_output):
        b2luigi.set_setting("submission_type", "array")
        check_output.return_value = b"Submitted batch job 101"
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))

        process.start_job()

        self.assertEqual(process.get_job_status(), JobStatus.running)
        check_output.assert_called_once()

    @mock.patch("subprocess.check_output")
    def test_complete_sub_tasks_are_left_out_of_the_array(self, check_output):
        b2luigi.set_setting("submission_type", "array")
        check_output.return_value = b"Submitted batch job 101"
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        self._mark_complete(task.clone(None, grouped=1))
        process = self._make_process(task)

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["101_0", "101_1"])
        submit_lines, dispatch_lines = self._read_submitted_files(check_output)
        self.assertIn("#SBATCH --array=0-1", submit_lines)
        # The indices are renumbered: element 1 runs the sub-task with grouped=2.
        log_dir = self._log_dir(task.clone(None, grouped=2))
        self.assertIn(str(log_dir / "stdout"), dispatch_lines[dispatch_lines.index("  1)") + 1])
        self.assertEqual(process._job_log_dirs["101_1"], get_log_file_dir(task.clone(None, grouped=2)))

    @mock.patch("subprocess.check_output")
    def test_array_status_is_tracked_per_element(self, check_output):
        b2luigi.set_setting("submission_type", "array")
        check_output.return_value = b"Submitted batch job 101"
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        process = self._make_process(task)
        process.start_job()

        _batch_job_status_cache["101_0"] = SlurmJobStatus.completed
        _batch_job_status_cache["101_1"] = SlurmJobStatus.failed
        _batch_job_status_cache["101_2"] = SlurmJobStatus.running
        self.assertEqual(process.get_job_status(), JobStatus.running)

        _batch_job_status_cache["101_2"] = SlurmJobStatus.completed
        self.assertEqual(process.get_job_status(), JobStatus.aborted)

        with open(os.path.join(get_log_file_dir(task), "failed_jobs.log")) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines, [f"101_1: {get_log_file_dir(task.clone(None, grouped=1))}"])

    @mock.patch("subprocess.check_output")
    def test_mpi_is_submitted_as_one_multi_node_job(self, check_output):
        b2luigi.set_setting("submission_type", "mpi")
        b2luigi.set_setting("tasks_per_node", 2)
        check_output.return_value = b"Submitted batch job 101"
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        process = self._make_process(task)

        process.start_job()

        check_output.assert_called_once()
        self.assertEqual(process._batch_job_ids, ["101"])
        submit_lines, dispatch_lines = self._read_submitted_files(check_output)
        self.assertIn("#SBATCH --nodes=2", submit_lines)
        self.assertIn("#SBATCH --ntasks=3", submit_lines)
        dispatch_file = (pathlib.Path(check_output.call_args.kwargs["cwd"]) / "slurm_dispatch.sh").resolve()
        # srun starts the dispatch file by its absolute path; without PATH it could not find "bash"
        self.assertEqual(submit_lines[-1], f"exec srun {dispatch_file}")
        self.assertTrue(os.access(dispatch_file, os.X_OK))
        self.assertIn("case $SLURM_PROCID in", dispatch_lines)

    @mock.patch("subprocess.check_output")
    def test_failed_mpi_job_lists_every_sub_task_in_failed_jobs_log(self, check_output):
        b2luigi.set_setting("submission_type", "mpi")
        check_output.return_value = b"Submitted batch job 101"
        task = MyGroupedTask(plain=0, grouped=(0, 1))
        process = self._make_process(task)
        process.start_job()

        _batch_job_status_cache["101"] = SlurmJobStatus.failed
        self.assertEqual(process.get_job_status(), JobStatus.aborted)

        with open(os.path.join(get_log_file_dir(task), "failed_jobs.log")) as f:
            lines = f.read().splitlines()
        self.assertEqual(
            lines,
            [
                f"101 (task {index}): {get_log_file_dir(task.clone(None, grouped=value))}"
                for index, value in enumerate((0, 1))
            ],
        )

    @mock.patch("subprocess.check_output")
    def test_mpi_overrides_ntasks_from_slurm_settings(self, check_output):
        b2luigi.set_setting("submission_type", "mpi")
        b2luigi.set_setting("slurm_settings", {"ntasks": 1, "partition": "some_partition"})
        check_output.return_value = b"Submitted batch job 101"
        self._make_process(MyGroupedTask(plain=0, grouped=(0, 1))).start_job()

        submit_lines, _ = self._read_submitted_files(check_output)
        self.assertIn("#SBATCH --ntasks=2", submit_lines)
        self.assertNotIn("#SBATCH --ntasks=1", submit_lines)
        self.assertIn("#SBATCH --partition=some_partition", submit_lines)
        # The group settings must not end up in the setting itself, i.e. in later submissions.
        self.assertEqual(b2luigi.get_setting("slurm_settings"), {"ntasks": 1, "partition": "some_partition"})

    @mock.patch("subprocess.check_output")
    def test_ungrouped_task_ignores_the_submission_type(self, check_output):
        b2luigi.set_setting("submission_type", "array")
        check_output.return_value = b"Submitted batch job 7"
        process = self._make_process(MyTask("some_parameter"))

        process.start_job()

        self.assertEqual(process._batch_job_ids, ["7"])
        submit_dir = pathlib.Path(check_output.call_args.kwargs["cwd"])
        submit_file = (submit_dir / "slurm_parameters.sh").read_text()
        self.assertNotIn("--array", submit_file)
        self.assertFalse((submit_dir / "slurm_dispatch.sh").exists())

    @mock.patch("subprocess.check_output")
    def test_unknown_submission_type_raises(self, check_output):
        b2luigi.set_setting("submission_type", "parallel")
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))

        with self.assertRaises(ValueError) as ctx:
            process.start_job()
        self.assertIn("parallel", str(ctx.exception))
        check_output.assert_not_called()

    @mock.patch("subprocess.check_output")
    def test_tasks_per_node_below_one_raises(self, check_output):
        b2luigi.set_setting("submission_type", "mpi")
        b2luigi.set_setting("tasks_per_node", 0)
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))

        with self.assertRaises(ValueError):
            process.start_job()
        check_output.assert_not_called()

    @mock.patch("subprocess.run")
    def test_terminate_cancels_an_array_via_its_job_id(self, run):
        process = self._make_process(MyGroupedTask(plain=0, grouped=(0, 1)))
        process._batch_job_ids = ["101_0", "101_1", "102"]

        process.terminate_job()

        run.assert_called_once()
        self.assertEqual(run.call_args.args[0], ["scancel", "101", "102"])
