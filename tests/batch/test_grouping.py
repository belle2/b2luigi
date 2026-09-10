"""
Tests for the batch-system-independent parameter grouping helpers in :mod:`b2luigi.batch.processes`.
"""

import os

from b2luigi.batch.processes import JobStatus, aggregate_job_status, expand_grouped_task

from ..helpers import B2LuigiTestCase
from .batch_task_1 import MyTask
from .batch_task_grouped import MyGroupedTask


def _mark_complete(task):
    output_file_name = task.get_output_file_name("grouped.txt")
    os.makedirs(os.path.dirname(output_file_name), exist_ok=True)
    with open(output_file_name, "w") as f:
        f.write("already done")
    assert task.complete()


class TestExpandGroupedTask(B2LuigiTestCase):
    def test_task_without_grouped_params_is_returned_as_is(self):
        task = MyTask("some_parameter")
        self.assertEqual(expand_grouped_task(task), [task])

    def test_scalar_grouped_value_is_returned_as_is(self):
        task = MyGroupedTask(plain=0, grouped=7)
        self.assertEqual(expand_grouped_task(task), [task])

    def test_tuple_grouped_value_expands_into_one_scalar_task_per_value(self):
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        sub_tasks = expand_grouped_task(task)
        self.assertEqual([t.grouped for t in sub_tasks], [0, 1, 2])
        self.assertEqual([t.plain for t in sub_tasks], [0, 0, 0])
        self.assertNotIn(task, sub_tasks)

    def test_complete_sub_tasks_are_left_out(self):
        task = MyGroupedTask(plain=0, grouped=(0, 1, 2))
        _mark_complete(task.clone(None, grouped=1))
        self.assertEqual([t.grouped for t in expand_grouped_task(task)], [0, 2])

    def test_fully_complete_group_expands_to_nothing(self):
        task = MyGroupedTask(plain=0, grouped=(0, 1))
        for value in (0, 1):
            _mark_complete(task.clone(None, grouped=value))
        self.assertEqual(expand_grouped_task(task), [])


class TestAggregateJobStatus(B2LuigiTestCase):
    def test_all_successful(self):
        self.assertEqual(aggregate_job_status([JobStatus.successful, JobStatus.successful]), JobStatus.successful)

    def test_one_running_keeps_the_group_running(self):
        self.assertEqual(
            aggregate_job_status([JobStatus.successful, JobStatus.running, JobStatus.aborted]), JobStatus.running
        )

    def test_one_aborted_aborts_the_group_once_nothing_runs(self):
        self.assertEqual(aggregate_job_status([JobStatus.successful, JobStatus.aborted]), JobStatus.aborted)

    def test_empty_list_is_aborted(self):
        self.assertEqual(aggregate_job_status([]), JobStatus.aborted)
