import unittest
from unittest.mock import Mock, patch

import luigi.worker

import b2luigi
from b2luigi.batch.workers import SendJobWorker, BatchSystems, supports_grouping
from b2luigi.core.settings import clear_setting, set_setting

from .batch_task_1 import MyTask
from .batch_task_grouped import MyGroupedTask


class TestSendJobWorker(unittest.TestCase):
    def setUp(self):
        self.worker = SendJobWorker()

    def test_create_task_process_lsf(self):
        task = Mock()
        task.batch_system = "lsf"
        batch_system = self.worker.detect_batch_system(task)
        self.assertEqual(batch_system, BatchSystems.lsf)

    def test_create_task_process_htcondor(self):
        task = Mock()
        task.batch_system = "htcondor"
        batch_system = self.worker.detect_batch_system(task)
        self.assertEqual(batch_system, BatchSystems.htcondor)

    def test_create_task_process_slurm(self):
        task = Mock()
        task.batch_system = "slurm"
        batch_system = self.worker.detect_batch_system(task)
        self.assertEqual(batch_system, BatchSystems.slurm)

    def test_create_task_process_gbasf2(self):
        task = Mock()
        task.batch_system = "gbasf2"
        batch_system = self.worker.detect_batch_system(task)
        self.assertEqual(batch_system, BatchSystems.gbasf2)

    def test_create_task_process_test(self):
        task = Mock()
        task.batch_system = "test"
        batch_system = self.worker.detect_batch_system(task)
        self.assertEqual(batch_system, BatchSystems.test)

    def test_create_process_auto(self):
        task = Mock()
        task.batch_system = "auto"
        batch_system = self.worker.detect_batch_system(task)
        self.assertIn(batch_system, [BatchSystems.lsf, BatchSystems.htcondor, BatchSystems.slurm, BatchSystems.local])

    def test_create_task_process_not_implemented(self):
        task = Mock()
        task.batch_system = "unknown"
        with self.assertRaises(ValueError):
            self.worker._create_task_process(task)


class LocalGroupedTask(MyGroupedTask):
    batch_system = "local"


class SlurmGroupedTask(MyGroupedTask):
    batch_system = "slurm"


class TestGroupingIsSkippedWhereItCannotBeUnpacked(unittest.TestCase):
    """
    Grouping is only formed where someone can expand it again. Everywhere else the
    group must never come into existence, so that a task can declare ``grouping=True``
    permanently and still be run e.g. locally.
    """

    def tearDown(self):
        clear_setting("_dispatch_local_execution")

    def test_a_grouped_task_is_not_batchable_on_the_local_batch_system(self):
        self.assertFalse(LocalGroupedTask(plain=0, grouped=0).batchable)

    def test_a_grouped_task_is_batchable_on_slurm(self):
        self.assertTrue(SlurmGroupedTask(plain=0, grouped=0).batchable)

    def test_the_decision_is_per_task_not_per_task_family(self):
        """
        ``luigi`` registers one ``max_batch_size`` per task family, so a family whose
        batch system varies with a parameter can only be handled task by task.
        """

        class MixedTask(b2luigi.Task):
            stage = b2luigi.Parameter(default="local_stage")
            grouped = b2luigi.BatchIntParameter(default=0, grouping=True)

            max_grouping_size = 10

            @property
            def batch_system(self):
                return {"local_stage": "local", "slurm_stage": "slurm"}[self.stage]

        local_task = MixedTask(stage="local_stage")
        slurm_task = MixedTask(stage="slurm_stage")

        self.assertEqual(local_task.get_task_family(), slurm_task.get_task_family())
        self.assertFalse(local_task.batchable)
        self.assertTrue(slurm_task.batchable)

    def test_a_task_without_grouped_parameters_keeps_the_luigi_behaviour(self):
        self.assertFalse(MyTask(some_parameter=0).batchable)

    def test_nothing_is_grouped_in_test_mode(self):
        """``b2luigi.process(..., test=True)`` runs every task in a plain luigi worker."""
        set_setting("_dispatch_local_execution", True)
        self.assertFalse(SlurmGroupedTask(plain=0, grouped=0).batchable)
        self.assertFalse(supports_grouping(SlurmGroupedTask(plain=0, grouped=0)))

    def test_a_grouped_task_is_dispatched_locally_without_raising(self):
        """
        This is the case that used to fail with a RuntimeError: a task declaring grouped
        parameters, going to the local batch system, holding a plain scalar value.
        """
        # A real worker, because the local branch delegates to luigi via zero argument super().
        worker = SendJobWorker()
        task = LocalGroupedTask(plain=0, grouped=0)

        with patch("b2luigi.batch.workers.create_output_dirs") as create_dirs, patch.object(
            luigi.worker.Worker, "_create_task_process", return_value="luigi process"
        ):
            self.assertEqual(worker._create_task_process(task), "luigi process")

        create_dirs.assert_called_once_with(task)


if __name__ == "__main__":
    unittest.main()
