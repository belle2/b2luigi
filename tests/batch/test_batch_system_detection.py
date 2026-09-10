import unittest
from unittest.mock import Mock
from b2luigi.batch.workers import SendJobWorker, BatchSystems


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


if __name__ == "__main__":
    unittest.main()


class TestGroupingGuard(unittest.TestCase):
    """``_create_task_process`` must reject grouping on batch systems that cannot expand a group."""

    def setUp(self):
        self.worker = SendJobWorker()

    @staticmethod
    def _grouped_task(batch_system):
        task = Mock()
        task.batch_system = batch_system
        task.has_grouped_params.return_value = True
        task.max_grouping_size = 5
        return task

    def test_gbasf2_rejects_grouping(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.worker._create_task_process(self._grouped_task("gbasf2"))
        self.assertIn("gbasf2", str(ctx.exception))

    def test_lsf_slurm_htcondor_accept_grouping(self):
        for batch_system in ("lsf", "slurm", "htcondor"):
            with self.subTest(batch_system=batch_system):
                process = self.worker._create_task_process(self._grouped_task(batch_system))
                self.assertEqual(process.task.batch_system, batch_system)
