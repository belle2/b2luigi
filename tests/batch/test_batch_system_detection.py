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
        self.assertIn(batch_system, [BatchSystems.lsf, BatchSystems.htcondor, BatchSystems.local])

    def test_create_task_process_not_implemented(self):
        task = Mock()
        task.batch_system = "unknown"
        with self.assertRaises(ValueError):
            self.worker._create_task_process(task)


if __name__ == "__main__":
    unittest.main()
