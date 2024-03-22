import unittest
from unittest.mock import Mock
from b2luigi.batch.worker import SendJobWorker, BatchSystems

class TestSendJobWorker(unittest.TestCase):
    def setUp(self):
        self.worker = SendJobWorker()

    def test_create_task_process_lsf(self):
        task = Mock()
        task.get_setting.return_value = "lsf"
        process = self.worker._create_task_process(task)
        self.assertIsInstance(process, LSFProcess)

    def test_create_task_process_htcondor(self):
        task = Mock()
        task.get_setting.return_value = "htcondor"
        process = self.worker._create_task_process(task)
        self.assertIsInstance(process, HTCondorProcess)

    def test_create_task_process_gbasf2(self):
        task = Mock()
        task.get_setting.return_value = "gbasf2"
        process = self.worker._create_task_process(task)
        self.assertIsInstance(process, Gbasf2Process)

    def test_create_task_process_test(self):
        task = Mock()
        task.get_setting.return_value = "test"
        process = self.worker._create_task_process(task)
        self.assertIsInstance(process, TestProcess)

    def test_create_task_process_local(self):
        task = Mock()
        task.get_setting.return_value = "local"
        process = self.worker._create_task_process(task)
        self.assertIsInstance(process, LocalProcess)

    def test_create_task_process_not_implemented(self):
        task = Mock()
        task.get_setting.return_value = "unknown"
        with self.assertRaises(NotImplementedError):
            self.worker._create_task_process(task)

if __name__ == "__main__":
    unittest.main()