import unittest
from unittest.mock import MagicMock
from b2luigi.batch.workers import SendJobWorker, BatchSystems
from b2luigi.batch.processes.apptainer import ApptainerProcess


class CustomApptainerTask:
    process_class = ApptainerProcess
    apptainer_image = "/cvmfs/belle.cern.ch/images/belle2-base-el9"
    apptainer_mounts = ["/cvmfs"]
    apptainer_mount_defaults = True
    apptainer_additional_params = "--cleanenv"


class CustomTaskNoProcessClass:
    pass


class TestCustomProcess(unittest.TestCase):
    def setUp(self):
        self.mock_scheduler = MagicMock()
        self.mock_result_queue = MagicMock()
        self.mock_worker_timeout = MagicMock()
        self.worker = SendJobWorker(scheduler=self.mock_scheduler, worker_processes=1, assistant=False)
        # Patch worker internals for test
        self.worker._scheduler = self.mock_scheduler
        self.worker._task_result_queue = self.mock_result_queue
        self.worker._config = MagicMock(timeout=self.mock_worker_timeout)
        # Patch detect_batch_system to always return custom
        self.worker.detect_batch_system = MagicMock(return_value=BatchSystems.custom)

    def test_custom_process_class_used(self):
        task = CustomApptainerTask()
        process = self.worker._create_task_process(task)
        self.assertIsInstance(process, ApptainerProcess)
        self.assertIs(process.task, task)

    def test_missing_process_class_raises(self):
        task = CustomTaskNoProcessClass()
        with self.assertRaises(AttributeError):
            self.worker._create_task_process(task)
