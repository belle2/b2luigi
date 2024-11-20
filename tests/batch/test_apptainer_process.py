import unittest
import subprocess
from unittest.mock import patch, MagicMock
from b2luigi.batch.processes.apptainer import ApptainerProcess
from b2luigi.batch.processes import JobStatus
from .batch_task_1 import MyTask


class MyApptainerTask(MyTask):
    apptainer_image = "/cvmfs/belle.cern.ch/images/belle2-base-el9"
    apptainer_mounts = ["/cvmfs"]
    apptainer_mount_defaults = True
    apptainer_additional_params = "--cleanenv"


class TestApptainerProcess(unittest.TestCase):
    def setUp(self):
        self.mock_task = MyApptainerTask("some_parameter")
        self.mock_scheduler = MagicMock()
        self.mock_result_queue = MagicMock()
        self.mock_worker_timeout = MagicMock()
        self.process = ApptainerProcess(
            task=self.mock_task,
            scheduler=self.mock_scheduler,
            result_queue=self.mock_result_queue,
            worker_timeout=self.mock_worker_timeout,
        )

    @patch("b2luigi.batch.processes.apptainer.create_cmd_from_task")
    @patch("b2luigi.batch.processes.apptainer.create_apptainer_command")
    @patch("subprocess.Popen")
    def test_start_job(self, mock_popen, mock_create_apptainer_command, mock_create_cmd_from_task):
        mock_create_cmd_from_task.return_value = ["echo", "hello"]
        mock_create_apptainer_command.return_value = "apptainer exec echo hello"
        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        self.process.start_job()

        mock_create_cmd_from_task.assert_called_once_with(self.mock_task)
        mock_create_apptainer_command.assert_called_once_with("echo hello", task=self.mock_task)
        mock_popen.assert_called_once_with("apptainer exec echo hello", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(self.process._process, mock_process)

    @patch("b2luigi.batch.processes.apptainer.get_log_file_dir")
    def test_write_output(self, mock_get_log_file_dir):
        mock_get_log_file_dir.return_value = "/tmp"
        self.process._stdout = b"stdout content"
        self.process._stderr = b"stderr content"

        self.process._write_output()

        with open("/tmp/stdout", "r") as f:
            self.assertEqual(f.read(), "stdout content")

        with open("/tmp/stderr", "r") as f:
            self.assertEqual(f.read(), "stderr content")

    def test_get_job_status_running(self):
        self.process._process = MagicMock()
        self.process._process.poll.return_value = None

        status = self.process.get_job_status()
        self.assertEqual(status, JobStatus.running)

    @patch("b2luigi.batch.processes.apptainer.ApptainerProcess._write_output")
    def test_get_job_status_successful(self, mock_write_output):
        mock_write_output.return_value = True
        self.process._process = MagicMock()
        self.process._process.poll.return_value = 0
        self.process._process.communicate.return_value = (b"stdout content", b"stderr content")
        self.process._process.returncode = 0

        status = self.process.get_job_status()
        self.assertEqual(status, JobStatus.successful)

    @patch("b2luigi.batch.processes.apptainer.ApptainerProcess._write_output")
    def test_get_job_status_aborted(self, mock_write_output):
        self.process._process = None
        status = self.process.get_job_status()
        self.assertEqual(status, JobStatus.aborted)

        mock_write_output.return_value = True
        self.process._process = MagicMock()
        self.process._process.poll.return_value = 1
        self.process._process.communicate.return_value = (b"stdout content", b"stderr content")
        self.process._process.returncode = 1

        status = self.process.get_job_status()
        self.assertEqual(status, JobStatus.aborted)

    def test_terminate_job(self):
        self.process._process = MagicMock()
        self.process.terminate_job()
        self.process._process.terminate.assert_called_once()
