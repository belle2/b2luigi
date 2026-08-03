import unittest
from unittest.mock import patch, MagicMock
from b2luigi.batch.processes.apptainer import ApptainerProcess
from b2luigi.batch.processes import JobStatus
from .batch_task_1 import MyTask


class MyApptainerTask(MyTask):
    apptainer_image = "/cvmfs/belle.cern.ch/images/belle2-base-el9"
    apptainer_mounts = ["/cvmfs"]
    apptainer_mount_defaults = True
    apptainer_additional_params = "--cleanenv"
    env_script = "/env.sh"


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

    @patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer")
    @patch("b2luigi.core.utils.map_folder", return_value="/tmp/results")
    @patch("b2luigi.core.utils.get_log_file_dir", return_value="/tmp/logs")
    @patch("os.makedirs")
    @patch("subprocess.Popen")
    def test_start_job_passes_payload_as_one_argv_element(
        self, mock_popen, _mock_makedirs, _mock_log_dir, _mock_map_folder, _mock_ap
    ):
        """Popen receives a real argv list whose bash payload is a single element."""
        mock_popen.return_value = MagicMock()

        self.process.start_job()

        argv = mock_popen.call_args[0][0]
        self.assertIsInstance(argv, list)
        self.assertEqual(argv[-3], "/bin/bash")
        self.assertEqual(argv[-2], "-c")
        self.assertFalse(argv[-1].startswith("'"))
        self.assertTrue(argv[-1].startswith("source /env.sh && "))
        self.assertNotIn("&&", argv[:-1])
        # apptainer_additional_params must be word-split, not one " --cleanenv" element
        self.assertIn("--cleanenv", argv)

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
