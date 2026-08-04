import shlex
import unittest
from unittest.mock import patch, MagicMock

import luigi

from b2luigi.batch.processes.apptainer import ApptainerProcess
from b2luigi.batch.processes import JobStatus
from b2luigi.cli.utils import split_kv_params
from b2luigi.core.settings import clear_setting, set_setting
from .batch_task_1 import MyTask


class MyApptainerTask(MyTask):
    apptainer_image = "/cvmfs/belle.cern.ch/images/belle2-base-el9"
    apptainer_mounts = ["/cvmfs"]
    apptainer_mount_defaults = True
    apptainer_additional_params = "--cleanenv"
    env_script = "/env.sh"
    some_list_parameter = luigi.ListParameter()
    some_spacey_parameter = luigi.Parameter()


class TestApptainerProcess(unittest.TestCase):
    def setUp(self):
        self.mock_task = MyApptainerTask(
            some_parameter="some_parameter",
            some_list_parameter=[1, 2, 3],
            some_spacey_parameter="value with spaces",
        )
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
        """Popen receives a real argv list whose bash payload is a single element, and the
        ``--param`` tokens inside that payload round-trip back to the submitter's ``task_id``
        even though one parameter contains spaces and another is a list.
        """
        set_setting("__batch_runner_use_cli", True)
        self.addCleanup(clear_setting, "__batch_runner_use_cli")
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

        # The payload is what would be handed to `bash -c`. Split it exactly like bash
        # would, pull out the --param tokens, and reconstruct the task on the "worker"
        # side to confirm it survives the round trip byte-for-byte.
        payload = argv[-1]
        payload_argv = shlex.split(payload)
        params = [
            payload_argv[i + 1]
            for i, token in enumerate(payload_argv)
            if token == "--param" and i + 1 < len(payload_argv)
        ]
        self.assertTrue(params, "expected at least one --param token in the payload")

        reconstructed = MyApptainerTask.from_str_params(split_kv_params(params))
        self.assertEqual(reconstructed.task_id, self.mock_task.task_id)

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
