import os
import subprocess

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import create_cmd_from_task, create_apptainer_command, get_log_file_dir


class ApptainerProcess(BatchProcess):
    """
    Simple implementation of a batch process for running jobs in an Apptainer container.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._process = None
        self._sdtout = None
        self._stderr = None

    def get_job_status(self):
        if self._process is None:
            return JobStatus.aborted

        # Poll the process to check if it is still running
        if self._process.poll() is None:
            return JobStatus.running
        else:
            # If the process has finished, write output and return the appropriate status
            self._stdout, self._stderr = self._process.communicate()
            self._write_output()
            return JobStatus.successful if self._process.returncode == 0 else JobStatus.aborted

    def start_job(self):
        command = " ".join(create_cmd_from_task(self.task))
        exec_command = create_apptainer_command(command, task=self.task)

        # Start the job and capture the job ID
        self._process = subprocess.Popen(exec_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def terminate_job(self):
        if self._process is not None:
            self._process.terminate()

    def _write_output(self):
        log_file_dir = get_log_file_dir(self.task)

        with open(os.path.join(log_file_dir, "stdout"), "w") as f:
            if self._stdout is not None:
                f.write(self._stdout.decode())

        with open(os.path.join(log_file_dir, "stderr"), "w") as f:
            if self._stderr is not None:
                f.write(self._stderr.decode())
