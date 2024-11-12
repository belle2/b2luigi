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
        self._job_id = None
        self._sdtout = None
        self._stderr = None

    def get_job_status(self):
        if self._job_id is None:
            return JobStatus.aborted

        try:
            # Check if the job is still running
            process = subprocess.run(["ps", "-p", str(self._job_id)], capture_output=True)
            if process.returncode == 0:
                return JobStatus.running
            else:
                self._write_output()
                return JobStatus.successful
        except Exception:
            self._write_output()
            return JobStatus.aborted

    def start_job(self):
        command = " ".join(create_cmd_from_task(self.task))
        exec_command = create_apptainer_command(command, task=self.task)

        # Start the job and capture the job ID
        process = subprocess.Popen(exec_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self._stdout, self._stderr = process.communicate()
        self._job_id = process.pid

    def terminate_job(self):
        if self._job_id is not None:
            subprocess.run(["kill", str(self._job_id)], stdout=subprocess.DEVNULL)

    def _write_output(self):
        log_file_dir = get_log_file_dir(self.task)

        with open(os.path.join(log_file_dir, "stdout"), "w") as f:
            if self._stdout is not None:
                f.write(self._stdout.decode())

        with open(os.path.join(log_file_dir, "stderr"), "w") as f:
            if self._stderr is not None:
                f.write(self._stderr.decode())
