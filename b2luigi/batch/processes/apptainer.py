import os
import subprocess

import shlex
from b2luigi.core.settings import get_setting
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import create_cmd_from_task, map_folder, get_log_file_dir


def is_subdir(path, parent_dir):
    path = os.path.abspath(path)
    parent_dir = os.path.abspath(parent_dir)
    return os.path.commonpath([path, parent_dir]) == parent_dir


class ApptainerProcess(BatchProcess):
    """
    Simple implementation of a batch process for running jobs in an Apptainer container.

    This process focuses solely on executing the job within an Apptainer container without the overhead of
    job scheduling systems like HTCondor.
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
        env_setup_script = get_setting("env_script", task=self.task, default="")
        command = " ".join(create_cmd_from_task(self.task))

        apptainer_image = get_setting("apptainer_image", task=self.task, default="")

        # If the batch system is gbasf2, we cannot use apptainer
        if get_setting("batch_system", default="lsf", task=self.task) == "gbasf2":
            raise ValueError("Invalid batch system for apptainer usage. Apptainer is not supported for gbasf2.")

        exec_command = ["apptainer", "exec"]
        additional_params = get_setting("apptainer_additional_params", default="", task=self.task)
        exec_command += [f" {additional_params}"] if additional_params else []

        # Add apptainer mount points if given
        mounts = get_setting("apptainer_mounts", task=self.task, default=[])

        if get_setting("apptainer_mount_defaults", task=self.task, default=True):
            local_mounts = [
                map_folder(get_setting("result_dir", task=self.task, default=".", deprecated_keys=["result_path"])),
                get_log_file_dir(task=self.task),
            ]
            for mount in local_mounts:
                os.makedirs(mount, exist_ok=True)
                if not is_subdir(mount, os.getcwd()):
                    mounts.append(mount)

        for mount in mounts:
            exec_command += ["--bind", mount]

        exec_command += [apptainer_image]
        exec_command += ["/bin/bash", "-c"]
        exec_command += [f"'source {env_setup_script} && {command}'"]

        # Start the job and capture the job ID
        # Do the shlex split for correct string interpretation
        process = subprocess.Popen(shlex.split(" ".join(exec_command)), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

    def _create_apptainer_command(self):
        # Create command setup here if needed (optional)

        pass
