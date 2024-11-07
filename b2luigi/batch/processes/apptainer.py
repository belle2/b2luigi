import subprocess
import enum


from b2luigi.core.settings import get_setting
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import create_cmd_from_task


class ApptainerProcess(BatchProcess):
    """
    Simple implementation of a batch process for running jobs in an Apptainer container.

    This process focuses solely on executing the job within an Apptainer container without the overhead of
    job scheduling systems like HTCondor.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._job_id = None

    def get_job_status(self):
        # In a simple execution model, we can assume the job is either running or completed.
        if self._job_id is None:
            return JobStatus.aborted

        try:
            # Check if the job is still running
            process = subprocess.run(["ps", "-p", str(self._job_id)], capture_output=True)
            if process.returncode == 0:
                return JobStatus.running  # Process is still running
            else:
                return JobStatus.successful  # Process has finished
        except Exception:
            return JobStatus.aborted

    def start_job(self):
        env_setup_script = get_setting("env_script", task=self.task, default="")
        command = " ".join(create_cmd_from_task(self.task))

        # 4. Forth part is to create the correct execution command
        # (a) If a valid apptainer image is provided, build an apptainer command

        apptainer_image = get_setting("apptainer_image", task=self.task, default="")

        # If the batch system is gbasf2, we cannot use apptainer
        if get_setting("batch_system", default="lsf", task=self.task) == "gbasf2":
            raise ValueError("Invalid batch system for apptainer usage. Apptainer is not supported for gbasf2.")

        exec_command = ["apptainer", "exec"]
        # Add apptainer mount points if given
        mounts = get_setting("apptainer_mounts", task=self.task, default=[])

        if get_setting("apptainer_mount_defaults", task=self.task, default=True):
            mounts += [get_setting("res_dir", task=self.task),get_setting("log_dir", task=self.task)]
        for mount in mounts:
            exec_command += ["--bind", mount]
        # Other mounts
        # additional_params = get_setting("apptainer_additional_params", default="", task=self.task)
        # exec_command += [f" {additional_params}" if additional_params else ""]
        exec_command += [apptainer_image]
        exec_command += ["/bin/bash", "-c"]
        exec_command += [f"'source {env_setup_script} && {command}'"]
        print(exec_command)
        # Start the job and capture the job ID
        self._job_id = subprocess.Popen(exec_command).pid

    def terminate_job(self):
        if self._job_id is not None:
            subprocess.run(["kill", str(self._job_id)], stdout=subprocess.DEVNULL)

    def _create_apptainer_command(self):
        # Create command setup here if needed (optional)

        pass


# JobStatus Enum is kept simple; adapt as needed
class ApptainerJobStatus(enum.IntEnum):
    running = 1
    completed = 2
    aborted = 999
