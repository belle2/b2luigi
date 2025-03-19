from retry import retry
import subprocess
import pathlib
import re
import getpass
import json
import enum

from luigi.parameter import _no_value
from b2luigi.core.settings import get_setting
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.batch.cache import BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir, get_task_file_dir
from b2luigi.core.executable import create_executable_wrapper


class SlurmJobStatusCache(BatchJobStatusCache):
    @retry(subprocess.CalledProcessError, tries=3, delay=2, backoff=3)  # retry after 2,6,18 seconds
    def _ask_for_job_status(self, job_id: int = None):
        """
        With Slurm, you can check the progress of your jobs using the `squeue` command.
        If no `jobID` is given as argument, this command shows you the status of all queued jobs.

        With the `--json` option, a detailed `squeue` output is returned in the JSON format. Using the default data_parser it is not possible to ask for only the pertinent information.

        NOTE: If this is a large performance issue it should be possible to implement a custom data_parser to only get the information we need.

        Sometimes it might happen that a job is completed in between the status checks. Then its final status
        can be found using `sacct` (works mostly in the same way as `squeue` but requires specifying a starting date and time if we don't specify a particular jobID).
        Both commands are used in order to find out the `JobStatus`.
        """
        # https://slurm.schedmd.com/squeue.html
        user = getpass.getuser()
        q_cmd = [
            "squeue",
            "--user",
            user,
            "--json",
        ]
        if job_id:
            output = subprocess.check_output(q_cmd + ["--job", str(job_id)])
        else:
            output = subprocess.check_output(q_cmd)

        seen_ids = self._fill_from_output(output)

        if not job_id:
            return

        # If the specified job can not be found in the squeue output, we need to request its history from the slurm job accounting log
        if job_id not in seen_ids:
            # https://slurm.schedmd.com/sacct.html
            history_cmd = [
                "sacct",
                "--user",
                user,
                "--json",
                "--job",
                str(job_id),
            ]
            output = subprocess.check_output(history_cmd)

            self._fill_from_output(output)
        else:
            # the specified job cannot be found on the slurm system. Return a failed.
            self[job_id] = SlurmJobStatus.failed.value

    def _fill_from_output(self, output):
        output = output.decode()

        seen_ids = set()

        if not output:
            return seen_ids

        for status_dict in json.loads(output)["jobs"]:
            jobID = status_dict["job_id"]  # int

            # the format of the output is different for squeue and sacct
            if "state" in status_dict.keys():
                # sacct
                state = status_dict["state"]["current"]
            elif "job_state" in status_dict.keys():
                # squeue
                state = status_dict["job_state"]
            else:
                raise KeyError(f"Could not find the state of the job in the output: {status_dict}")

            # the state can contain a base state and additional flags. (https://slurm.schedmd.com/job_state_codes.html)
            # We only want the base state.
            if len(state) > 1:
                state = [x for x in state if x in [e.value for e in SlurmJobStatus]]
            assert len(state) == 1, f"state ({state}) has more than one entry."
            state = state[0]

            self[jobID] = state
            seen_ids.add(jobID)

        return seen_ids


class SlurmJobStatus(enum.Enum):
    """
    See https://slurm.schedmd.com/job_state_codes.html
    TODO: make this a StrEnum with python>=3.11

    The following are the possible states of a job in Slurm:
    """

    # successful:
    completed = "COMPLETED"

    # running:
    pending = "PENDING"
    running = "RUNNING"
    suspended = "SUSPENDED"
    preempted = "PREEMPTED"

    # failed:
    boot_fail = "BOOT_FAIL"
    canceled = "CANCELED"
    deadline = "DEADLINE"
    node_fail = "NODE_FAIL"
    out_of_memory = "OUT_OF_MEMORY"
    failed = "FAILED"
    timeout = "TIMEOUT"


_batch_job_status_cache = SlurmJobStatusCache()


class SlurmProcess(BatchProcess):
    """
    Reference implementation of the batch process for a Slurm batch system.

    Additional to the basic batch setup (see :ref:`batch-label`), additional
    Slurm-specific things are:

    * Please note that most of the Slurm batch farms by default copy the user environment from the submission node to the worker machine. As this can lead to different results when running the same tasks depending on your active environment, you probably want to pass the argument `export=NONE`. This ensures that a reproducible environment is used. You can provide an ``env_script``, an ``env`` :meth:`setting <b2luigi.set_setting>`, and/or a different ``executable`` to create the environment necessary for your task.

    * Via the ``slurm_settings`` setting you can provide a dict for additional options, such as requested memory etc. Its value has to be a dictionary
      containing Slurm settings as key/value pairs. These options will be written into the job
      submission file. For an overview of possible settings refer to the `Slurm documentation
      <https://slurm.schedmd.com/sbatch.html#>_` and the documentation of the cluster you are using.

    * Same as for the :ref:`lsf` and :ref:`htcondor`, the ``job_name`` setting allows giving a meaningful name to a group of jobs. If you want to be task-instance-specific, you can provide the ``job-name`` as an entry in the ``slurm_settings`` dict, which will override the global ``job_name`` setting. This is useful for manually checking the status of specific jobs with

      .. code-block:: bash

        squeue --name <job name>

    Example:

        .. literalinclude:: ../../examples/slurm/slurm_example.py
           :linenos:
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._batch_job_id = None

    def get_job_status(self):
        if not self._batch_job_id:
            return JobStatus.aborted
        try:
            job_status = _batch_job_status_cache[self._batch_job_id]
        except KeyError:
            return JobStatus.aborted

        # See https://slurm.schedmd.com/job_state_codes.html
        if job_status in [SlurmJobStatus.completed.value]:
            return JobStatus.successful
        if job_status in [
            SlurmJobStatus.pending.value,
            SlurmJobStatus.running.value,
            SlurmJobStatus.suspended.value,
            SlurmJobStatus.preempted.value,
        ]:
            return JobStatus.running
        if job_status in [
            SlurmJobStatus.boot_fail.value,
            SlurmJobStatus.canceled.value,
            SlurmJobStatus.deadline.value,
            SlurmJobStatus.node_fail.value,
            SlurmJobStatus.out_of_memory.value,
            SlurmJobStatus.failed.value,
            SlurmJobStatus.timeout.value,
        ]:
            return JobStatus.aborted
        raise ValueError(f"Unknown Slurm Job status: {job_status}")

    def start_job(self):
        submit_file = self._create_slurm_submit_file()

        # Slurm submit needs to be called in the folder of the submit file
        path = pathlib.Path(submit_file)
        output = subprocess.check_output(["sbatch", path.name], cwd=path.parent)

        output = output.decode()
        match = re.search(r"[0-9]+", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)
        self._batch_job_id = int(match.group(0))

    def terminate_job(self):
        if not self._batch_job_id:
            return
        subprocess.run(["scancel", str(self._batch_job_id)], stdout=subprocess.DEVNULL)

    def _create_slurm_submit_file(self):
        submit_file_content = ["#!/usr/bin/bash"]

        # Specify where to write the log to
        log_file_dir = pathlib.Path(get_log_file_dir(self.task))
        log_file_dir.mkdir(parents=True, exist_ok=True)

        stdout_log_file = (log_file_dir / "stdout").resolve()
        submit_file_content.append(f"#SBATCH --output={stdout_log_file}")

        stderr_log_file = (log_file_dir / "stderr").resolve()
        submit_file_content.append(f"#SBATCH --error={stderr_log_file}")

        # Specify additional settings
        # A default value of None requires that the user must set the setting. We therefore use luigi.parameters._no_value.
        general_settings = get_setting("slurm_settings", default=_no_value)
        if general_settings == _no_value:
            general_settings = {}

        task_slurm_settings = get_setting("slurm_settings", task=self.task, default=_no_value)
        if task_slurm_settings != _no_value:
            general_settings.update(task_slurm_settings)

        job_name = get_setting("job_name", task=self.task, default=False)
        if job_name is not False:
            general_settings.setdefault("job-name", job_name)

        for key, item in general_settings.items():
            submit_file_content.append(f"#SBATCH --{key}={item}")

        # Specify the executable
        executable_file = create_executable_wrapper(self.task)
        submit_file_content.append(f"exec {pathlib.Path(executable_file).resolve()}")

        # Now we can write the submit file
        output_path = pathlib.Path(get_task_file_dir(self.task))
        submit_file_path = output_path / "slurm_parameters.sh"

        output_path.mkdir(parents=True, exist_ok=True)
        with open(submit_file_path, "w") as submit_file:
            submit_file.write("\n".join(submit_file_content))
        return submit_file_path
