import subprocess
import pathlib
import re
import getpass
import enum
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from luigi.parameter import _no_value
from b2luigi.core.settings import get_setting
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.batch.cache import BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir, get_task_file_dir
from b2luigi.core.executable import create_executable_wrapper


class SlurmJobStatusCache(BatchJobStatusCache):
    sacct_disabled = None

    @retry(
        retry=retry_if_exception_type(subprocess.CalledProcessError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, exp_base=3),  # 2, 6, 18 seconds
        reraise=True,
    )
    def _ask_for_job_status(self, job_id: int = None):
        """
        With Slurm, you can check the progress of your jobs using the ``squeue`` command.
        If no ``jobID`` is given as argument, this command shows you the status of all queued jobs.

        Sometimes it might happen that a job is completed in between the status checks. Then its final status
        can be found using ``sacct`` (works mostly in the same way as ``squeue``).

        If in the unlikely case the server has the Slurm accounting disabled, then the `scontrol` command is used as a last
        resort to access the jobs history. This is the fail safe command as the scontrol by design only holds onto a jobs
        information for a short period of time after completion. The time between status checks is sufficiently short however
        so the scontrol command should still have the jobs information on hand.

        All three commands are used in order to find out the :obj:`SlurmJobStatus`.
        """
        # https://slurm.schedmd.com/squeue.html
        user = getpass.getuser()
        q_cmd = ["squeue", "--noheader", "--user", user, "--format", "'%i %T'"] + (
            ["--job", str(job_id)] if job_id else []
        )
        output = subprocess.check_output(q_cmd)

        output = output.decode()
        seen_ids = self._fill_from_output(output)
        # If no job_id was passed, then exit
        if not job_id:
            return
        # If the specified job can not be found in the squeue output, we need to request its history from the slurm job accounting log
        # We also check that the working Slurm server has the Slurm accounting storage active
        if job_id not in seen_ids and not self._check_if_sacct_is_disabled_on_server():
            # https://slurm.schedmd.com/sacct.html
            history_cmd = [
                "sacct",
                "--noheader",
                "-X",
                "--user",
                user,
                "--format=JobID,State",
                "--job",
                str(job_id),
            ]
            output = subprocess.check_output(history_cmd)
            output = output.decode()
            self._fill_from_output(output)

        # If the Slurm accounting storage is disabled, we resort to the scontrol command
        elif job_id not in seen_ids and self._check_if_sacct_is_disabled_on_server():
            output = subprocess.check_output(["scontrol", "show", "job", str(job_id)])
            output = output.decode()

            # Extract the job state from the output
            re_output = re.search(r"JobState=([A-Z_]+)", output)
            if re_output:
                state_string = re_output.group(1)
                self[job_id] = self._get_SlurmJobStatus_from_string(state_string)

            # the specified job cannot be found on the slurm system. Return a failed.
        else:
            self[job_id] = SlurmJobStatus.failed

    def _fill_from_output(self, output: str) -> set:
        """
        Parses the output of a Slurm command to extract job IDs and their states,
        updating the internal job status mapping and returning a set of seen job IDs.

        Args:
            output (str): The output string from a Slurm command, expected to be
                          formatted as '<job id> <state>' per line.

        Returns:
            set: A set of job IDs that were parsed from the output.

        Raises:
            AssertionError: If a line in the output does not contain exactly two
                            entries (job ID and state).

        Notes:
            - If the output is empty, an empty set is returned.
            - Lines in the output that are empty or contain unexpected formatting
              are skipped.
            - Job states with a '+' suffix (e.g., 'CANCELLED+') are normalized by
              stripping the '+' character.
        """
        seen_ids = set()

        # If the output is empty return an empty set
        if not output:
            return seen_ids

        # If no jobs exist then output=='' and this loop does not see any id's
        for job_info_str in output.split("\n"):
            if not job_info_str:
                continue  # When splitting by \n, the final entry of the list is likely an empty string
            job_info = job_info_str.strip("'").split()

            # We have formatted the squeue and sacct outputs to be '<job id> <state>'
            # hence we expect there to always be two entries in the list
            assert (
                len(job_info) == 2
            ), "Unexpected behaviour has occurred whilst retrieving job information. There may be an issue with the sqeue, sacct or scontrol commands."
            id, state_string = job_info
            id = int(id)
            # Manually cancelling jobs gives the state 'CANCELLED+'
            state_string = state_string.strip("+")
            self[id] = self._get_SlurmJobStatus_from_string(state_string)
            seen_ids.add(id)

        return seen_ids

    def _get_SlurmJobStatus_from_string(self, state_string: str) -> str:
        """
        Converts a state string into a :obj:`SlurmJobStatus` enumeration value.

        Args:
            state_string (str): The state string to be converted.

        Returns:
            str: The corresponding :obj:`SlurmJobStatus` value.

        Raises:
            KeyError: If the provided state string does not match any valid :obj:`SlurmJobStatus`.
        """
        try:
            state = SlurmJobStatus(state_string)
        except KeyError:
            raise KeyError(f"The state {state_string} could not be found in the SlurmJobStatus states")
        return state

    def _check_if_sacct_is_disabled_on_server(self) -> bool:
        """
        Checks if the Slurm accounting command ``sacct`` is disabled on the system.

        This method determines whether the ``sacct`` command is unavailable or
        disabled by attempting to execute it and analyzing the output. The result
        is cached in the ``self.sacct_disabled`` attribute to avoid repeated checks.

        Returns:
            bool: True if ``sacct`` is disabled on the system, ``False`` otherwise.
        """
        if self.sacct_disabled is None:
            # Don't continually call the function, instead call it once and set self.sacct_disabled
            output = subprocess.run(["sacct"], capture_output=True)

            # If the call to 'sacct' returns an error code 1 and checking the stderr returns 'Slurm accounting storage is disabled'
            self.sacct_disabled = (
                output.returncode == 1 and output.stderr.strip().decode() == "Slurm accounting storage is disabled"
            )
        return self.sacct_disabled


class SlurmJobStatus(enum.Enum):
    """
    See https://slurm.schedmd.com/job_state_codes.html
    TODO: make this a StrEnum with python>=3.11

    Attributes:
        completed (str): The job has completed successfully.
        pending (str): The job is waiting to be scheduled.
        running (str): The job is currently running.
        suspended (str): The job has been suspended.
        preempted (str): The job has been preempted by another job.
        completing (str): The job is in the process of completing.

        boot_fail (str): The job failed during the boot process.
        cancelled (str): The job was cancelled by the user or system.
        deadline (str): The job missed its deadline.
        node_fail (str): The job failed due to a node failure.
        out_of_memory (str): The job ran out of memory.
        failed (str): The job failed for an unspecified reason.
        timeout (str): The job exceeded its time limit.
    """

    # successful:
    completed = "COMPLETED"

    # running:
    pending = "PENDING"
    running = "RUNNING"
    suspended = "SUSPENDED"
    preempted = "PREEMPTED"
    completing = "COMPLETING"

    # failed:
    boot_fail = "BOOT_FAIL"
    cancelled = "CANCELLED"
    deadline = "DEADLINE"
    node_fail = "NODE_FAIL"
    out_of_memory = "OUT_OF_MEMORY"
    failed = "FAILED"
    timeout = "TIMEOUT"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        elif isinstance(other, SlurmJobStatus):
            return self.value == other.value
        raise TypeError(
            "The equivalence of a SlurmJobStatus can only be checked with a string or another SlurmJobStatus object."
        )


_batch_job_status_cache = SlurmJobStatusCache()


class SlurmProcess(BatchProcess):
    """
    Reference implementation of the batch process for a Slurm batch system.

    Additional to the basic batch setup (see :ref:`batch-label`), additional
    Slurm-specific things are:

    * Please note that most of the Slurm batch farms by default copy the user environment from
      the submission node to the worker machine. As this can lead to different results when running
      the same tasks depending on your active environment, you probably want to pass the argument
      ``export=NONE``. This ensures that a reproducible environment is used. You can provide an
      ``env_script``, an ``env`` :meth:`setting <b2luigi.set_setting>`, and/or a different
      ``executable`` to create the environment necessary for your task.

    * Via the ``slurm_settings`` setting you can provide a dict for additional options, such as
      requested memory etc. Its value has to be a dictionary
      containing Slurm settings as key/value pairs. These options will be written into the job
      submission file. For an overview of possible settings refer to the `Slurm documentation
      <https://slurm.schedmd.com/sbatch.html#>_` and the documentation of the cluster you are using.

    * Same as for the :ref:`lsf` and :ref:`htcondor`, the ``job_name`` setting allows giving a meaningful
      name to a group of jobs. If you want to be task-instance-specific, you can provide the ``job-name``
      as an entry in the ``slurm_settings`` dict, which will override the global ``job_name`` setting.
      This is useful for manually checking the status of specific jobs with

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
        """
        Determine the status of a batch job based on its Slurm job status.

        Returns:
            JobStatus: The status of the job, which can be one of the following:
                - :meth:`JobStatus.successful <b2luigi.process.JobStatus.successful>`: If the job has completed successfully.
                - :meth:`JobStatus.running <b2luigi.process.JobStatus.running>`: If the job is currently running, pending, suspended, preempted, or completing.
                - :meth:`JobStatus.aborted <b2luigi.process.JobStatus.aborted>`: If the job has failed, been cancelled, exceeded its deadline, encountered a node failure,
                  ran out of memory, timed out, or if the job ID is not found.

        Raises:
            ValueError: If the Slurm job status is unknown or not handled.
        """
        if not self._batch_job_id:
            return JobStatus.aborted
        try:
            job_status = _batch_job_status_cache[self._batch_job_id]
        except KeyError:
            return JobStatus.aborted

        # See https://slurm.schedmd.com/job_state_codes.html
        if job_status in [SlurmJobStatus.completed]:
            return JobStatus.successful
        if job_status in [
            SlurmJobStatus.pending,
            SlurmJobStatus.running,
            SlurmJobStatus.suspended,
            SlurmJobStatus.preempted,
            SlurmJobStatus.completing,
        ]:
            return JobStatus.running
        if job_status in [
            SlurmJobStatus.boot_fail,
            SlurmJobStatus.cancelled,
            SlurmJobStatus.deadline,
            SlurmJobStatus.node_fail,
            SlurmJobStatus.out_of_memory,
            SlurmJobStatus.failed,
            SlurmJobStatus.timeout,
        ]:
            return JobStatus.aborted
        raise ValueError(f"Unknown Slurm Job status: {job_status}")

    def start_job(self):
        """
        Starts a job by submitting the Slurm submission script.

        This method creates a Slurm submit file and submits it using the ``sbatch`` command.
        After submission, it parses the output to extract the batch job ID.

        Raises:
            RuntimeError: If the batch submission fails or the job ID cannot be extracted.

        Attributes:
            self._batch_job_id (int): The ID of the submitted Slurm batch job.
        """
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
        """
        Terminates a batch job if a job ID is available.

        This method checks if a batch job ID is set. If it is, it executes the
        ``scancel`` command to terminate the job associated with the given batch
        job ID. The command's output is suppressed.
        """
        if not self._batch_job_id:
            return
        subprocess.run(["scancel", str(self._batch_job_id)], stdout=subprocess.DEVNULL)

    def _create_slurm_submit_file(self):
        """
        Creates a Slurm submit file for the current task.

        This method generates a Slurm batch script that specifies the necessary
        configurations for submitting a job to a Slurm workload manager.

        Returns:
            pathlib.Path: The path to the generated Slurm submit file.

        Note:
            - The ``stdout`` and ``stderr`` log files are created in the task's log directory. See :obj:`get_log_file_dir`.
            - The Slurm settings are specified in the ``slurm_settings`` setting, which is a dictionary of key-value pairs.
            - The ``job_name`` setting can be used to specify a meaningful name for the job.
            - The executable is created with :obj:`create_executable_wrapper`.
            - The submit file is named `slurm_parameters.sh` and is created in the task's output directory (:obj:`get_task_file_dir`).
        """
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
