import subprocess
import pathlib
import re
import getpass
import os
from enum import StrEnum
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

import luigi
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
    def _ask_for_job_status(self, job_id: str = None):
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

        Args:
            job_id: The job ID to check. Can be a regular job ID (e.g., "123456") or a job array ID (e.g., "123456_1").
                    If None, checks all jobs for the current user.
        """
        # https://slurm.schedmd.com/squeue.html
        user = getpass.getuser()
        q_cmd = ["squeue", "--noheader", "--user", user, "--format", "'%i %T'"] + (
            ["--job", str(job_id)] if job_id else []
        )
        try:
            output = subprocess.check_output(q_cmd, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            # When a specific job_id has already been purged from Slurm's live job table,
            # squeue exits non-zero with "Invalid job id specified" instead of just returning
            # empty output. This is an expected outcome (not a transient failure), so we treat
            # it as "no jobs seen" and let the sacct/scontrol fallback below resolve the status,
            # rather than retrying (which would just fail identically) and crashing.
            if job_id and e.stderr and b"Invalid job id specified" in e.stderr:
                output = b""
            else:
                raise

        output = output.decode()
        seen_ids = self._fill_from_output(output)
        # If no job_id was passed, then exit
        if not job_id:
            return

        # For job arrays, the job ID is in the format "123456_123".
        # We need to check if the job_id is the base of some ids in the seen_ids set.
        # For example, if job_id is "123456", we need to check if any id in seen_ids starts with "123456_".
        job_id_in_seen_ids = any(id.startswith(f"{job_id}_") or id == job_id for id in seen_ids)

        # If the specified job can not be found in the squeue output, we need to request its history from the slurm job accounting log
        # We also check that the working Slurm server has the Slurm accounting storage active
        if not job_id_in_seen_ids and not self._check_if_sacct_is_disabled_on_server():
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
        elif not job_id_in_seen_ids and self._check_if_sacct_is_disabled_on_server():
            output = subprocess.check_output(["scontrol", "show", "job", str(job_id)])
            output = output.decode()

            # FIXME: If the base id of a job array is passed, the scontrol command will return the state of only the first job in the array.
            # Extract the job state from the output
            re_output = re.search(r"JobState=([A-Z_]+)", output)
            if re_output:
                state_string = re_output.group(1)
                self[job_id] = self._get_SlurmJobStatus_from_string(state_string)


            # Parse the output to find the specific job_id we're looking for
            # scontrol can show multiple entries for job arrays, each starting with "JobId=..."
            job_entries = re.split(r"(?=JobId=)", output)

            found = False
            for entry in job_entries:
                # Extract JobId from this entry
                job_id_match = re.search(r"JobId=(\d+)", entry)
                if job_id_match:
                    entry_job_id = job_id_match.group(1)

                    # For array jobs, we like to construct the full job ID as "ArrayJobId_ArrayTaskId"
                    # Note the the scontrol output for array jobs contains both "ArrayJobId" and "ArrayTaskId" fields,
                    # and the "JobId" field, which corresponds to ArrayJobId+ArrayTaskId.
                    array_job_id_match  = re.search(r"ArrayJobId=(\d+)",  entry)
                    array_task_id_match = re.search(r"ArrayTaskId=(\d+)", entry)

                    if array_job_id_match and array_task_id_match:
                        # This is an array task job
                        constructed_job_id = f"{array_job_id_match.group(1)}_{array_task_id_match.group(1)}"
                    else:
                        # This is a regular job
                        constructed_job_id = entry_job_id

                    if constructed_job_id == str(job_id):
                        # Found the matching job entry
                        state_match = re.search(r"JobState=([A-Z_]+)", entry)
                        if state_match:
                            state_string = state_match.group(1)
                            self[job_id] = self._get_SlurmJobStatus_from_string(state_string)
                            found = True
                            break

            if not found:
                # the specified job cannot be found on the slurm system. Return a failed.
                pass

        else:
            self[job_id] = SlurmJobStatus.failed

    def _fill_from_output(self, output: str) -> set:
        """
        Parses the output of a Slurm command to extract job IDs and their states,
        updating the internal job status mapping and returning a set of seen job IDs.

        Args:
            output (str): The output string from a Slurm command, expected to be
                          formatted as '<job id> <state>' per line.
                          For job arrays, the job ID may be in the format "123456_[3-10] <state>".

        Returns:
            set: A set of job IDs (str) that were parsed from the output.

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
            # Manually cancelling jobs gives the state 'CANCELLED+'
            state_string = state_string.strip("+")
            # For job arrays, the job ID may be in the format "123456_[3-10] PENDING".
            # We need to handle this case separately and assign the same state to all jobs in the array.
            if "[" in id and "]" in id:
                match = re.match(r"(\d+)_\[(\d+)-(\d+)\]", id)
                if match:
                    base_id, start_index, end_index = match.groups()
                    for index in range(int(start_index), int(end_index) + 1):
                        array_job_id = f"{base_id}_{index}"
                        self[array_job_id] = self._get_SlurmJobStatus_from_string(state_string)
                        seen_ids.add(array_job_id)
            else:
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


class SlurmJobStatus(StrEnum):
    """
    See https://slurm.schedmd.com/job_state_codes.html

    Attributes:
        completed (str): The job has completed successfully.
        pending (str): The job is waiting to be scheduled.
        running (str): The job is currently running.
        configuring (str): The job is allocated resources but waiting for nodes to be prepared.
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
    configuring = "CONFIGURING"

    # failed:
    boot_fail = "BOOT_FAIL"
    cancelled = "CANCELLED"
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

        self._batch_job_ids = []

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
        if not self._batch_job_ids:
            return JobStatus.aborted

        # Get status for all jobs in the list
        job_stati = {}
        for job_id in self._batch_job_ids:
            try:
                job_status = _batch_job_status_cache[job_id]
                job_stati[job_id] = job_status
            except KeyError:
                # If any job is not found in cache, consider it aborted
                return JobStatus.aborted


        def get_job_status_from_slurm_status(slurm_status):
            """
            Convert a Slurm job status to a b2luigi job status (completed, running, aborted).
            See https://slurm.schedmd.com/job_state_codes.html

            Args:
                slurm_status (SlurmJobStatus): The Slurm job status.

            Returns:
                str: The corresponding b2luigi job status.
            """
            if slurm_status in [SlurmJobStatus.completed]:
                return "completed"
            if slurm_status in [
                SlurmJobStatus.pending,
                SlurmJobStatus.running,
                SlurmJobStatus.suspended,
                SlurmJobStatus.preempted,
                SlurmJobStatus.completing,
                SlurmJobStatus.configuring,
            ]:
                return "running"
            if slurm_status in [
                SlurmJobStatus.boot_fail,
                SlurmJobStatus.cancelled,
                SlurmJobStatus.deadline,
                SlurmJobStatus.node_fail,
                SlurmJobStatus.out_of_memory,
                SlurmJobStatus.failed,
                SlurmJobStatus.timeout,
            ]:
                return "aborted"
            raise ValueError(f"Unknown Slurm Job status: {slurm_status}")

        # Convert Slurm job stati to b2luigi job stati
        job_stati = {job_id: get_job_status_from_slurm_status(status) for job_id, status in job_stati.items()}

        # All jobs completed successfully
        if all(status == "completed" for status in job_stati.values()):
            _batch_job_status_cache.remove_job_ids(job_ids=self._batch_job_ids)
            return JobStatus.successful

        # Check if any job is running
        if any(status == "running" for status in job_stati.values()):
            return JobStatus.running

        # Check if any job has failed
        if any(status == "aborted" for status in job_stati.values()):
            failed_job_ids = [job_id for job_id, status in job_stati.items() if status == "aborted"]

            log_file_dir = get_log_file_dir(task=self.task)
            os.makedirs(log_file_dir, exist_ok=True)
            with open(os.path.join(log_file_dir, "failed_jobs.log"), "w") as f:
                for job_id in failed_job_ids:
                    # For job arrays, we might need to handle the array index format
                    f.write(f"{job_id}\n")

            _batch_job_status_cache.remove_job_ids(job_ids=self._batch_job_ids)
            return JobStatus.aborted

    def start_job(self):
        """
        Starts a job by submitting the Slurm submission script.

        This method creates a Slurm submit file and submits it using the :obj:`_create_slurm_submit_file`
        method, then submits the job using the ``sbatch`` command.
        After submission, it parses the output to extract the batch job ID(s).

        For job arrays, multiple job IDs may be extracted and stored in _batch_job_ids.

        Raises:
            RuntimeError: If the batch submission fails or the job ID cannot be extracted.

        Attributes:
            self._batch_job_ids (list): The IDs of the submitted Slurm batch jobs.
        """
        submit_file = self._create_slurm_submit_file()

        # Slurm submit needs to be called in the folder of the submit file
        path = pathlib.Path(submit_file)
        output = subprocess.check_output(["sbatch", path.name], cwd=path.parent)

        output = output.decode()
        match = re.search(r"[0-9]+", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)
        job_id = match.group(0)
        # For job arrays, the job ID is in the format "123456_1", "123456_2", etc..
        # However, when submitting the job array, Slurm returns only the base job ID (e.g., "123456") and the array indices
        # are specified in the submit file.
        # FIXME: Is the following working?
        self._batch_job_ids = [job_id]
        _batch_job_status_cache.add_job_ids(self._batch_job_ids)

    def terminate_job(self):
        """
        Terminates a batch job if a job ID is available.

        This method checks if batch job IDs are set. If they are, it executes the
        ``scancel`` command to terminate the jobs associated with the given batch
        job IDs. The command's output is suppressed.
        """
        if not self._batch_job_ids:
            return

        # Extract cluster IDs from job IDs to handle both regular jobs and job arrays
        cluster_ids = set()
        for job_id in self._batch_job_ids:
            # For job arrays like "123456" or "123456_[1-10]", extract "123456"
            cluster_id = str(job_id).split("_")[0]
            cluster_ids.add(cluster_id)

        # Cancel all unique cluster IDs
        if cluster_ids:
            cancel_cmd = ["scancel"]
            cancel_cmd.extend(list(cluster_ids))
            subprocess.run(cancel_cmd, stdout=subprocess.DEVNULL)

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

        # Ask for a property submission_type. This is a custom property that can be used to specify
        # the type of submission ("single", "array", "mpi").
        # If not provided, it defaults to "single".
        # TODO: Add "mpi". Need to have additional attribute "tasks_per_node", which specifies how many tasks can be run on each node.
        submission_type = get_setting("submission_type", task=self.task, default="single")
        # if submission_type != "single":
        #     general_settings.setdefault("submission_type", submission_type)

        for key, item in general_settings.items():
            submit_file_content.append(f"#SBATCH --{key}={item}")

        # Check for grouped parameters
        grouped_params = self.task.grouped_param_names()

        if len(grouped_params) == 0:
            # No grouping - single job
            executable_file = create_executable_wrapper(self.task)
            submit_file_content.append(f"exec {pathlib.Path(executable_file).resolve()}")
        elif not isinstance(self.task.param_kwargs[grouped_params[0]], tuple):
            # Grouped parameter but not a tuple (single value) - single job
            executable_file = create_executable_wrapper(self.task)
            submit_file_content.append(f"exec {pathlib.Path(executable_file).resolve()}")
        elif submission_type not in ["array", "mpi"]:
            executable_file = create_executable_wrapper(self.task)
            submit_file_content.append(f"exec {pathlib.Path(executable_file).resolve()}")
        else:
            # Grouped parameters with tuple values - multiple jobs
            len_combinations = len(self.task.param_kwargs[grouped_params[0]])

            grouped_param_dicts = [
                {param: value[i] for param, value in self.task.param_kwargs.items() if param in grouped_params}
                for i in range(len_combinations)
            ]

            # Create executable wrappers for each grouped task
            executable_wrappers = []
            n_submitted_tasks = 0
            for idx, param_dict in enumerate(grouped_param_dicts):
                sub_task = self.task.clone(None, **param_dict)

                # If a sub_task was already successful do not resubmit it
                if sub_task.complete():
                    continue

                n_submitted_tasks += 1
                # Specify the executable
                executable_file = create_executable_wrapper(task=sub_task)
                # Add the path to the list of executable wrappers
                executable_wrappers.append(executable_file)

            # If no tasks need to be submitted, mark as terminated
            if len(executable_wrappers) == 0:
                self._put_to_result_queue(status=luigi.scheduler.DONE, explanation="")
                self._terminated = True
            else:
                if submission_type == "array":
                    submit_file_content.append(f"#SBATCH --array=0-{n_submitted_tasks - 1}")

                    # The SLURM_ARRAY_TASK_ID environment variable is used to determine which task in the array is being executed.
                    procid="SLURM_ARRAY_TASK_ID"
                elif submission_type == "mpi":
                    raise NotImplementedError("MPI submission type is not yet implemented.")
                    # TODO: Add MPI support.
                    # # For MPI submission, we need to calculate the number of nodes and tasks per node based on the number of combinations.
                    # # We will use the setting "tasks_per_node" to determine how many tasks can be run on each node.
                    # tasks_per_node = get_setting("tasks_per_node", task=self.task, default=64)
                    # nnodes = n_submitted_tasks // tasks_per_node
                    # if n_submitted_tasks % tasks_per_node != 0:
                    #     nnodes += 1
                    # submit_file_content.append(f"#SBATCH --nodes={nnodes}")
                    # submit_file_content.append(f"#SBATCH --ntasks={n_submitted_tasks}")

                    # The SLURM_PROCID environment variable is used to determine which task in the MPI job is being executed.
                    # procid="SLURM_PROCID"
                else:
                    raise ValueError(f"Unknown submission type: {submission_type}")

                submit_file_content.append(f"case ${procid} in")
                for idx, wrapper_path in enumerate(executable_wrappers):
                    submit_file_content.append(f"  {idx})")
                    submit_file_content.append(f"    exec {pathlib.Path(wrapper_path).resolve()}")
                    submit_file_content.append("    ;;")
                submit_file_content.append("  *)")
                submit_file_content.append(f"    echo \"Invalid {procid}: ${procid}\" >&2")
                submit_file_content.append("    exit 1")
                submit_file_content.append("    ;;")
                submit_file_content.append("esac")

        # Now we can write the submit file
        output_path = pathlib.Path(get_task_file_dir(self.task))
        submit_file_path = output_path / "slurm_parameters.sh"

        output_path.mkdir(parents=True, exist_ok=True)
        with open(submit_file_path, "w") as submit_file:
            submit_file.write("\n".join(submit_file_content))
        return submit_file_path
