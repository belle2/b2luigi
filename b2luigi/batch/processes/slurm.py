import math
import subprocess
import pathlib
import re
import getpass
import stat
from enum import StrEnum
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from luigi.parameter import _no_value
from b2luigi.core.settings import get_setting
import luigi.scheduler
from b2luigi.batch.processes import (
    BatchProcess,
    JobStatus,
    aggregate_job_status,
    expand_grouped_task,
    write_failed_jobs_log,
)
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
            - Job IDs are kept as strings. Array jobs report IDs like ``123456_7``, which are not
              integers, and the output covers every job of the user, including job arrays that
              were not submitted by b2luigi.
            - Job states with a '+' suffix (e.g., 'CANCELLED+') are normalized by
              stripping the '+' character.
            - Array elements that have not started yet are summarised by Slurm in one line, e.g.
              ``123456_[5-9,12%4]``. Such a line is expanded into one entry per element
              (``123456_5``, ..., ``123456_12``), see :obj:`_expand_array_job_id`.
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
            state = self._get_SlurmJobStatus_from_string(state_string)
            for element_id in self._expand_array_job_id(id):
                self[element_id] = state
                seen_ids.add(element_id)

        return seen_ids

    @staticmethod
    def _expand_array_job_id(job_id: str) -> list[str]:
        """
        Expand Slurm's summary notation for pending array elements into the individual element ids.

        ``squeue`` and ``sacct`` list array elements that have not started yet in a single line,
        e.g. ``123456_[0-3,7,10-14:2%4]``: comma separated indices or ranges, with an optional
        ``:step`` per range and an optional ``%`` throttle limit for the whole array.

        Args:
            job_id (str): A job id as reported by Slurm.

        Returns:
            list[str]: ``[job_id]`` for a plain job or a single array element, otherwise one
            ``<base id>_<index>`` entry per element.
        """
        match = re.fullmatch(r"(?P<base>\d+)_\[(?P<indices>[^\]]+)\]", job_id)
        if not match:
            return [job_id]

        base = match.group("base")
        # The "%" throttle limit only limits how many elements run at once.
        index_spec = match.group("indices").split("%")[0]

        element_ids = []
        for part in index_spec.split(","):
            index_range, _, step = part.partition(":")
            first, _, last = index_range.partition("-")
            for index in range(int(first), int(last or first) + 1, int(step or 1)):
                element_ids.append(f"{base}_{index}")
        return element_ids

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

#: Allowed values of the ``submission_type`` setting, see :obj:`SlurmProcess`.
SUBMISSION_TYPES = ("single", "array", "mpi")


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

    * Parameter grouping (see :ref:`parameter-grouping-label`) is supported. When a task has a grouped,
      tuple-valued parameter, the ``submission_type`` setting controls how the resulting sub-tasks are submitted:

      - ``"single"`` (default): one independent ``sbatch`` submission per sub-task.
      - ``"array"``: all sub-tasks are submitted as a single Slurm job array
        (``#SBATCH --array=...``), dispatched by ``$SLURM_ARRAY_TASK_ID``. Every array element
        is tracked as a job of its own (``<job id>_<index>``).
      - ``"mpi"``: all sub-tasks are submitted as a single Slurm job spanning multiple nodes,
        dispatched by ``$SLURM_PROCID`` after being launched via ``srun``. Use the
        ``tasks_per_node`` setting to tell b2luigi how many sub-tasks may be packed onto a single
        node (defaults to 64); b2luigi does not query the cluster's hardware configuration, so
        this must match your cluster's partition.

      In all three cases every sub-task writes its ``stdout``/``stderr`` into its own log directory,
      and the task is only reported successful once every sub-task has finished successfully.

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

        # One id per submitted job: a single entry for a plain task, one per scalar
        # sub-task for a parameter-grouped task (see :obj:`expand_grouped_task`).
        # For submission_type "array" these are the array elements ("<job id>_<index>"),
        # for "mpi" the single job that runs all sub-tasks.
        self._batch_job_ids: list[str] = []
        # Log directory of every submitted job, keyed by job id, for ``failed_jobs.log``.
        self._job_log_dirs: dict[str, str] = {}
        # For submission_type "mpi": the log directories of all sub-tasks run by one job.
        self._job_sub_task_log_dirs: dict[str, list[str]] = {}

    def get_job_status(self) -> JobStatus:
        """
        Determine the status of the task from the Slurm status of all its jobs.

        A plain task has exactly one job. A parameter-grouped task has one job per
        scalar sub-task, and their statuses are combined with :obj:`aggregate_job_status`:
        running while any job runs, aborted if any job failed, successful only when all did.
        When the task is aborted, a ``failed_jobs.log`` listing the failed job ids and their log
        directories is written into the task's log directory (see :obj:`write_failed_jobs_log`).

        Returns:
            JobStatus: The aggregated status, or :meth:`JobStatus.aborted <b2luigi.process.JobStatus.aborted>`
            if no job was submitted.
        """
        job_statuses = {job_id: self._get_job_status_for_id(job_id) for job_id in self._batch_job_ids}
        status = aggregate_job_status(job_statuses.values())
        if status == JobStatus.aborted:
            failed_jobs = {}
            for job_id, job_status in job_statuses.items():
                if job_status != JobStatus.aborted:
                    continue
                if job_id in self._job_sub_task_log_dirs:
                    # An "mpi" job runs several sub-tasks, and Slurm does not tell which of them failed.
                    for index, log_dir in enumerate(self._job_sub_task_log_dirs[job_id]):
                        failed_jobs[f"{job_id} (task {index})"] = log_dir
                else:
                    failed_jobs[job_id] = self._job_log_dirs.get(job_id, "")
            write_failed_jobs_log(self.task, failed_jobs)
        return status

    @staticmethod
    def _get_job_status_for_id(job_id: str) -> JobStatus:
        """
        Determine the status of one batch job based on its Slurm job status.

        Returns:
            JobStatus: The status of the job, which can be one of the following:
                - :meth:`JobStatus.successful <b2luigi.process.JobStatus.successful>`: If the job has completed successfully.
                - :meth:`JobStatus.running <b2luigi.process.JobStatus.running>`: If the job is currently running, pending, suspended, preempted, or completing.
                - :meth:`JobStatus.aborted <b2luigi.process.JobStatus.aborted>`: If the job has failed, been cancelled, exceeded its deadline, encountered a node failure,
                  ran out of memory, timed out, or if the job ID is not found.

        Raises:
            ValueError: If the Slurm job status is unknown or not handled.
        """
        try:
            job_status = _batch_job_status_cache[job_id]
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
            SlurmJobStatus.configuring,
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

        For a parameter-grouped task (see :ref:`parameter-grouping-label`) only the scalar sub-tasks
        that are not complete yet are submitted. If every sub-task is already complete, nothing is
        submitted and the task is reported as done. How the sub-tasks are submitted depends on the
        ``submission_type`` setting:

        - ``"single"`` (default): one submit file and one ``sbatch`` call per sub-task.
        - ``"array"``: one submit file and one ``sbatch`` call for a job array with one element per
          sub-task. Every element is tracked as a job of its own (``<job id>_<index>``).
        - ``"mpi"``: one submit file and one ``sbatch`` call for a job running every sub-task as
          one ``srun`` task.

        A task without grouped parameters is always submitted as ``"single"``.

        Raises:
            RuntimeError: If the batch submission fails or the job ID cannot be extracted.
            ValueError: If the ``submission_type`` setting is unknown.

        Attributes:
            self._batch_job_ids (list[str]): The IDs of the submitted Slurm batch jobs.
        """
        sub_tasks = expand_grouped_task(self.task)
        if not sub_tasks:
            self._put_to_result_queue(status=luigi.scheduler.DONE, explanation="")
            self._terminated = True
            return

        submission_type = get_setting("submission_type", task=self.task, default="single")
        if submission_type not in SUBMISSION_TYPES:
            raise ValueError(
                f"Unknown submission_type {submission_type!r}, expected one of {', '.join(map(repr, SUBMISSION_TYPES))}."
            )

        if submission_type == "single" or not self.task.is_grouped():
            for sub_task in sub_tasks:
                job_id = self._submit(self._create_slurm_submit_file(sub_task))
                self._batch_job_ids.append(job_id)
                # Avoid a full squeue for the first status check of every new job
                _batch_job_status_cache.seed_submitted([job_id], SlurmJobStatus.pending)
                self._job_log_dirs[job_id] = get_log_file_dir(sub_task)
            return

        # The whole group is submitted as one Slurm job.
        job_id = self._submit(self._create_slurm_submit_file(sub_tasks=sub_tasks, submission_type=submission_type))
        if submission_type == "array":
            for index, sub_task in enumerate(sub_tasks):
                element_id = f"{job_id}_{index}"
                self._batch_job_ids.append(element_id)
                self._job_log_dirs[element_id] = get_log_file_dir(sub_task)
        else:
            self._batch_job_ids.append(job_id)
            self._job_sub_task_log_dirs[job_id] = [get_log_file_dir(sub_task) for sub_task in sub_tasks]
        # Avoid a full squeue for the first status check of every new job
        _batch_job_status_cache.seed_submitted(self._batch_job_ids, SlurmJobStatus.pending)

    @staticmethod
    def _submit(submit_file) -> str:
        """
        Submit a Slurm submit file with ``sbatch``.

        Args:
            submit_file: The path of the submit file.

        Returns:
            str: The job ID reported by ``sbatch``. For a job array this is the ID of the array.

        Raises:
            RuntimeError: If the batch submission fails or the job ID cannot be extracted.
        """
        # Slurm submit needs to be called in the folder of the submit file
        path = pathlib.Path(submit_file)
        output = subprocess.check_output(["sbatch", path.name], cwd=path.parent)

        output = output.decode()
        match = re.search(r"[0-9]+", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)
        return match.group(0)

    def terminate_job(self):
        """
        Terminates all batch jobs of this task if any were submitted.

        This method executes a single ``scancel`` command with every submitted job ID
        (one for a plain task, several for a parameter-grouped task). The command's
        output is suppressed.

        Array elements (``<job id>_<index>``) are cancelled via the job ID of their array, which
        also cancels elements that have not started yet.
        """
        if not self._batch_job_ids:
            return
        # dict.fromkeys removes the duplicates of the array elements while keeping the order
        job_ids = dict.fromkeys(str(job_id).split("_")[0] for job_id in self._batch_job_ids)
        subprocess.run(["scancel", *job_ids], stdout=subprocess.DEVNULL)

    def _create_slurm_submit_file(self, task=None, sub_tasks=None, submission_type="single"):
        """
        Creates a Slurm submit file for a task.

        This method generates a Slurm batch script that specifies the necessary
        configurations for submitting a job to a Slurm workload manager.

        Args:
            task: The task to write the submit file for. Defaults to ``self.task``; a
                parameter-grouped task passes each scalar sub-task in turn.
            sub_tasks: For ``submission_type`` ``"array"`` or ``"mpi"``: the scalar sub-tasks of
                the group ``task`` that are submitted together in this one job.
            submission_type: ``"single"`` (default), ``"array"`` or ``"mpi"``.

        Returns:
            pathlib.Path: The path to the generated Slurm submit file.

        Note:
            - The ``stdout`` and ``stderr`` log files are created in the task's log directory. See :obj:`get_log_file_dir`.
            - The Slurm settings are specified in the ``slurm_settings`` setting, which is a dictionary of key-value pairs.
            - The ``job_name`` setting can be used to specify a meaningful name for the job.
            - The executable is created with :obj:`create_executable_wrapper`.
            - The submit file is named `slurm_parameters.sh` and is created in the task's output directory (:obj:`get_task_file_dir`).
            - For ``submission_type`` ``"array"`` or ``"mpi"``, the ``$SLURM_ARRAY_TASK_ID``/``$SLURM_PROCID`` dispatch
              logic is written to a separate `slurm_dispatch.sh` file in the same directory, which the submit file
              executes (see :obj:`_create_slurm_dispatch_file`). The task's own ``stdout``/``stderr`` then only
              receive the output of the dispatch itself; every sub-task logs into its own log directory.
        """
        if task is None:
            task = self.task

        submit_file_content = ["#!/usr/bin/bash"]

        # Specify where to write the log to
        log_file_dir = pathlib.Path(get_log_file_dir(task))
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

        task_slurm_settings = get_setting("slurm_settings", task=task, default=_no_value)
        if task_slurm_settings != _no_value:
            general_settings.update(task_slurm_settings)

        job_name = get_setting("job_name", task=task, default=False)
        if job_name is not False:
            general_settings.setdefault("job-name", job_name)

        if sub_tasks is not None and submission_type != "single":
            group_settings, command = self._create_slurm_dispatch_file(task, sub_tasks, submission_type)
            # A new dict, so that the group settings do not end up in the ``slurm_settings`` setting itself
            general_settings = {**general_settings, **group_settings}
        else:
            # Specify the executable
            executable_file = create_executable_wrapper(task)
            command = f"exec {pathlib.Path(executable_file).resolve()}"

        for key, item in general_settings.items():
            submit_file_content.append(f"#SBATCH --{key}={item}")

        submit_file_content.append(command)

        # Now we can write the submit file
        output_path = pathlib.Path(get_task_file_dir(task))
        submit_file_path = output_path / "slurm_parameters.sh"

        output_path.mkdir(parents=True, exist_ok=True)
        with open(submit_file_path, "w") as submit_file:
            submit_file.write("\n".join(submit_file_content))
        return submit_file_path

    @staticmethod
    def _create_slurm_dispatch_file(task, sub_tasks, submission_type):
        """
        Creates the ``slurm_dispatch.sh`` file of a group submitted as one job array or MPI job.

        The dispatch file selects the sub-task to run from ``$SLURM_ARRAY_TASK_ID`` (``"array"``)
        or ``$SLURM_PROCID`` (``"mpi"``) and executes its executable wrapper, redirecting its output
        into the sub-task's own log directory, the same place a ``"single"`` submission writes to.
        The dispatch logic lives in its own file rather than being inlined into the submit script,
        so it can simply be executed once per sub-task with the right index already set in its
        environment.

        Args:
            task: The grouped task; the dispatch file is written into its task file directory.
            sub_tasks: The scalar sub-tasks to run, in index order.
            submission_type: ``"array"`` or ``"mpi"``.

        Returns:
            tuple[dict, str]: The additional ``#SBATCH`` settings for the submit file, and the
            command that starts the dispatch file.

        Raises:
            ValueError: If ``submission_type`` is not ``"array"`` or ``"mpi"``, or the
                ``tasks_per_node`` setting is smaller than 1.
        """
        n_sub_tasks = len(sub_tasks)

        if submission_type == "array":
            group_settings = {"array": f"0-{n_sub_tasks - 1}"}
            # Slurm starts one independent job instance per array index, each already having
            # SLURM_ARRAY_TASK_ID set, so the dispatch file can simply be executed directly.
            index_variable = "SLURM_ARRAY_TASK_ID"
            launcher = "exec"
        elif submission_type == "mpi":
            # b2luigi does not query the cluster's hardware itself, so the number of sub-tasks
            # per node has to be provided by the user to match the target partition.
            tasks_per_node = get_setting("tasks_per_node", task=task, default=64)
            if tasks_per_node < 1:
                raise ValueError(f"tasks_per_node must be >= 1, got {tasks_per_node}")
            group_settings = {"nodes": math.ceil(n_sub_tasks / tasks_per_node), "ntasks": n_sub_tasks}
            # Unlike an array job, sbatch only starts this job's batch step once, and the
            # SLURM_PROCID environment variable is only set inside a job step started via
            # srun. The dispatch file therefore has to be launched through srun to get one
            # instance per sub-task, each with its own SLURM_PROCID.
            index_variable = "SLURM_PROCID"
            launcher = "exec srun"
        else:
            raise ValueError(f"Unknown submission type for a grouped submission: {submission_type}")

        dispatch_file_content = ["#!/usr/bin/bash", f"case ${index_variable} in"]
        for index, sub_task in enumerate(sub_tasks):
            executable_file = pathlib.Path(create_executable_wrapper(sub_task)).resolve()
            log_file_dir = pathlib.Path(get_log_file_dir(sub_task)).resolve()
            log_file_dir.mkdir(parents=True, exist_ok=True)
            dispatch_file_content.append(f"  {index})")
            dispatch_file_content.append(
                f"    exec {executable_file} > {log_file_dir / 'stdout'} 2> {log_file_dir / 'stderr'}"
            )
            dispatch_file_content.append("    ;;")
        dispatch_file_content.append("  *)")
        dispatch_file_content.append(f'    echo "Invalid {index_variable}: ${index_variable}" >&2')
        dispatch_file_content.append("    exit 1")
        dispatch_file_content.append("    ;;")
        dispatch_file_content.append("esac")

        dispatch_file_path = pathlib.Path(get_task_file_dir(task)) / "slurm_dispatch.sh"
        dispatch_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dispatch_file_path, "w") as dispatch_file:
            dispatch_file.write("\n".join(dispatch_file_content))
        # Started by its absolute path, like the executable wrapper, so that it needs no PATH: with
        # ``export=NONE`` the job (and every srun task) starts without the submitting environment.
        dispatch_file_path.chmod(dispatch_file_path.stat().st_mode | stat.S_IEXEC)

        return group_settings, f"{launcher} {dispatch_file_path.resolve()}"
