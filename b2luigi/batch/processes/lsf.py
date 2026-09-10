import json
import re
import subprocess
import os
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

import luigi.scheduler

from b2luigi.batch.processes import (
    BatchProcess,
    JobStatus,
    aggregate_job_status,
    expand_grouped_task,
    write_failed_jobs_log,
)
from b2luigi.batch.cache import BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir
from b2luigi.core.executable import create_executable_wrapper
from b2luigi.core.settings import get_setting


class LSFJobStatusCache(BatchJobStatusCache):
    @retry(
        retry=retry_if_exception_type(subprocess.CalledProcessError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, exp_base=3),  # 2, 6, 18 seconds
        reraise=True,
    )
    def _ask_for_job_status(self, job_id=None):
        """
        Queries the job status from the LSF batch system and updates the internal job status mapping.

        Args:
            job_id (str, optional): The ID of the job to query. If not provided,
                                    the status of all jobs will be queried.

        Notes:
            - This method uses the ``bjobs`` command-line tool to fetch job statuses in JSON format.
            - The output is expected to contain a "RECORDS" key with a list of job records.
            - Each job record should have "JOBID" and "STAT" keys, which are used to update the internal mapping.
        """
        if job_id:
            output = subprocess.check_output(["bjobs", "-json", "-o", "jobid stat", str(job_id)])
        else:
            output = subprocess.check_output(["bjobs", "-json", "-o", "jobid stat"])
        output = output.decode()
        output = json.loads(output)["RECORDS"]

        for record in output:
            self[record["JOBID"]] = record["STAT"]


_batch_job_status_cache = LSFJobStatusCache()


class LSFProcess(BatchProcess):
    """
    Reference implementation of the batch process for a LSF batch system.
    Heavily inspired by `this post <https://github.com/spotify/luigi/pull/2373/files>`_.

    Additional to the basic batch setup (see :ref:`batch-label`), there are
    LSF-specific :meth:`settings <b2luigi.set_setting>`. These are:

    * the LSF queue: ``queue``.
    * the number of slots for the job. On KEKCC this increases the memory available to the job: ``job_slots``.
    * the LSF job name: ``job_name``.

    Parameter grouping (see :ref:`parameter-grouping-label`) is supported: a grouped task is
    submitted as one ``bsub`` call per scalar sub-task, and the task is only reported
    successful once every one of these jobs has finished successfully.

    For example:

    .. code-block:: python

        class MyLongTask(b2luigi.Task):
            queue = "l"
            job_name = "my_long_task"

    The default queue is the short queue ``"s"``. If no ``job_name`` is set the task
    will appear as ::

        <result_dir>/parameter1=value/.../executable_wrapper.sh"

    when running ``bjobs``.

    By default, the environment variables from the scheduler are copied to
    the workers.
    This also implies we start in the same working directory, can reuse
    the same executable, etc.
    Normally, you do not need to supply ``env_script`` or alike.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # One id per submitted job: a single entry for a plain task, one per scalar
        # sub-task for a parameter-grouped task (see :obj:`expand_grouped_task`).
        self._batch_job_ids: list[str] = []
        # Log directory of every submitted job, keyed by job id, for ``failed_jobs.log``.
        self._job_log_dirs: dict[str, str] = {}

    def get_job_status(self) -> JobStatus:
        """
        Determine the status of the task from the LSF status of all its jobs.

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
            write_failed_jobs_log(
                self.task,
                {
                    job_id: self._job_log_dirs.get(job_id, "")
                    for job_id, job_status in job_statuses.items()
                    if job_status == JobStatus.aborted
                },
            )
        return status

    @staticmethod
    def _get_job_status_for_id(job_id: str) -> JobStatus:
        """
        Retrieves the current status of one batch job.

        Returns:
            JobStatus: The status of the job, which can be one of the following:
                - :meth:`JobStatus.successful <b2luigi.process.JobStatus.successful>`: If the job has completed successfully ("DONE").
                - :meth:`JobStatus.aborted <b2luigi.process.JobStatus.aborted>`: If the job has been aborted or is not found in the cache ("EXIT" or missing ID).
                - :meth:`JobStatus.running <b2luigi.process.JobStatus.running>`: If the job is still in progress.
        """
        try:
            job_status = _batch_job_status_cache[job_id]
        except KeyError:
            return JobStatus.aborted

        if job_status == "DONE":
            return JobStatus.successful
        if job_status == "EXIT":
            return JobStatus.aborted

        return JobStatus.running

    def start_job(self):
        """
        Submits a batch job to the LSF system.

        This method constructs a command to submit a job using the ``bsub`` command-line tool.
        It dynamically configures the job submission parameters based on task-specific settings
        and creates necessary log files for capturing standard output and error.

        For a parameter-grouped task (see :ref:`parameter-grouping-label`) one ``bsub`` call is
        made per scalar sub-task that is not complete yet. If every sub-task is already
        complete, nothing is submitted and the task is reported as done.

        Raises:
            RuntimeError: If the batch submission fails or the job ID cannot be extracted
                          from the ``bsub`` command output.

        Steps:
            1. Retrieve optional settings for ``queue`` (``-q``), ``job_slots`` (``-n``), and ``job_name`` (``-J``).
            2. The ``stdout`` and ``stderr`` log files are created in the task's log directory. See :obj:`get_log_file_dir`.
            3. The executable is created with :obj:`create_executable_wrapper`.
        """
        sub_tasks = expand_grouped_task(self.task)
        if not sub_tasks:
            self._put_to_result_queue(status=luigi.scheduler.DONE, explanation="")
            self._terminated = True
            return

        for sub_task in sub_tasks:
            job_id = self._submit_task(sub_task)
            self._batch_job_ids.append(job_id)
            self._job_log_dirs[job_id] = get_log_file_dir(sub_task)

    @staticmethod
    def _submit_task(task) -> str:
        """
        Submit one task with ``bsub`` and return its LSF job ID.

        Args:
            task: The task to submit. Its settings, log directory and executable wrapper are used.

        Returns:
            str: The LSF job ID parsed from the ``bsub`` output.

        Raises:
            RuntimeError: If the job ID cannot be extracted from the ``bsub`` output.
        """
        command = ["bsub", "-env all"]

        queue = get_setting("queue", task=task, default=False)
        if queue is not False:
            command += ["-q", queue]

        job_slots = str(get_setting("job_slots", task=task, default=False))
        if job_slots is not str(False):
            command += ["-n", job_slots]

        job_name = get_setting("job_name", task=task, default=False)
        if job_name is not False:
            command += ["-J", job_name]

        log_file_dir = get_log_file_dir(task)
        os.makedirs(log_file_dir, exist_ok=True)

        stdout_log_file = os.path.join(log_file_dir, "stdout")
        stderr_log_file = os.path.join(log_file_dir, "stderr")

        command += ["-eo", stderr_log_file, "-oo", stdout_log_file]

        executable_file = create_executable_wrapper(task)
        command.append(executable_file)

        output = subprocess.check_output(command)
        output = output.decode()

        # Output of the form Job <72065926> is submitted to default queue <s>.
        match = re.search(r"<[0-9]+>", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        return match.group(0)[1:-1]

    def terminate_job(self):
        """
        Terminate all batch jobs of this task if any were submitted, with a single
        ``bkill`` command listing every job ID. The command's output is suppressed,
        and errors during execution are not raised.
        """
        if not self._batch_job_ids:
            return

        subprocess.run(["bkill", *self._batch_job_ids], stdout=subprocess.DEVNULL, check=False)
