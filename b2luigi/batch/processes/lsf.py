import json
import re
import subprocess
import os
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from b2luigi.batch.processes import BatchProcess, JobStatus
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

        self._batch_job_id = None

    def get_job_status(self):
        """
        Retrieves the current status of the batch job associated with this instance.

        Returns:
            JobStatus: The status of the job, which can be one of the following:
                - :meth:`JobStatus.successful <b2luigi.process.JobStatus.successful>`: If the job has completed successfully ("DONE").
                - :meth:`JobStatus.aborted <b2luigi.process.JobStatus.aborted>`: If the job has been aborted or is not found in the cache ("EXIT" or missing ID).
                - :meth:`JobStatus.running <b2luigi.process.JobStatus.running>`: If the job is still in progress.
        """
        if not self._batch_job_id:
            return JobStatus.aborted

        try:
            job_status = _batch_job_status_cache[self._batch_job_id]
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

        Raises:
            RuntimeError: If the batch submission fails or the job ID cannot be extracted
                          from the ``bsub`` command output.

        Steps:
            1. Retrieve optional settings for ``queue`` (``-q``), ``job_slots`` (``-n``), and ``job_name`` (``-J``).
            2. The ``stdout`` and ``stderr`` log files are created in the task's log directory. See :obj:`get_log_file_dir`.
            3. The executable is created with :obj:`create_executable_wrapper`.
        """
        command = ["bsub", "-env all"]

        queue = get_setting("queue", task=self.task, default=False)
        if queue is not False:
            command += ["-q", queue]

        job_slots = get_setting("job_slots", task=self.task, default=False)
        if job_slots is not False:
            command += ["-n", job_slots]

        job_name = get_setting("job_name", task=self.task, default=False)
        if job_name is not False:
            command += ["-J", job_name]

        log_file_dir = get_log_file_dir(self.task)
        os.makedirs(log_file_dir, exist_ok=True)

        stdout_log_file = os.path.join(log_file_dir, "stdout")
        stderr_log_file = os.path.join(log_file_dir, "stderr")

        command += ["-eo", stderr_log_file, "-oo", stdout_log_file]

        executable_file = create_executable_wrapper(self.task)
        command.append(executable_file)

        output = subprocess.check_output(command)
        output = output.decode()

        # Output of the form Job <72065926> is submitted to default queue <s>.
        match = re.search(r"<[0-9]+>", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_id = match.group(0)[1:-1]

    def terminate_job(self):
        """
        This method checks if a batch job ID is set. If it exists, it attempts to
        terminate the job using the ``bkill`` command. The command's output is suppressed,
        and errors during execution are not raised.
        """
        if not self._batch_job_id:
            return

        subprocess.run(["bkill", self._batch_job_id], stdout=subprocess.DEVNULL, check=False)
