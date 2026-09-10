import enum
import os
import time

import luigi
import luigi.scheduler

from b2luigi.core.utils import get_log_file_dir, on_failure, get_luigi_logger


logger = get_luigi_logger()


class JobStatus(enum.Enum):
    running = "running"
    successful = "successful"
    aborted = "aborted"
    idle = "idle"


def expand_grouped_task(task: luigi.Task) -> list[luigi.Task]:
    """
    Expand a parameter-grouped task into the scalar sub-tasks that still need to run.

    luigi's batching hands a batch process one task instance whose grouped parameters
    (see :ref:`parameter-grouping-label`) carry a *tuple* of values. Every batch system
    that supports grouping submits one job per element of that tuple, so the expansion
    lives here and is shared by all of them.

    :param task: The task handed to the batch process.
    :return: ``[task]`` when the task has no grouped parameters or luigi did not batch it
        (the grouped value is a plain scalar). Otherwise one clone per tuple index with the
        grouped parameters set to their scalar values, **excluding** clones whose output
        already exists, so a resubmission only re-runs the failed members of the group.
        An empty list means every member is already complete and nothing must be submitted.
    """
    grouped_params = task.grouped_param_names()
    if not grouped_params or not isinstance(task.param_kwargs[grouped_params[0]], tuple):
        return [task]

    n_values = len(task.param_kwargs[grouped_params[0]])
    sub_tasks = []
    for i in range(n_values):
        sub_task = task.clone(None, **{param: task.param_kwargs[param][i] for param in grouped_params})
        # If a sub_task was already successful do not resubmit it
        if sub_task.complete():
            continue
        sub_tasks.append(sub_task)
    return sub_tasks


def write_failed_jobs_log(task: luigi.Task, failed_jobs: dict) -> str:
    """
    Record which jobs of a (possibly grouped) task failed, in the task's own log directory.

    :obj:`on_failure <b2luigi.core.utils.on_failure>` points the user at
    :obj:`get_log_file_dir(task) <b2luigi.core.utils.get_log_file_dir>`. For a parameter-grouped
    task (see :ref:`parameter-grouping-label`) that is the directory of the *group*, while every
    sub-task writes its ``stdout``/``stderr`` into its own scalar directory, so without this file
    the advertised directory would not even exist. Mirrors the ``failed_jobs.log`` HTCondor writes.

    :param task: The task the batch process was created for (the group, if grouped).
    :param failed_jobs: Mapping of every failed batch job id to the log directory of the job.
    :return: The path of the written ``failed_jobs.log``.
    """
    log_file_dir = get_log_file_dir(task)
    os.makedirs(log_file_dir, exist_ok=True)
    failed_jobs_log = os.path.join(log_file_dir, "failed_jobs.log")
    with open(failed_jobs_log, "w") as f:
        for job_id, job_log_dir in failed_jobs.items():
            f.write(f"{job_id}: {job_log_dir}\n")
    return failed_jobs_log


def aggregate_job_status(statuses) -> JobStatus:
    """
    Collapse the statuses of the jobs of one (possibly grouped) task into a single status.

    :param statuses: An iterable of :obj:`JobStatus` values, one per submitted job.
    :return: :attr:`JobStatus.aborted` if no job was submitted at all, :attr:`JobStatus.running`
        while any job still runs, otherwise :attr:`JobStatus.aborted` if any job failed, otherwise
        :attr:`JobStatus.successful`. A group is only successful when every member is, and an
        empty group has nothing to be successful about, so it fails closed.
    """
    statuses = list(statuses)
    if not statuses:
        return JobStatus.aborted
    if any(status == JobStatus.running for status in statuses):
        return JobStatus.running
    if any(status == JobStatus.aborted for status in statuses):
        return JobStatus.aborted
    return JobStatus.successful


class BatchProcess:
    """
    This is the base class for all batch algorithms that allow luigi to run on a specific batch system.
    This is an abstract base class and inheriting classes need to supply functionalities for

    * starting a job using the commands in ``self.task_cmd``
    * getting the job status of a running, finished or failed job
    * and terminating a job

    All those commands are called from the main process, which is not running on the batch system.
    Every batch system that is capable of these functions can in principle work together with ``b2luigi``.

    Implementation note:
        In principle, using the batch system is transparent to the user. In case of problems, it
        may however be useful to understand how it is working.

        When you start your ``luigi`` dependency tree with ``process(..., batch=True)``, the normal
        ``luigi`` process is started looking for unfinished tasks and running them etc.
        Normally, luigi creates a process for each running task and runs them either directly
        or on a different core (if you have enabled more than one worker).
        In the batch case, this process is not a normal python multiprocessing process,
        but this :obj:`BatchProcess`, which has the same interface (one can check the status of the process,
        start or terminate it). The process does not need to wait for the batch job to finish but
        is asked repeatedly for the job status. By this, most of the core functionality of ``luigi``
        is kept and reused.
        This also means, that every batch job only includes a single task and is finished whenever
        this task is done decreasing the batch runtime. You will need exactly as many batch jobs
        as you have tasks and no batch job will idle waiting for input data as all are scheduled
        only when the task they should run is actually runnable (the input files are there).

        What is the batch command now? In each job, we call a specific executable bash script
        only created for this task. It contains the setup of the environment (if given by the
        user via the settings), the change of the working directory (the directory of the
        python script or a specified directory by the user) and a call of this script with the
        current python interpreter (the one you used to call this main file or given by the
        setting ``executable``) . However, we give this call an additional parameter, which tells it
        to only run one single task. Task can be identified by their task id. A typical task command may look like::

            /<path-to-your-exec>/python /your-project/some-file.py --batch-runner --task-id MyTask_38dsf879w3

        if the batch job should run the ``MyTask``. The implementation of the
        abstract functions is responsible for creating an running the executable file and writing the log of
        the job into appropriate locations. You can use the functions :meth:`create_executable_wrapper <b2luigi.core.executable.create_executable_wrapper>`
        and :meth:`get_log_file_dir <b2luigi.core.utils.get_log_file_dir>` to get the needed information.

        Checkout the implementation of the ``lsf`` task for some implementation example.
    """

    def __init__(self, task, scheduler, result_queue, worker_timeout):
        self.use_multiprocessing = False
        self.task = task
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None
        self._terminated = False

        self._result_queue = result_queue
        self._scheduler = scheduler

    @property
    def exitcode(self):
        """
        Retrieves the exit code for the process.

        This method always returns ``0``, which indicates successful execution.
        By consistently setting the exit code to ``0``, the result queue can be
        reliably used for delivering the result of the process.

        Returns:
            int: The exit code, always ``0``.
        """
        # We cheat here a bit: if the exit code is set to 0 all the time, we can always use the result queue for
        # delivering the result
        return 0

    def get_job_status(self):
        """
        Implement this function to return the current job status.
        How you identify exactly your job is dependent on the implementation and needs to
        be handled by your own child class.

        Must return one item of the JobStatus enumeration: running, aborted, successful or idle.
        Will only be called after the job is started but may also be called when
        the job is finished already.
        If the task status is unknown, return aborted. If the task has not started already but
        is scheduled, return running nevertheless (for ``b2luigi`` it makes no difference).
        No matter if aborted via a call to ``terminate_job``, by the batch system or by an exception in the
        job itself, you should return aborted if the job is not finished successfully
        (maybe you need to check the exit code of your job).
        """
        raise NotImplementedError

    def start_job(self):
        """
        Override this function in your child class to start a job on the batch system.
        It is called exactly once. You need to store any information identifying
        your batch job on your own.

        You can use the ``b2luigi.core.utils.get_log_file_dir`` and the
        ``b2luigi.core.executable.create_executable_wrapper`` functions to get the log base name
        and to create the executable script which you should call in your batch job.

        After the ``start_job`` function is called by the framework (and no exception is thrown),
        it is assumed that a batch job is started or scheduled.

        After the job is finished (no matter if aborted or successful) we assume the stdout and stderr
        is written into the two files given by ``b2luigi.core.utils.get_log_file_dir(self.task)``.
        """
        raise NotImplementedError

    def terminate_job(self):
        """
        This command is used to abort a job started by the ``start_job`` function.
        It is only called once to abort a job, so make sure to either block until the job is really
        gone or be sure that it will go down soon. Especially, do not wait until the job is finished.
        It is called for example when the user presses ``Ctrl-C``.

        In some strange corner cases it may happen that this function is called even before the
        job is started (the ``start_job`` function is called). In this case, you do not need to do anything
        (but also not raise an exception).
        """
        raise NotImplementedError

    def run(self):
        """
        Executes the batch process.

        This method logs the start of the batch process, including the class name
        and associated task, and then initiates the job by calling ``start_job``.
        """
        logger.info("Batch process %s running  %s", self.__class__.__name__, self.task)
        self.start_job()

    def terminate(self):
        """
        Terminates the current process by invoking the ``terminate_job`` method.

        This method is responsible for ensuring that the associated job or process
        is properly terminated. It acts as a wrapper around the ``terminate_job``
        method, which contains the specific logic for termination.
        """
        self.terminate_job()

    def is_alive(self):
        """
        Check if the job is still alive based on its current status.

        Returns:
            bool: ``True`` if the job is running, ``False`` if it has terminated
                  (either successfully or due to failure).

        Raises:
            ValueError: If the job status returned by :obj:`get_job_status`
                        is not recognized.

        Behavior:
            - If the job has terminated successfully, it updates the result queue
              with a "DONE" status and marks the job as terminated.
            - If the job has been aborted, it calls the `on_failure` handler,
              updates the result queue with a "FAILED" status, and marks the job
              as terminated.
            - If the job is still running, it returns True.
        """
        if self._terminated:
            return False

        job_status = self.get_job_status()

        if job_status == JobStatus.successful:
            self._put_to_result_queue(status=luigi.scheduler.DONE, explanation="")
            self._terminated = True
            return False
        if job_status == JobStatus.aborted:
            explanation = on_failure(self.task, None)
            self._put_to_result_queue(status=luigi.scheduler.FAILED, explanation=explanation)
            self._terminated = True
            return False
        if job_status == JobStatus.running:
            return True

        raise ValueError("get_job_status() returned an unknown job state!")

    def _put_to_result_queue(self, status, explanation):
        missing = []
        new_deps = []
        self._result_queue.put((self.task.task_id, status, explanation, missing, new_deps))
