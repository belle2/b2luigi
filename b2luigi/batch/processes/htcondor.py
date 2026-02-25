import json
import os
import re
import subprocess
import enum
import time
from itertools import chain

import luigi

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from b2luigi.core.settings import get_setting
from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.batch.cache import BatchJobStatusCache
from b2luigi.core.utils import get_log_file_dir, get_luigi_logger, get_task_file_dir
from b2luigi.core.executable import create_executable_wrapper


class HTCondorJobStatusCache(BatchJobStatusCache):
    @retry(
        retry=retry_if_exception_type(subprocess.CalledProcessError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, exp_base=3),  # 2, 6, 18 seconds
        reraise=True,
    )
    def _ask_for_job_status(self, job_id: int = None):
        """
        With HTCondor, you can check the progress of your jobs using the ``condor_q`` command.
        If no ``JobId`` is given as argument, this command shows you the status of all queued jobs
        (usually only your own by default).

        Normally the HTCondor ``JobID`` is stated as ``ClusterId.ProcId``. Since only on job is queued per
        cluster, we can identify jobs by their ``ClusterId`` (The ``ProcId`` will be ``0`` for all submitted jobs).
        With the ``-json`` option, the ``condor_q`` output is returned in the JSON format. By specifying some
        attributes, not the entire job ``ClassAd`` is returned, but only the necessary information to match a
        job to its :meth:`JobStatus <b2luigi.process.JobStatus>`. The output is given as string and cannot be directly parsed into a json
        dictionary. It has the following form:

        .. code-block:: text

            [
                {...}
                ,
                {...}
                ,
                {...}
            ]

        The ``{...}`` are the different dictionaries including the specified attributes.
        Sometimes it might happen that a job is completed in between the status checks. Then its final status
        can be found in the ``condor_history`` file (works mostly in the same way as ``condor_q``).
        Both commands are used in order to find out the :meth:`JobStatus <b2luigi.process.JobStatus>`.
        """
        logger = get_luigi_logger()
        # https://htcondor.readthedocs.io/en/latest/man-pages/condor_q.html
        q_cmd = ["condor_q", "-json", "-attributes", "ClusterId,JobStatus,ExitStatus,ExitCode,UserLog"]

        if job_id:
            output = subprocess.check_output(q_cmd + [str(job_id)])
        else:
            output = subprocess.check_output(q_cmd)

        seen_ids = self._fill_from_output(output)

        # If the specified job can not be found in the condor_q output, we need to request its history

        if job_id and job_id not in seen_ids:
            # https://htcondor.readthedocs.io/en/latest/man-pages/condor_history.html
            history_cmd = [
                "condor_history",
                "-json",
                "-attributes",
                "ClusterId,JobStatus,ExitCode,UserLog",
                "-match",
                "1",
                str(job_id),
            ]

            # as there can be a delay between jobs being available in condor_q and condor_history try multiple times
            for i in range(5):
                output = subprocess.check_output(history_cmd)
                seen_ids = self._fill_from_output(output)

                if len(seen_ids) > 0:
                    break
                logger.debug(f"Could not find status of job {job_id}! Trying again.")
                time.sleep((i + 1) * 2)
        else:
            # run condor_history for all jobs that are currently in the task flow, which is way faster then calling condor_history for each of them individually
            # only run this for the jobs that are not already in the cache
            history_ids = [
                str(job_id)
                for job_id in chain.from_iterable(self._job_ids)
                if job_id not in seen_ids and job_id not in self
            ]
            if len(history_ids) > 0:
                history_cmd = [
                    "condor_history",
                    "-json",
                    "-attributes",
                    "ClusterId,JobStatus,ExitCode,UserLog",
                    "-match",
                    str(len(history_ids)),
                ]
                history_cmd.extend(history_ids)
                output = subprocess.check_output(history_cmd)
                self._fill_from_output(output)

    def _fill_from_output(self, output):
        """
        Processes the output from an HTCondor job query and updates the job statuses.

        Args:
            output (bytes): The raw output from the HTCondor job query, encoded as bytes.

        Returns:
            set: A set of ``ClusterId`` values representing the jobs seen in the output.
        """
        output = output.decode()

        seen_ids = set()

        if not output:
            return seen_ids

        for status_dict in json.loads(output):
            # this can only happen if the status information comes from condor_q which does not provide an ExitCode
            if "ExitCode" not in status_dict.keys():
                if "ExitStatus" in status_dict.keys():
                    status_dict["ExitCode"] = status_dict["ExitStatus"]
                else:
                    status_dict["ExitCode"] = 1

            if status_dict["JobStatus"] == HTCondorJobStatus.completed and status_dict["ExitCode"]:
                self[status_dict["ClusterId"]] = (HTCondorJobStatus.failed, status_dict["UserLog"])
            else:
                self[status_dict["ClusterId"]] = (status_dict["JobStatus"], status_dict["UserLog"])

            seen_ids.add(status_dict["ClusterId"])

        return seen_ids


class HTCondorJobStatus(enum.IntEnum):
    """
    See https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html
    """

    idle = 1
    running = 2
    removed = 3
    completed = 4
    held = 5
    transferring_output = 6
    suspended = 7
    failed = 999


_batch_job_status_cache = HTCondorJobStatusCache()


class HTCondorProcess(BatchProcess):
    """
    Reference implementation of the batch process for a HTCondor batch system.

    Additional to the basic batch setup (see :ref:`batch-label`), additional
    HTCondor-specific things are:

    * Please note that most of the HTCondor batch farms do not have the same
      environment setup on submission and worker machines, so you probably want to give an
      ``env_script``, an ``env`` :meth:`setting <b2luigi.set_setting>` and/or a different ``executable``.

    * HTCondor supports copying files from submission to workers. This means if the
      folder of your script(s)/python project/etc. is not accessible on the worker, you can
      copy it from the submission machine by adding it to the setting ``transfer_files``.
      This list can host both folders and files.
      Please note that due to HTCondors file transfer mechanism, all specified folders
      and files will be copied into the worker node flattened, so if you specify
      `a/b/c.txt` you will end up with a file `c.txt`.
      If you use the ``transfer_files`` mechanism, you need to set the ``working_dir`` setting to "."
      as the files will end up in the current worker scratch folder.
      All specified files/folders should be absolute paths.

      .. hint::
        Please do not specify any parts or the full results folder. This will lead to unexpected
        behavior. We are working on a solution to also copy results, but until this the
        results folder is still expected to be shared.

      If you copy your python project using this setting to the worker machine, do not
      forget to actually set it up in your setup script.
      Additionally, you might want to copy your ``settings.json`` as well.

    * Via the ``htcondor_settings`` setting you can provide a dict as
      a for additional options, such as requested memory etc. Its value has to be a dictionary
      containing HTCondor settings as key/value pairs. These options will be written into the job
      submission file. For an overview of possible settings refer to the `HTCondor documentation
      <https://htcondor.readthedocs.io/en/latest/users-manual/submitting-a-job.html#>`_.

    * Same as for the :ref:`lsf`, the ``job_name`` setting allows giving a meaningful name to a
      group of jobs. If you want to be htcondor-specific, you can provide the ``JobBatchName`` as an
      entry in the ``htcondor_settings`` dict, which will override the global ``job_name`` setting.
      This is useful for manually checking the status of specific jobs with

      .. code-block:: bash

        condor_q -batch <job name>

    Example:

        .. literalinclude:: ../../examples/htcondor/htcondor_example.py
           :linenos:
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._batch_job_ids = []

    @staticmethod
    def get_job_status_for_id(job_id):
        """
        Determines the status of a batch job based on its HTCondor job status.

        Returns:
            JobStatus: The status of the job, which can be one of the following:
                - :meth:`JobStatus.successful <b2luigi.process.JobStatus.successful>`: If the HTCondor job status is 'completed'.
                - :meth:`JobStatus.running <b2luigi.process.JobStatus.running>`: If the HTCondor job status is one of 'idle',
                  'running', 'transferring_output', or 'suspended'.
                - :meth:`JobStatus.aborted <b2luigi.process.JobStatus.aborted>`: If the HTCondor job status is 'removed',
                  'held', 'failed', or if the job ID is not found in the cache.

        Raises:
            ValueError: If the HTCondor job status is unknown.
        """
        if not job_id:
            return JobStatus.aborted

        try:
            job_status, _ = _batch_job_status_cache[job_id]
        except KeyError:
            return JobStatus.aborted

        if job_status in [HTCondorJobStatus.completed]:
            return JobStatus.successful
        if job_status in [
            HTCondorJobStatus.idle,
            HTCondorJobStatus.running,
            HTCondorJobStatus.transferring_output,
            HTCondorJobStatus.suspended,
        ]:
            return JobStatus.running
        if job_status in [HTCondorJobStatus.removed, HTCondorJobStatus.held, HTCondorJobStatus.failed]:
            return JobStatus.aborted
        raise ValueError(f"Unknown HTCondor Job status: {job_status}")

    def get_job_status(self):
        print(f"Checking job status for task {self.task.task_id}")
        job_status_list = [self.get_job_status_for_id(job_id=job_id) for job_id in self._batch_job_ids]
        if any([s == JobStatus.running for s in job_status_list]):
            return JobStatus.running
        elif any([s == JobStatus.aborted for s in job_status_list]):
            aborted_job_ids = [
                job_id for job_id, status in zip(self._batch_job_ids, job_status_list) if status == JobStatus.aborted
            ]
            aborted_log_files = [(job_id, _batch_job_status_cache[job_id][1]) for job_id in aborted_job_ids]

            log_file_dir = get_log_file_dir(task=self.task)
            os.makedirs(log_file_dir, exist_ok=True)
            with open(os.path.join(log_file_dir, "failed_jobs.log"), "w") as f:
                for job_id, log_file in aborted_log_files:
                    f.write(f"{job_id}: {log_file}\n")

            _batch_job_status_cache.remove_job_ids(job_ids=self._batch_job_ids)
            return JobStatus.aborted
        else:
            _batch_job_status_cache.remove_job_ids(job_ids=self._batch_job_ids)
            return JobStatus.successful

    def start_job(self):
        """
        Starts a job by creating and submitting an HTCondor submit file.

        This method generates an HTCondor submit file using the :obj:`_create_htcondor_submit_file`
        method, then submits the job using the ``condor_submit`` command.

        Raises:
            RuntimeError: If the batch submission fails or the job ID cannot be extracted
                          from the ``condor_submit`` output.
        """
        submit_file = self._create_htcondor_submit_file()

        # Check if the submit file is empty and the task was already terminated
        if self._terminated:
            print("Task was already terminated.")
            return

        # HTCondor submit needs to be called in the folder of the submit file
        submit_file_dir, submit_file = os.path.split(submit_file)
        output = subprocess.check_output(["condor_submit", submit_file], cwd=submit_file_dir)

        output = output.decode()
        match = re.findall(r"[0-9]+\.", output)
        if not match:
            raise RuntimeError("Batch submission failed with output " + output)

        self._batch_job_ids.extend(int(m[:-1]) for m in match)
        _batch_job_status_cache.add_job_ids(self._batch_job_ids)

    def terminate_job(self):
        """
        Terminates a batch job managed by HTCondor.

        This method checks if a batch job ID is available. If a valid job ID exists,
        it executes the ``condor_rm`` command to remove the job from the HTCondor queue.
        """
        if not self._batch_job_ids:
            return

        rm_cmd = ["condor_rm"]
        rm_cmd.extend([str(j) for j in self._batch_job_ids])
        subprocess.run(rm_cmd, stdout=subprocess.DEVNULL)

    @staticmethod
    def _create_submit_file_content(task):
        """
        Creates an HTCondor submit file for the current task.

        This method generates the content of an HTCondor submit file based on the
        task's configuration and writes it to a file.

        Returns:
            str: The path to the generated HTCondor submit file.

        Raises:
            ValueError: If ``transfer_files`` contains non-absolute file paths or if
                        ``working_dir`` is not explicitly set to '.' when using
                        ``transfer_files``.

        Note:
            - The ``stdout`` and ``stderr`` log files are created in the task's log directory. See :obj:`get_log_file_dir`.
            - The HTCondor settings are specified in the ``htcondor_settings`` setting, which is a dictionary of key-value pairs.
            - The executable is created with :obj:`create_executable_wrapper`.
            - The ``transfer_files`` setting can be used to specify files or directories to be transferred to the worker node.
            - The ``job_name`` setting can be used to specify a meaningful name for the job.
            - The submit file is named `job.submit` and is created in the task's output directory (:obj:`get_task_file_dir`).
        """

        submit_file_content = []

        # Specify where to write the log to
        log_file_dir = get_log_file_dir(task)
        os.makedirs(log_file_dir, exist_ok=True)

        stdout_log_file = os.path.abspath(os.path.join(log_file_dir, "stdout"))
        submit_file_content.append(f"output = {stdout_log_file}")

        stderr_log_file = os.path.abspath(os.path.join(log_file_dir, "stderr"))
        submit_file_content.append(f"error = {stderr_log_file}")

        job_log_file = os.path.abspath(os.path.join(log_file_dir, "job.log"))
        submit_file_content.append(f"log = {job_log_file}")

        # Specify the executable
        executable_file = create_executable_wrapper(task)
        submit_file_content.append(f"executable = {os.path.abspath(executable_file)}")

        # Specify additional settings
        general_settings = get_setting("htcondor_settings", dict())
        general_settings.update(get_setting("htcondor_settings", task=task, default=dict()))

        transfer_files = get_setting("transfer_files", task=task, default=[])
        if transfer_files:
            working_dir = get_setting("working_dir", task=task, default="")
            if not working_dir or working_dir != ".":
                raise ValueError("If using transfer_files, the working_dir must be explicitly set to '.'")

            general_settings.setdefault("should_transfer_files", "YES")
            general_settings.setdefault("when_to_transfer_output", "ON_EXIT")

            transfer_files = set(transfer_files)

            for transfer_file in transfer_files:
                if os.path.abspath(transfer_file) != transfer_file:
                    raise ValueError(
                        "You should only give absolute file names in transfer_files!"
                        + f"{os.path.abspath(transfer_file)} != {transfer_file}"
                    )

            env_setup_script = get_setting("env_script", task=task, default="")
            if env_setup_script:
                # TODO: make sure to call it relatively
                transfer_files.add(os.path.abspath(env_setup_script))

            general_settings.setdefault("transfer_input_files", ",".join(transfer_files))

        job_name = get_setting("job_name", task=task, default=False)
        if job_name is not False:
            general_settings.setdefault("JobBatchName", job_name)

        for key, item in general_settings.items():
            submit_file_content.append(f"{key} = {item}")

        # Finally also start the process
        submit_file_content.append("queue 1")

        return submit_file_content

    def _create_htcondor_submit_file(self):
        submit_file_contents = []

        output_path = get_task_file_dir(task=self.task)
        os.makedirs(output_path, exist_ok=True)
        submit_file_path = os.path.join(output_path, "job.submit")

        grouped_params = self.task.grouped_param_names()
        if len(grouped_params) == 0:
            submit_file_contents.extend(self._create_submit_file_content(task=self.task))
        elif not isinstance(self.task.param_kwargs[grouped_params[0]], tuple):
            submit_file_contents.extend(self._create_submit_file_content(task=self.task))
        else:
            len_combinations = len(self.task.param_kwargs[grouped_params[0]])

            grouped_param_dicts = [
                {param: value[i] for param, value in self.task.param_kwargs.items() if param in grouped_params}
                for i in range(len_combinations)
            ]

            for param_dict in grouped_param_dicts:
                sub_task = self.task.clone(None, **param_dict)

                # If a sub_task was already successful do not resubmit it
                if sub_task.complete():
                    continue

                submit_file_content = self._create_submit_file_content(task=sub_task)
                submit_file_contents.extend(submit_file_content)

        if len(submit_file_contents) == 0:
            print("All tasks are already done!")
            self._put_to_result_queue(status=luigi.scheduler.DONE, explanation="")
            self._terminated = True

        with open(submit_file_path, "w") as submit_file:
            submit_file.write("\n".join(submit_file_contents))

        return submit_file_path
