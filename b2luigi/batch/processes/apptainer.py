import os
import subprocess

from b2luigi.batch.processes import BatchProcess, JobStatus
from b2luigi.core.utils import create_cmd_from_task, create_apptainer_command, get_log_file_dir


class ApptainerProcess(BatchProcess):
    """
    Simple implementation of a batch process for running jobs in an Apptainer container. Strictly speaking,
    this is not a batch process, but it is a simple way to run jobs in a container environment.

    This process inherits the basic properties from :class:`b2luigi.batch.processes.BatchProcess` but does not
    need to be executed in the ``batch`` context. However, running in ``batch`` mode is possible for the
    ``lsf`` and the ``htcondor`` batch systems. Although, for the latter batch system it is not recommended
    to use apptainer images since HTCondor is already running in a container environment.

    The core principle of this process is to run the task in an Apptainer container. To achieve the execution of
    tasks, an ``apptainer exec`` command is build within this class and executed in a subprocess. To steer the
    execution, one can use the following settings:

    * ``apptainer_image``: The image to use for the Apptainer container.s
        This parameter is mandatory and needs to be set if the task should be executed in an Apptainer container.
        The image needs to be accessible from the machine where the task is executed. There are no further checks
        if the image is available or valid. When using custom images, it may be helpful to first check the image
        with ``apptainer inspect``. For people with access to the Belle II own ``/cvmfs`` directory, images are
        provided in the ``/cvmfs/belle.cern.ch/images`` directory. The description of the images (the repository
        contains the docker images which are transformed to Apptainer images) and instructions on how to create them
        can be found in https://gitlab.desy.de/belle2/software/docker-images.

    * ``apptainer_mounts``: A list of directories to mount into the Apptainer container.
        This parameter is optional and can be used to mount directories into the Apptainer container. The directories
        need to be accessible from the machine where the task is executed. The directories are mounted under the exact
        same path as they are provided/on the host machine. For most usecases mounts need to be provided to access software
        or data locations. For people using for example ``basf2`` software in the Apptainer container, the ``/cvmfs``
        directory needs to be mounted. Caution is required when system specific directories are mounted.

    * ``apptainer_mount_defaults``: Boolean parameter to mount ``log_dir`` and ``result_dir`` by default.
        The default value is ``True`` meaning the ``result_dir`` and ``log_dir`` are automatically created and mounted if
        they are not accessible from the execution location. When using custom targets with non local output directories,
        this parameter should be set to ``False`` to avoid mounting non-existing directories.

    * ``apptainer_additional_params``: Additional parameters to pass to the ``apptainer exec`` command.
        This parameter should be a string and will be directly appended to the ``apptainer exec`` command. It can be used to
        pass additional parameters to the ``apptainer exec`` command as they would be added in the CLI. A very
        useful parameter is the ``--cleanenv`` parameter which will clean the environment before executing the task in the
        Apptainer container. This can be useful to avoid conflicts with the environment in the container.
        A prominent usecase is the usage of software which depends on the operating system.


    A simple example of how an Apptainer based task can be defined is shown below:

    .. code-block:: python

        class MyApptainerTask(luigi.Task):
            apptainer_image = "/cvmfs/belle.cern.ch/images/belle2-base-el9"
            apptainer_mounts = ["/cvmfs"]
            apptainer_mount_defaults = True
            apptainer_additional_params = "--cleanenv"

            <rest of the task definition>
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._process = None
        self._sdtout = None
        self._stderr = None

    def get_job_status(self):
        """
        Determine the current status of the job associated with this process.

        Returns:
            JobStatus: The current status of the job. Possible values are:
                - :meth:`JobStatus.aborted <b2luigi.process.JobStatus.aborted>`: If the process is not initialized or has a non-zero return code.
                - :meth:`JobStatus.running <b2luigi.process.JobStatus.running>`: If the process is still running.
                - :meth:`JobStatus.running <b2luigi.process.JobStatus.running>`: If the process has finished successfully (return code is 0).
        """
        if self._process is None:
            return JobStatus.aborted

        # Poll the process to check if it is still running
        if self._process.poll() is None:
            return JobStatus.running
        else:
            # If the process has finished, write output and return the appropriate status
            self._stdout, self._stderr = self._process.communicate()
            self._write_output()
            return JobStatus.successful if self._process.returncode == 0 else JobStatus.aborted

    def start_job(self):
        """
        Starts a job by constructing and executing an Apptainer command.

        This method generates the necessary command to execute a task using
        Apptainer, starts the job as a subprocess, and captures the process
        information for further management.

        The command is constructed by combining the task-specific command (see :meth:`create_cmd_from_task`)
        and the Apptainer execution wrapper (see :obj:`create_apptainer_command`).

        Attributes:
            self.task: The task containing the details required to
                       construct the command.
        """
        command = " ".join(create_cmd_from_task(self.task))
        exec_command = create_apptainer_command(command, task=self.task)

        # Start the job and capture the job ID
        self._process = subprocess.Popen(exec_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def terminate_job(self):
        """
        Terminates the currently running process if it exists.

        This method checks if a process is associated with the instance
        and terminates it by calling the `terminate` method on the process.
        """
        if self._process is not None:
            self._process.terminate()

    def _write_output(self):
        """
        Writes the captured standard output and standard error of the task to
        separate log files in the task's log directory (see :obj:`get_log_file_dir`).

        The method creates two files, "stdout" and "stderr", in the log directory
        obtained from the task. If the standard output or standard error is not
        None, their contents are decoded and written to the respective files.
        """
        log_file_dir = get_log_file_dir(self.task)

        with open(os.path.join(log_file_dir, "stdout"), "w") as f:
            if self._stdout is not None:
                f.write(self._stdout.decode())

        with open(os.path.join(log_file_dir, "stderr"), "w") as f:
            if self._stderr is not None:
                f.write(self._stderr.decode())
