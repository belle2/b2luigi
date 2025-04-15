import enum
import shutil

import luigi.interface
import luigi.worker

from b2luigi.batch.processes.lsf import LSFProcess
from b2luigi.batch.processes.htcondor import HTCondorProcess
from b2luigi.batch.processes.slurm import SlurmProcess
from b2luigi.batch.processes.gbasf2 import Gbasf2Process
from b2luigi.batch.processes.apptainer import ApptainerProcess
from b2luigi.batch.processes.test import TestProcess
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import create_output_dirs


class BatchSystems(enum.Enum):
    """
    An enumeration representing different batch systems.
    """

    lsf = "lsf"
    htcondor = "htcondor"
    slurm = "slurm"
    gbasf2 = "gbasf2"
    local = "local"
    test = "test"


class SendJobWorker(luigi.worker.Worker):
    """
    A custom ``luigi`` worker that determines the appropriate batch system for a task
    and creates a task process accordingly.
    """

    def detect_batch_system(self, task):
        """
        Detects the batch system to be used for task execution.

        This method determines the batch system setting based on the provided task
        or automatically detects the available batch system on the system if the
        setting is ``auto``. The detection checks for the presence of specific
        commands associated with known batch systems (e.g., ``bsub`` for LSF,
        ``condor_submit`` for HTCondor, ``sbatch`` for SLURM). If no known batch system
        is detected, it defaults to ``local``.

        Args:
            task: The task for which the batch system is being determined.

        Returns:
            BatchSystems: An instance of the :obj:`BatchSystems` enumeration representing
            the detected or configured batch system.
        """
        batch_system_setting = get_setting("batch_system", default=BatchSystems.lsf, task=task)
        if batch_system_setting == "auto":
            if shutil.which("bsub"):
                batch_system_setting = "lsf"
            elif shutil.which("condor_submit"):
                batch_system_setting = "htcondor"
            elif shutil.which("sbatch"):
                batch_system_setting = "slurm"
            else:
                batch_system_setting = "local"

        return BatchSystems(batch_system_setting)

    def _create_task_process(self, task):
        """
        Creates and returns a process instance for the given task based on the detected batch system.

        This method determines the appropriate process class to use for the task by detecting the batch
        system associated with it. Depending on the batch system, it initializes and returns an instance
        of the corresponding process class. If the batch system is not supported, a ``NotImplementedError``
        is raised.

        Args:
            task: The task for which the process is to be created.

        Returns:
            An instance of the appropriate process class for the given task.

        Raises:
            NotImplementedError: If the batch system is not recognized or supported.
        """
        batch_system = self.detect_batch_system(task)
        if batch_system == BatchSystems.lsf:
            process_class = LSFProcess
        elif batch_system == BatchSystems.htcondor:
            process_class = HTCondorProcess
        elif batch_system == BatchSystems.slurm:
            process_class = SlurmProcess
        elif batch_system == BatchSystems.gbasf2:
            process_class = Gbasf2Process
        elif batch_system == BatchSystems.test:
            process_class = TestProcess
        elif batch_system == BatchSystems.local:
            if get_setting("apptainer_image", default="", task=task):
                process_class = ApptainerProcess
            else:
                create_output_dirs(task)
                return super()._create_task_process(task)
        else:
            raise NotImplementedError

        return process_class(
            task=task,
            scheduler=self._scheduler,
            result_queue=self._task_result_queue,
            worker_timeout=self._config.timeout,
        )


class SendJobWorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):
    """
    A factory class for creating instances of :obj:`SendJobWorker`.

    This class extends ``luigi.interface._WorkerSchedulerFactory`` and overrides the
    :obj:`create_worker` method to return a :obj:`SendJobWorker` instance.

    Args:
        scheduler: The scheduler instance to be used by the worker.
        worker_processes (int): The number of worker processes to be used.
        assistant (bool, optional): Indicates whether the worker is in assistant mode.
            Defaults to False.
    """

    def create_worker(self, scheduler, worker_processes, assistant=False):
        """
        Creates and returns an instance of  :obj:`SendJobWorker`.

        Args:
            scheduler: The scheduler instance to be used by the worker.
            worker_processes (int): The number of worker processes to be used.
            assistant (bool, optional): Indicates whether the worker should act as an assistant. Defaults to False.

        Returns:
            SendJobWorker: An instance of the :obj:`SendJobWorker` class configured with the provided parameters.
        """
        return SendJobWorker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)
