import enum
import functools
import shutil
import logging

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
    custom = "custom"


#: Batch systems whose process classes expand a grouped task into one job per group element.
GROUPING_CAPABLE_BATCH_SYSTEMS = (BatchSystems.htcondor, BatchSystems.slurm)

#: Batch systems for which grouping is silently skipped instead of being an error.
#: Their tasks simply run one by one, exactly as if no parameter had ``grouping=True``.
UNGROUPED_BATCH_SYSTEMS = (BatchSystems.local,)


def detect_batch_system(task) -> BatchSystems:
    """
    Detects the batch system to be used for task execution.

    This function determines the batch system setting based on the provided task
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
    batch_system_setting = get_setting("batch_system", default="auto", task=task)
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


def supports_grouping(task) -> bool:
    """
    Check whether the task may be grouped, i.e. whether whoever executes it is able to
    expand a group back into its individual tasks again.

    Grouping is built on top of the ``luigi`` batching mechanism, which *combines* the
    parameter values of several tasks into a single task and leaves it to that task to
    deal with the combined value. Only the grouping capable batch systems undo this
    packing, by submitting one job per group element. Everywhere else a group would
    reach an unsuspecting ``run()`` as a tuple, so the group must not be formed in the
    first place. See :meth:`b2luigi.Task.batchable`.

    Args:
        task: The task that is about to be scheduled.

    Returns:
        bool: ``False`` if grouping has to be skipped for this task.
    """
    # In test mode every task is run in-process by a plain luigi worker, which knows
    # nothing about batch systems and therefore cannot expand a group either.
    if get_setting("_dispatch_local_execution", default=False):
        return False

    return detect_batch_system(task) not in UNGROUPED_BATCH_SYSTEMS


@functools.cache
def _warn_grouping_skipped(task_family: str, batch_system: BatchSystems) -> None:
    """Warn once per task family that its grouped parameters are being ignored."""
    logging.warning(
        f"The task {task_family} has grouped parameters, but grouping is not implemented for the "
        f"{batch_system.value} batch system. The tasks are run individually instead."
    )


class SendJobWorker(luigi.worker.Worker):
    """
    A custom ``luigi`` worker that determines the appropriate batch system for a task
    and creates a task process accordingly.
    """

    def detect_batch_system(self, task):
        """
        Detects the batch system to be used for task execution.

        Thin wrapper around :obj:`detect_batch_system`, see there.

        Args:
            task: The task for which the batch system is being determined.

        Returns:
            BatchSystems: An instance of the :obj:`BatchSystems` enumeration representing
            the detected or configured batch system.
        """
        return detect_batch_system(task)

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

        # Only the grouping capable batch systems know how to expand a group again. Tasks
        # going to a batch system in UNGROUPED_BATCH_SYSTEMS are never grouped in the first
        # place (see b2luigi.Task.batchable), so they cannot arrive here carrying a group.
        if task.is_grouped() and batch_system not in GROUPING_CAPABLE_BATCH_SYSTEMS:
            raise RuntimeError(
                "The grouping of tasks is currently only implemented for HTCondor and Slurm processes "
                f"and not for {batch_system.value}!"
            )

        if task.has_grouped_params():
            if batch_system in GROUPING_CAPABLE_BATCH_SYSTEMS:
                logging.warning(
                    "Grouping of tasks is currently an experimental feature and should be treated with care!"
                )
            elif batch_system in UNGROUPED_BATCH_SYSTEMS:
                _warn_grouping_skipped(task.get_task_family(), batch_system)

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
        elif batch_system == BatchSystems.custom:
            if not hasattr(task, "process_class"):
                raise AttributeError(
                    "The task object does not have a 'process_class' attribute. Please ensure the task defines this attribute."
                )
            process_class = task.process_class
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
