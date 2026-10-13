import enum
import functools
import importlib
import importlib.metadata
import logging
import shutil
import warnings
from typing import Any, Literal, Self

import luigi.interface
import luigi.worker

from b2luigi.batch.processes import BatchProcess
from b2luigi.batch.processes.apptainer import ApptainerProcess
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import create_output_dirs

ENTRY_POINT_GROUP = "b2luigi.batch_systems"


def _plugin_entry_points() -> importlib.metadata.EntryPoints:
    """Read the plugin batch-system entry points. Metadata only — imports nothing."""
    return importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)


class _BatchSystemsMeta(enum.EnumType):
    """
    Meta Enum class, that adds plugin batch systems as enum members when the class is created.
    Batch systems are added as actual enum members, but import is deferred until the process class is actually needed (see :meth:`BatchSystems.load_process_class`).
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: Any,  # an enum._EnumDict at runtime, whose type is private to the enum module
        **kwargs: Any,
    ) -> "_BatchSystemsMeta":
        seen: dict[str, str] = {}
        for ep in _plugin_entry_points():
            dist = ep.dist.name if ep.dist else "an unknown distribution"
            # check for duplicated names
            if ep.name in seen:
                warnings.warn(
                    f"Multiple plugins register the batch system '{ep.name}'; "
                    f"using '{seen[ep.name]}', ignoring '{ep.value}' from '{dist}'.",
                    stacklevel=2,
                )
                continue
            # check for reserved names
            if ep.name in namespace:
                warnings.warn(
                    f"Ignoring plugin batch system '{ep.name}' from '{dist}': "
                    "the name is reserved by a built-in batch system.",
                    stacklevel=2,
                )
                continue
            seen[ep.name] = ep.value
            namespace[ep.name] = (ep.name, ep.value, "plugin")
        return super().__new__(mcs, name, bases, namespace, **kwargs)


class BatchSystems(enum.StrEnum, metaclass=_BatchSystemsMeta):
    """
    All available batch systems.

    There are four built-in batch systems (``lsf``, ``htcondor``, ``slurm``, ``test``)
    and three special ones (``auto``, ``local``, ``custom``).

    Additional batch system can be provided by plugins, which register themselves through the
    ``b2luigi.batch_systems`` entry-point group.
    The entry point's name is the batch system's name, and its value is the import path of the process class (``module:ClassName``).
    These plugin batch systems are discovered lazily and only imported when actually needed (see :meth:`BatchSystems.load_process_class`).

    """

    lsf = ("lsf", "b2luigi.batch.processes.lsf:LSFProcess", "builtin")
    htcondor = ("htcondor", "b2luigi.batch.processes.htcondor:HTCondorProcess", "builtin")
    slurm = ("slurm", "b2luigi.batch.processes.slurm:SlurmProcess", "builtin")
    test = ("test", "b2luigi.batch.processes.test:TestProcess", "builtin")
    auto = ("auto", None, "special")
    local = ("local", None, "special")
    custom = ("custom", None, "special")

    # Declare attributes here for type checkers
    import_path: str | None  # The import path of the process class
    kind: Literal["builtin", "special", "plugin"]

    def __new__(cls, value: str, import_path: str | None, kind: Literal["builtin", "special", "plugin"]) -> Self:
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.import_path = import_path
        obj.kind = kind
        return obj

    def load_process_class(self) -> type[BatchProcess] | None:
        """
        Import this batch system's process class and return it.

        Built-in and plugin systems are resolved lazily here, so that merely listing
        the available batch systems (or importing b2luigi) never imports a plugin's
        dependencies — only actually selecting the system does.

        Returns:
            The :class:`BatchProcess <b2luigi.batch.processes.BatchProcess>`
            subclass for this system, or ``None`` for the special names (``auto``,
            ``local``, ``custom``), which have no process class of their own.
        Raises:
            ImportError: If the process class (or one of its optional
                dependencies) cannot be imported. The original error is attached as
                the cause and typically names the missing optional dependency and
                how to install it.
            TypeError: If the entry point resolves to something that is not a
                ``BatchProcess`` subclass.
            ValueError: If the batch system is not a special name and has no import path.
        """

        if self.kind == "special":
            return None  # special names have no process class
        if self.import_path is None:
            raise ValueError(f"Batch system '{self.value}' has no import path for its process class.")
        module_name, _, class_name = self.import_path.partition(":")
        try:
            module = importlib.import_module(module_name)
            process_class = getattr(module, class_name)
        except ImportError as err:
            raise ImportError(
                f"Batch system '{self.value}' is provided by '{self.import_path}' but could not be imported: {err}"
            ) from err
        if not (isinstance(process_class, type) and issubclass(process_class, BatchProcess)):
            raise TypeError(
                f"Batch system '{self.value}' resolved to {process_class!r}, which is not a BatchProcess subclass."
            )
        return process_class


#: Batch systems whose process classes expand a grouped task into one job per group element.
GROUPING_CAPABLE_BATCH_SYSTEMS = (BatchSystems.htcondor, BatchSystems.slurm, BatchSystems.lsf)

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

        The detected batch system determines the process class used for the task.
        Batch systems can be built-in, or provided via plugins.

        Args:
            task: The task for which the process is to be created.

        Returns:
            An instance of the appropriate process class for the given task.

        Raises:
            RuntimeError: If grouping is requested for a batch system that cannot
                expand a group.
            AttributeError: If the batch system is ``custom`` but the task defines
                no ``process_class`` attribute.
            NotImplementedError: If the detected batch system has no process class,
                which none of the supported batch systems has.
        """
        batch_system = self.detect_batch_system(task)

        # Only the grouping capable batch systems know how to expand a group again. Tasks
        # going to a batch system in UNGROUPED_BATCH_SYSTEMS are never grouped in the first
        # place (see b2luigi.Task.batchable), so they cannot arrive here carrying a group.
        if task.is_grouped() and batch_system not in GROUPING_CAPABLE_BATCH_SYSTEMS:
            raise RuntimeError(
                "The grouping of tasks is currently only implemented for HTCondor, Slurm and LSF "
                f"processes and not for {batch_system.value}!"
            )

        if task.has_grouped_params():
            if batch_system in GROUPING_CAPABLE_BATCH_SYSTEMS:
                logging.warning(
                    "Grouping of tasks is currently an experimental feature and should be treated with care!"
                )
            elif batch_system in UNGROUPED_BATCH_SYSTEMS:
                _warn_grouping_skipped(task.get_task_family(), batch_system)

        if batch_system == BatchSystems.custom:
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

        else:  # else load the process class from built-in or plugin batch system, undefined batch systems are already caught by the BatchSystems enum
            process_class = batch_system.load_process_class()
            if process_class is None:
                # auto, local and custom are handled above, so every batch system
                # reaching this branch has a process class.
                raise NotImplementedError(f"Batch system '{batch_system}' has no process class.")

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
