import os
import shutil

import b2luigi
from b2luigi.basf2_helper.targets import ROOTLocalTarget

import subprocess

from b2luigi.basf2_helper.utils import get_basf2_git_hash
from b2luigi.core.utils import (
    create_output_dirs,
    get_serialized_parameters,
    flatten_to_dict_of_lists,
)


class Basf2Task(b2luigi.DispatchableTask):
    """
    A custom task class for handling ``basf2``-related tasks in the ``b2luigi`` framework. This class extends
    :class:`b2luigi.DispatchableTask` and provides additional functionality for managing output file targets
    and serialized parameters.
    """

    #: A parameter representing the ``basf2`` git hash. It is set to the
    #: current ``basf2`` git hash (see: :meth:`get_basf2_git_hash <b2luigi.basf2_helper.utils.get_basf2_git_hash>`) by default and marked as non-significant to avoid affecting the
    #: task's unique ID.
    git_hash = b2luigi.Parameter(default=get_basf2_git_hash(), significant=False)

    def get_output_file_target(self, *args, **kwargs):
        """
        Determines the appropriate output file target based on the file extension.

        If the output file has a ".root" extension, it returns a :class:`b2luigi.basf2_helper.ROOTLocalTarget`
        for the specified file. Otherwise, it delegates to the superclass
        implementation of :meth:`b2luigi.Task.get_output_file_target`.

        Args:
            *args: Positional arguments passed to ``get_output_file_name`` and the
                   superclass method.
            **kwargs: Keyword arguments passed to ``get_output_file_name`` and the
                      superclass method.

        Returns:
            Target: A :class:`b2luigi.basf2_helper.ROOTLocalTarget`  if the file extension is ".root", otherwise
                    the result of the superclass's ``get_output_file_target`` method.
        """
        file_name = self.get_output_file_name(*args, **kwargs)
        if os.path.splitext(file_name)[-1] == ".root":
            return ROOTLocalTarget(file_name)
        return super().get_output_file_target(*args, **kwargs)

    def get_serialized_parameters(self):
        """
        Retrieve the serialized parameters of the current task. (see :meth:`get_serialized_parameters <b2luigi.core.utils.get_serialized_parameters>`)

        Returns:
            dict: A dictionary containing the serialized parameters of the task.
        """
        return get_serialized_parameters(self)


class Basf2PathTask(Basf2Task):
    """
    A task for running a ``basf2`` processing path within the ``b2luigi`` framework.

    In contrast to the normal ``(b2)luigi`` tasks, the execution logic of a
    :class:`Basf2PathTask <b2luigi.basf2_helper.tasks.Basf2PathTask>` is not defined in
    a ``run`` method but in
    :meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>`.
    The :meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>` method needs
    to return the ``basf2`` path that is created in the steering file.
    Furthermore, the ``Progress`` module is automatically added and ``print(b2.statistics)``
    is called after the path is processed.

    .. warning::
        Due to technical reasons, the path needs to be created within the :meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>`
        method. The path can be used in further objects, however, it is not possible
        for it to originate from an outer scope.
    """

    num_processes = b2luigi.IntParameter(significant=False, default=0)
    max_event = b2luigi.IntParameter(significant=False, default=0)

    def create_path(self):
        raise NotImplementedError()

    @b2luigi.on_temporary_files
    def process(self):
        """
        Executes the processing task using the ``basf2`` framework.

        It sets the number of processes for basf2 if
        ``self.num_processes`` is specified. Then, it creates a processing path,
        adds the ``basf2.Progress`` module to the path, and prints the path configuration.

        Finally, it processes the path with the specified maximum number of events
        (``self.max_event``). If ``self.max_event`` is not set, it defaults to ``0``
        (process all events). After processing, it prints the ``basf2`` statistics.

        Raises:
            ImportError: If the basf2 module cannot be found.
        """
        try:
            import basf2
        except ImportError:
            raise ImportError("Can not find basf2. Can not use the basf2 task.")

        if self.num_processes:
            basf2.set_nprocesses(self.num_processes)

        path = self.create_path()

        path.add_module("Progress")
        basf2.print_path(path)
        max_event = self.max_event if self.max_event else 0
        basf2.process(path=path, max_event=max_event)

        print(basf2.statistics)


class SimplifiedOutputBasf2Task(Basf2PathTask):
    """
    A task that simplifies the handling of output files in a ``basf2`` processing path.

    This class is intended to be subclassed (see :obj:`Basf2PathTask`), and the ``create_path`` method must be
    implemented to define the ``basf2`` processing path. The ``output`` method identifies
    and collects output file targets from the modules in the path.
    """

    def create_path(self):
        raise NotImplementedError()

    def output(self):
        """
        Generates the output targets for the task by inspecting the modules in the created path.

        Returns:
            list: A list of ``ROOTLocalTarget`` objects corresponding to the output file names
                  specified in the "RootOutput" modules of the path.
        """
        path = self.create_path()
        outputs = []

        for module in path.modules():
            if module.type() == "RootOutput":
                for param in module.available_params():
                    if param.name == "outputFileName":
                        outputs.append(ROOTLocalTarget(param.values))

        return outputs


class MergerTask(Basf2Task):
    """
    A task class for merging input files using a specified command.
    """

    cmd = []
    keys = []

    @property
    def input_keys(self):
        all_input_keys = flatten_to_dict_of_lists(self.input()).keys()
        for key in all_input_keys:
            # Filter out keys not in self.keys
            if hasattr(self, "keys") and key not in self.keys:
                continue

            yield key

    def output(self):
        """
        Generates the output for the task by iterating over input file names and
        applying filters based on the ``keys`` attribute.

        Yields:
            The result of :meth:`b2luigi.Task.add_to_output` for each key that passes the filtering
            conditions.

        Filtering Conditions:
            - If the task has a ``keys`` attribute, only keys present in ``self.keys``
              are processed.
        """
        for key in self.input_keys:
            if hasattr(self, "keys") and key not in self.keys:
                continue

            yield self.add_to_output(key)

    @b2luigi.on_temporary_files
    def process(self):
        """
        Processes input files and generates output files by executing a command.

        1. Creates necessary output directories using :meth:`create_output_dirs <b2luigi.core.utils.create_output_dirs>`.
        2. Iterates over the input file names grouped by keys.
        3. Processing for only the keys specified in ``self.keys``.
        4. Constructs a command by appending the output file name and input file list to ``self.cmd``.
        5. Executes the constructed command using ``subprocess.check_call``.
        """
        create_output_dirs(self)

        for key in self.input_keys:
            if not isinstance(self.input(), dict):
                file_list = self.get_input_file_names(key)
            else:
                raise ValueError(
                    f"Input is of type {type(self.input())}. "
                    + "The behaviour of a merger Task with a dict input is not well defined. "
                )
            args = self.cmd + [self.get_output_file_name(key)] + file_list
            subprocess.check_call(args)


class HaddTask(MergerTask):
    """
    :obj:`HaddTask` is a subclass of :obj:`MergerTask` that represents a task for merging ROOT files
    using the ``hadd`` command-line tool.
    """

    cmd = ["hadd", "-f"]


class Basf2FileMergeTask(MergerTask):
    """
    :obj:`Basf2FileMergeTask` is a subclass of :obj:`MergerTask` that represents a task for merging ``basf2`` ROOT files
    using the ``b2file-merge`` command-line tool.
    """

    cmd = ["b2file-merge", "-f"]


class Basf2nTupleMergeTask(MergerTask):
    @property
    def cmd(self):
        "Command to use to merge basf2 tuple files."
        # ``fei_merge_files`` has been renamed to ``analysis-fei-mergefiles``, use
        # the newer command if it exists in the release.
        new_cmd_name = "analysis-fei-mergefiles"
        old_cmd_name = "fei_merge_files"
        if shutil.which(new_cmd_name):
            return [new_cmd_name]
        return [old_cmd_name]
