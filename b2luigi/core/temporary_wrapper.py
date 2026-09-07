from contextlib import ExitStack
from functools import wraps
from multiprocessing.pool import ThreadPool
from typing import Callable, Optional
import tempfile
import os

from b2luigi import Task
from b2luigi.core import utils
from b2luigi.core.settings import get_setting


class TemporaryFileContextManager(ExitStack):
    """
    A context manager for managing temporary file paths for a given task. This class overrides
    the task's methods for retrieving input and output file names, ensuring that temporary file
    paths are used during the context's lifetime. Upon exiting the context, the original methods
    are restored.

    :param task: The task whose file-name methods are redirected while the context is active.
    :param inputs: When ``True`` (default), the input-side methods are overridden so every input is
        copied into ``scratch_dir`` on first access. When ``False``, inputs are left untouched and read
        from their real location.
    :param outputs: When ``True`` (default), :meth:`b2luigi.Task.get_output_file_name` returns a temporary
        path that is moved to the final location only when the context exits without an exception.
        When ``False``, outputs are written directly to their final location.

    Usage:
        This class is not meant to be used directly. Instead, it is used within the :obj:`on_temporary_files`
    """

    def __init__(self, task: Task, inputs: bool = True, outputs: bool = True):
        super().__init__()

        self._task = task
        self._inputs = inputs
        self._outputs = outputs
        self._task_output_function = task.get_output_file_name
        self._task_input_function = task.get_input_file_names
        self._task_all_input_function = task.get_all_input_file_names
        self._task_input_function_from_dict = task.get_input_file_names_from_dict

        self._open_output_files = {}
        self._open_input_files = {}

    def __enter__(self):
        """
        This method is called when entering the context manager. It overrides the task's methods
        for retrieving input and output file names, replacing them with custom implementations
        that handle temporary file paths.

        **Redefinitions**:

        - ``get_output_file_name``

          Retrieves or creates a temporary output file for the given key.

          If the file corresponding to the specified key is not already open, this method
          generates a temporary file path using the task's :meth:`b2luigi.Task._get_output_target` and
          :meth:`b2luigi.Target.temporary_path` methods, and opens the file within the context of the
          task.

          Args:
              key (str): The unique identifier for the output file.

              tmp_file_kwargs: Additional keyword arguments to be passed to the :meth:`b2luigi.Target.temporary_path` method.

          Returns:
              The opened temporary file associated with the given key.

        - ``get_input_file_names``

          Retrieves the input file names associated with a given key, ensuring that the files are
          temporarily available for processing. Behaves the same as :meth:`b2luigi.Task.get_input_file_names`.

          Args:
              key (str): The identifier for the input files to retrieve.

              tmp_file_kwargs: Additional keyword arguments to pass to the :meth:`b2luigi.Target.get_temporary_input` method of the target.

          Returns:
              list: A list of opened temporary input files corresponding to the given key.

          Notes:
              - If the ``n_download_threads`` setting is specified, the input files are fetched
                concurrently using a thread pool.
              - If ``n_download_threads`` is not specified, the input files are fetched sequentially.

        - ``get_all_input_file_names``
            Retrieves all input file names from the task's input, ensuring that all files are temporarily available for processing.
            Behaves the same as :meth:`b2luigi.Task.get_all_input_file_names`.

            Args:
                tmp_file_kwargs: Additional keyword arguments to pass to the :meth:`b2luigi.Target.get_temporary_input` method of the target.

            Returns:
                list: A list of all opened temporary input files from the task's input.

            Notes:
              - If the ``n_download_threads`` setting is specified, the input files are fetched concurrently
                using a thread pool.
              - If ``n_download_threads`` is not specified, the input files are fetched sequentially.

        - ``get_input_file_names_from_dict``

          Retrieves input file names from a dictionary structure, handling temporary file paths.
          Behaves the same as :meth:`b2luigi.Task.get_input_file_names_from_dict`.

          Args:
              requirement_key (str): The key used to identify the required input in the task's input dictionary.

              key (Optional[str], optional): A specific key to extract targets from the target dictionary. If ``None``, all targets are retrieved. Defaults to ``None``.

              tmp_file_kwargs: Additional keyword arguments passed to the :meth:`b2luigi.Target.get_temporary_input` method for generating temporary file paths.

          Returns:
              list: A list of temporary file paths corresponding to the input files.

          Raises:
              KeyError: If the specified `key` is not found in the target dictionary.
        """

        def get_output_file_name(key: str, **tmp_file_kwargs):
            if key not in self._open_output_files:
                target = self._task._get_output_target(key)
                temporary_path = target.temporary_path(task=self._task, **tmp_file_kwargs)
                self._open_output_files[key] = self.enter_context(temporary_path)

            return self._open_output_files[key]

        if self._outputs:
            self._task.get_output_file_name = get_output_file_name

        if not self._inputs:
            return self

        def get_input_file_names(key: str, **tmp_file_kwargs):
            """ """
            if key not in self._open_input_files:
                targets = self._task._get_input_targets(key)
                self._open_input_files[key] = []
                n_download_threads = get_setting("n_download_threads", default=2, task=self._task)
                if n_download_threads is not None:
                    with ThreadPool(n_download_threads) as pool:
                        self._open_input_files[key] = pool.map(
                            lambda target: self.enter_context(
                                target.get_temporary_input(task=self._task, **tmp_file_kwargs)
                            ),
                            targets,
                        )
                else:
                    for target in targets:  # TODO: This does not work in wrapping tasks
                        temporary_path = target.get_temporary_input(task=self._task, **tmp_file_kwargs)
                        self._open_input_files[key].append(self.enter_context(temporary_path))

            return self._open_input_files[key]

        self._task.get_input_file_names = get_input_file_names

        def get_all_input_file_names(**tmp_file_kwargs):
            input_dict = utils.flatten_to_dict_of_lists(self._task.input())
            n_download_threads = get_setting("n_download_threads", default=2, task=self._task)
            for key, value in input_dict.items():
                if key not in self._open_input_files:
                    self._open_input_files[key] = []
                    if n_download_threads is not None:
                        with ThreadPool(n_download_threads) as pool:
                            self._open_input_files[key] = pool.map(
                                lambda target: self.enter_context(
                                    target.get_temporary_input(task=self._task, **tmp_file_kwargs)
                                ),
                                value,
                            )
                    else:
                        for target in value:
                            temporary_path = target.get_temporary_input(task=self._task, **tmp_file_kwargs)
                            self._open_input_files[key].append(self.enter_context(temporary_path))

            return list(self._open_input_files.values())

        self._task.get_all_input_file_names = get_all_input_file_names

        def get_input_file_names_from_dict(requirement_key: str, key: Optional[str] = None, **tmp_file_kwargs):
            internal_key = f"{requirement_key}_{str(key)}"
            if internal_key not in self._open_input_files:
                # Expected output of task.input is {requirement_key: [generators, ...]}
                target_generator_dict = utils.flatten_to_dict_of_lists(self._task.input()[requirement_key])
                # Now, the output is structured as {key: [target1, target2, ...]}

                self._open_input_files[internal_key] = []

                if key is not None and key in target_generator_dict.keys():
                    targets = target_generator_dict[key]

                # If key is None, we want to get all targets from the dictionary
                else:
                    targets = [item for sublist in target_generator_dict.values() for item in sublist]

                for target in targets:
                    temporary_path = target.get_temporary_input(task=self._task, **tmp_file_kwargs)
                    self._open_input_files[internal_key].append(self.enter_context(temporary_path))

            return self._open_input_files[internal_key]

        self._task.get_input_file_names_from_dict = get_input_file_names_from_dict
        return self

    def __exit__(self, *exc_details):
        super().__exit__(*exc_details)

        self._task.get_output_file_name = self._task_output_function
        self._task.get_input_file_names = self._task_input_function
        self._task.get_all_input_file_names = self._task_all_input_function
        self._task.get_input_file_names_from_dict = self._task_input_function_from_dict


def on_temporary_files(run_function: Callable | None = None, *, inputs: bool = True, outputs: bool = True):
    """
    Wrapper for decorating a task's run function to use temporary files for its inputs and outputs.

    A common problem when using long running tasks in ``luigi`` is the so called thanksgiving bug
    (see https://www.arashrouhani.com/luigi-budapest-bi-oct-2015/#/21).
    It occurs, when you define an output of a task and in its run function,
    you create this output before filling it with content
    (maybe even only after a long lasting calculation).
    It may happen, that during the creation of the output and the finish of the calculation
    some other tasks checks if the output is already there, finds it and assumes,
    that the task is already finished (although there is probably only non-sense in the file
    so far).

    A solution is already given by luigi itself, when using the ``temporary_path()`` function
    of the file system targets, which is really nice!
    Unfortunately, this means you have to open all your output files with a context manager
    and this is very hard to do if you have external tasks also (because they will
    probably use the output file directly instead of the temporary file version of if).

    This wrapper simplifies the usage of the temporary files:

    .. code-block:: python

      import b2luigi

      class MyTask(b2luigi.Task):
          def output(self):
              yield self.add_to_output("test.txt")

          @b2luigi.on_temporary_files
          def run(self):
              with open(self.get_output_file_name("test.txt"), "w") as f:
                  raise ValueError()
                  f.write("Test")

    Instead of creating the file "test.txt" at the beginning and filling it with content
    later (which will never happen because of the exception thrown, which makes the file
    existing but the task actually not finished), the file will be written to a temporary
    file first and copied to its final location at the end of the run function (but only if there
    was no error).

    Inputs are handled the same way: by default every file returned by :meth:`b2luigi.Task.get_input_file_names`,
    :meth:`b2luigi.Task.get_all_input_file_names` and :meth:`b2luigi.Task.get_input_file_names_from_dict` is copied
    into the ``scratch_dir`` first, so a task always works on a local copy (which is what makes remote targets usable
    transparently).

    Both halves can be switched off independently with keyword arguments. A task that reads a large number of
    input files and produces only a small output usually does not want its inputs duplicated into ``scratch_dir``:

    .. code-block:: python

      class MergeTask(b2luigi.Task):
          @b2luigi.on_temporary_files(inputs=False)
          def run(self):
              # inputs are read from their real location, the output is still staged
              ...

    :param run_function: The ``run`` method being decorated. Supplied automatically when the decorator is
        used without parentheses.
    :param inputs: Copy every input file into ``scratch_dir`` before handing its path to the task.
        Defaults to ``True``. Set to ``False`` to read inputs in place.
    :param outputs: Write every output to a temporary path and move it to the final location only when
        ``run`` finishes without an exception. Defaults to ``True``. Set to ``False`` to write outputs directly.
    :raises TypeError: If the first positional argument is not the function being decorated. The flags are
        keyword-only, so ``@on_temporary_files(False)`` is rejected instead of silently doing something else.

    Warning:
        The decorator only edits the file-name methods listed above. If you are using
        the targets directly, you have to take care of using the temporary path correctly by yourself!

    """

    def decorate(function: Callable) -> Callable:
        @wraps(function)
        def run(self, *args, **kwargs):
            with TemporaryFileContextManager(self, inputs=inputs, outputs=outputs):
                function(self, *args, **kwargs)

        return run

    if run_function is None:
        return decorate
    if not callable(run_function):
        raise TypeError(
            "on_temporary_files() takes the decorated function as its only positional argument; "
            "pass the flags as keywords, e.g. @on_temporary_files(inputs=False)"
        )
    return decorate(run_function)


class EnsuredTemporaryScratchDirectory(tempfile.TemporaryDirectory):
    """
    A TemporaryDirectory that ensures the parent directory exists.

    This subclass behaves like `tempfile.TemporaryDirectory`, but if the `dir` argument is provided,
    the directory is created automatically (using `os.makedirs(..., exist_ok=True)`) before the temporary
    directory is created.
    """

    def __init__(self, dir=None, **tmp_file_kwargs):
        try:
            if dir is not None:
                os.makedirs(name=dir, exist_ok=True)
            super().__init__(dir=dir, **tmp_file_kwargs)
        except PermissionError:
            raise PermissionError(
                f"You do not have the permission to write to the temporary directory {dir}! Please set a different 'scratch_dir' using 'b2luigi.set_setting('scratch_dir', <scratch_dir>)'."
            )
