from contextlib import ExitStack
from functools import wraps
from multiprocessing.pool import ThreadPool

import b2luigi


class TemporaryFileContextManager(ExitStack):
    def __init__(self, task: b2luigi.Task):
        super().__init__()

        self._task = task
        self._task_output_function = task.get_output_file_name
        self._task_input_function = task.get_input_file_names
        self._task_input_function_from_dict = task.get_input_file_names_from_dict

        self._open_output_files = {}
        self._open_input_files = {}

    def __enter__(self):
        def get_output_file_name(key: str):
            if key not in self._open_output_files:
                target = self._task._get_output_target(key)
                temporary_path = target.temporary_path()
                self._open_output_files[key] = self.enter_context(temporary_path)

            return self._open_output_files[key]

        self._task.get_output_file_name = get_output_file_name

        def get_input_file_names(key: str):
            if key not in self._open_input_files:
                targets = self._task._get_input_targets(key)
                self._open_input_files[key] = []
                if hasattr(self._task, "n_download_threads"):
                    with ThreadPool(self._task.n_download_threads) as pool:
                        self._open_input_files[key] = pool.map(
                            lambda target: self.enter_context(target.get_temporary_input()),
                            targets,
                        )
                else:
                    for target in targets:  # TODO: This does not work in wrapping tasks
                        temporary_path = target.get_temporary_input()
                        self._open_input_files[key].append(self.enter_context(temporary_path))

            return self._open_input_files[key]

        self._task.get_input_file_names = get_input_file_names

        def get_input_file_names_from_dict(requirement_key: str, key: str):
            internal_key = f"{requirement_key}_{key}"
            if internal_key not in self._open_input_files:
                target_dicts = self._task.input()[requirement_key]

                self._open_input_files[internal_key] = []
                for target_dict in target_dicts:
                    if key in target_dict.keys():
                        target = target_dict[key]
                        temporary_path = target.get_temporary_input()
                        self._open_files[internal_key].append(self.enter_context(temporary_path))

            return self._open_files[internal_key]

        self._task.get_input_file_names_from_dict = get_input_file_names_from_dict

    def __exit__(self, *exc_details):
        super().__exit__(*exc_details)

        self._task.get_output_file_name = self._task_output_function
        self._task.get_input_file_names = self._task_input_function
        self._task.get_input_file_names_from_dict = self._task_input_function_from_dict


def on_temporary_files(run_function):
    """
    Wrapper for decorating a task's run function to use temporary files as outputs.

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

    Warning:
        The decorator only edits the function :meth:`b2luigi.Task.get_output_file_name`. If you are using
        the output directly, you have to take care of using the temporary path correctly by yourself!

    """

    @wraps(run_function)
    def run(self, *args, **kwargs):
        with TemporaryFileContextManager(self):
            run_function(self, *args, **kwargs)

    return run
