from typing import Iterable, Iterator
from b2luigi.core import utils
from typing import Any, Union, List, Dict, Optional, Type

import luigi

from b2luigi.core.utils import create_output_file_name
from b2luigi.core.target import LocalTarget, FileSystemTarget
from b2luigi.core.settings import get_setting, _no_value


class Task(luigi.Task):
    """
    Drop in replacement for ``luigi.Task`` which is 100% API compatible.
    It just adds some useful methods for handling output file name generation using
    the parameters of the task.
    See :ref:`quick-start-label` on information on how to use the methods.

    Example:

        .. code-block:: python

          class MyAverageTask(b2luigi.Task):
              def requires(self):
                  for i in range(100):
                      yield self.clone(MyNumberTask, some_parameter=i)

              def output(self):
                  yield self.add_to_output("average.txt")

              def run(self):
                  # Build the mean
                  summed_numbers = 0
                  counter = 0
                  for input_file in self.get_input_file_names("output_file.txt"):
                      with open(input_file, "r") as f:
                          summed_numbers += float(f.read())
                          counter += 1

                  average = summed_numbers / counter

                  with self.get_output_file("average.txt").open("w") as f:
                      f.write(f"{average}\\n")
    """

    def add_to_output(
        self,
        output_file_name: str,
        target_class: Optional[Type[FileSystemTarget]] = None,
        result_dir: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, LocalTarget]:
        """
        Call this in your ``output()`` function to add a target to the list of files,
        this task will output.
        Always use in combination with ``yield``.
        This function will automatically add all current parameter values to
        the file name when used in the form::

            <result-path>/param1=value1/param2=value2/.../<output-file-name.ext>

        This function will by default use a :class:`b2luigi.LocalTarget`, but you can also pass a different ``target_class`` as an argument.
        If you do not want this, you can override the :obj:`_get_output_file_target` function.

        Example:
            This adds two files called ``some_file.txt`` and ``some_other_file.txt`` to the output:

            .. code-block:: python

              def output(self):
                  yield self.add_to_output("some_file.txt")
                  yield self.add_to_output("some_other_file.txt")

        Args:
            output_file_name (str): the file name of the output file.
                Refer to this file name as a key when using :obj:`get_input_file_names`,
                :obj:`get_output_file_names` or :obj:`get_output_file`.
            target_class: which class of :obj:`FileSystemTarget` to instantiate for this target.
                defaults to :class:`b2luigi.LocalTarget`
            result_dir (str, optional): Optionally pass a `result_dir` to the :obj:`create_output_file_name`.
            kwargs: kwargs to be passed to the `__init__` of the Target_class via the :obj:`_get_output_file_target` function

        Returns:
            A dictionary with the output file name as key and the target as value.
        """
        return {
            output_file_name: self._get_output_file_target(
                output_file_name, target_class, result_dir=result_dir, **kwargs
            )
        }

    @staticmethod
    def _transform_io(input_generator: Iterable[FileSystemTarget]) -> Dict[str, List[str]]:
        file_paths: Dict[str, List[str]] = utils.flatten_to_file_paths(input_generator)

        return file_paths

    def get_all_input_file_names(self) -> Iterator[str]:
        """
        Return all file paths required by this task.

        Example:

            .. code-block:: python

              class TheSuperFancyTask(b2luigi.Task):
                  def dry_run(self):
                      for name in self.get_all_output_file_names():
                          print(f"\t\toutput:\t{name}")

        Yields:
            Iterator[str]: An iterator over the input file paths as strings.
        """
        for file_names in self._transform_io(self.input()).values():
            for file_name in file_names:
                yield file_name

    def get_input_file_names(self, key: Optional[str] = None) -> Union[Dict[str, List[str]], List[str]]:
        """
        Get a dictionary of input file names of the tasks, which are defined in our requirements.
        Either use the key argument or dictionary indexing with the key given to :obj:`add_to_output`
        to get back a list (!) of file paths.

        Args:
            key (str, optional): If given, only return a list of file paths with this given key.

        Return:
            If key is none, returns a dictionary of keys to list of file paths.
            Else, returns only the list of file paths for this given key.
        """
        if key is not None:
            return self._transform_io(self.input())[key]
        return self._transform_io(self.input())

    def get_input_file_names_from_dict(
        self, requirement_key: str, key: Optional[str] = None
    ) -> Union[Dict[str, List[str]], List[str]]:
        """
        Get a dictionary of input file names of the tasks, which are defined in our requirements.

        The requirement method should return a dict whose values are generator expressions (!)
        yielding required task objects.

        Example:

            .. code-block:: python

              class TaskB(luigi.Task):

                  def requires(self):
                      return {
                          "a": (TaskA(5.0, i) for i in range(100)),
                          "b": (TaskA(1.0, i) for i in range(100)),
                      }

                  def run(self):
                      result_a = do_something_with_a(
                          self.get_input_file_names_from_dict("a")
                      )
                      result_b = do_something_with_b(
                          self.get_input_file_names_from_dict("b")
                      )

                      combine_a_and_b(
                          result_a,
                          result_b,
                          self.get_output_file_name("combined_results.txt")
                      )

                  def output(self):
                      yield self.add_to_output("combined_results.txt")

        Either use the key argument or dictionary indexing with the key given to :obj:`add_to_output`
        to get back a list (!) of file paths.

        Args:
            requirement_key (str): Specifies the required task expression.
            key (str, optional): If given, only return a list of file paths with this given key.

        Return:
            If key is none, returns a dictionary of keys to list of file paths.
            Else, returns only the list of file paths for this given key.
        """
        if key is not None:
            return self._transform_io(self.input()[requirement_key])[key]
        return self._transform_io(self.input()[requirement_key])

    def get_input_file_name(self, key: Optional[str] = None) -> str:
        """
        Wraps :obj:`get_input_file_names` and asserts there is only one input file.

        Args:
            key (str, optional): Return the file path with this given key.

        Return:
            File path for the given key.
        """
        input_obj = self.get_input_file_names(key)
        if isinstance(input_obj, list):
            if len(input_obj) == 1:
                return input_obj[0]
        elif isinstance(input_obj, dict):
            if len(input_obj) == 1:
                value = next(iter(input_obj.values()))
                if isinstance(value, list) and len(value) == 1:
                    return value[0]
        raise ValueError(
            f"Found more than 1 input file for the key '{key}'. If this is expected use self.get_input_file_names instead."
        )

    def get_all_output_file_names(self) -> Iterator[str]:
        """
        Return all file paths created by this task.

        Example:

            .. code-block:: python

              class TheSuperFancyTask(b2luigi.Task):
                  def dry_run(self):
                      for name in self.get_all_output_file_names():
                          print(f"\t\toutput:\t{name}")

        Yields:
            str: The file path of each output file.
        """
        for file_names in self._transform_io(self.output()).values():
            for file_name in file_names:
                yield file_name

    def get_output_file_name(self, key: str) -> str:
        """
        Analogous to :obj:`get_input_file_names` this function returns
        a an output file defined in out output function with
        the given key.

        In contrast to :obj:`get_input_file_names`, only a single file name
        will be returned (as there can only be a single output file with a given name).

        Args:
            key (str): Return the file path with this given key.

        Return:
            Returns only the file path for this given key.
        """
        target: luigi.Target = self._get_output_target(key)
        file_path: str = target.path

        return file_path

    def _get_input_targets(self, key: str) -> List[luigi.Target]:
        """
        Retrieve the input targets associated with a specific key.

        This method acts as a shortcut to access the input targets for a given key
        from the task's input.

        Args:
            key (str): The key for which to retrieve the corresponding input targets.

        Returns:
            luigi.Target: The luigi target(s) associated with the specified key.

        Raises:
            KeyError: If the specified key is not found in the input dictionary.
        """
        input_dict = utils.flatten_to_dict_of_lists(self.input())
        return input_dict[key]

    def _get_output_target(self, key: str) -> FileSystemTarget:
        """
        Retrieves the output target associated with a specific key.

        This method acts as a shortcut to access a ``luigi`` target from the task's output.

        Args:
            key (str): The key for which the output target is to be retrieved.

        Returns:
            luigi.Target: The ``luigi`` target associated with the specified key.
        """
        output_dict: Dict[str, FileSystemTarget] = utils.flatten_to_dict(self.output())
        return output_dict[key]

    def _get_output_file_target(
        self,
        base_filename: str,
        target_class: Optional[Type[FileSystemTarget]] = None,
        result_dir: Optional[str] = None,
        **kwargs: Any,
    ) -> FileSystemTarget:
        """
        Generates a Luigi file system target for the output file.

        This method constructs the output file name using the provided base filename
        and additional keyword arguments, and then returns a file system target
        instance of the specified target class.

        Args:
            base_filename (str): The base name of the output file.
            target_class (Type[FileSystemTarget], optional): The class of the file system target to use.
                Defaults to :class:`b2luigi.LocalTarget`.
            result_dir (str, optional): Optionally pass a `result_dir` to the :obj:`create_output_file_name`.
            kwargs (Any): Additional keyword arguments passed to the target_class' `__init__`

        Returns:
            LocalTarget: An instance of the specified file system target class pointing to the output file.
        """
        file_name: str = create_output_file_name(
            self,
            base_filename,
            result_dir=result_dir,
        )
        target = self._resolve_output_file_target(
            file_name,
            target_class=target_class,
            **kwargs,
        )
        return target

    def _resolve_output_file_target(
        self,
        filename: str,
        target_class: Optional[Type[FileSystemTarget]] = None,
        **kwargs: Any,
    ) -> FileSystemTarget:
        """
        Resolves and returns a file system target based on the provided filename and optional target class.
        This method determines the appropriate file system target class to use for the given filename.
        It prioritizes the provided `target_class`, falls back to a default target class specified in
        the settings, or defaults to using a :class:`b2luigi.LocalTarget` if no other target class is specified.

        Args:
            filename (str): The name of the file for which the target is being resolved.
            target_class (Optional[Type[FileSystemTarget]]): An optional file system target class to use.
              If provided, this class will be used to create the target.
            kwargs (Any): Additional keyword arguments to pass to the target class constructor.

        Returns:
            FileSystemTarget: The resolved file system target class.

        Notes:
            - The `default_task_target_class` and `target_class_kwargs` settings are retrieved using
              the :meth:`b2luigi.get_setting` function.
            - If `target_class` is provided, it takes precedence over the default target class.
            - If neither `target_class` nor `default_task_target_class` is provided, a :class:`b2luigi.LocalTarget`
              is used as the default.
        """

        setting_target_class = get_setting("default_task_target_class", default=_no_value, task=self)
        setting_target_class_kwargs = get_setting("target_class_kwargs", default=_no_value, task=self)

        if setting_target_class_kwargs is not _no_value:
            kwargs.update(setting_target_class_kwargs)

        # Target class was already given
        if target_class is not None:
            return target_class(filename, **kwargs)

        # Neither target class nor default_task_target_class was given
        # Use the local target
        if setting_target_class is _no_value:
            return LocalTarget(filename, **kwargs)

        # Finally, use the given target class
        return setting_target_class(filename, **kwargs)

    def _remove_output_file_target(self, base_filename: str) -> None:
        """
        Removes the output file target associated with the given base filename.

        This method retrieves the output file target using the provided base filename
        and attempts to remove it. If the target does not have a `remove` method,
        a `NotImplementedError` is raised.

        Args:
            base_filename (str): The base filename used to identify the output file target.

        Raises:
            NotImplementedError: If the target does not have a `remove` method.
        """
        target: FileSystemTarget = self._get_output_file_target(base_filename)
        if hasattr(target, "remove"):
            target.remove()
        else:
            raise NotImplementedError(
                f"Cannot remove output file target for {base_filename}. " "The target does not have a remove method."
            )

    def _remove_output(self) -> None:
        """
        Removes all output file targets associated with the task.

        This method iterates through all output file names retrieved from
        :obj:`get_all_output_file_names` and removes each corresponding output
        file target by calling :obj:`_remove_output_file_target`.

        .. warning::
            Be very careful with this method!
            It will remove all output files of this task!
            This is not reversible.

        .. hint::
            If you are very sure in what you are doing, you can use this method
            to remove all output files of this task by calling it in the
            :obj:`remove_output` method of your task.

        Example:
            .. code-block:: python

              class TheSuperFancyTask(b2luigi.Task):
                  def remove_output(self):
                      self._remove_output()
        Returns:
            None
        """
        for key in self.get_all_output_file_names():
            try:
                self._remove_output_file_target(key)
            except Exception as ex:
                print(f"Could not remove output file {key}: {ex}")


class ExternalTask(Task, luigi.ExternalTask):
    """Direct copy of :obj:`luigi.ExternalTask`, but with the capabilities of :obj:`Task` added."""

    pass


class WrapperTask(Task, luigi.WrapperTask):
    """Direct copy of :obj:`luigi.WrapperTask`, but with the capabilities of :obj:`Task` added."""

    pass


class NotCompletedTask(Task):
    """
    A custom task class that extends the base Task class and provides a
    specialized `complete` method to determine task completion status.
    This class introduces an additional `check_complete` attribute to
    control whether child tasks are checked for completion.

    Attributes:
        check_complete (bool): A flag indicating whether to check the
            completion status of child tasks. Defaults to True.

    Methods:
        complete() -> bool:
            Determines if the task is complete. This method checks the
            completion status of the current task and its child tasks
            (if `check_complete` is True). If any child task is incomplete,
            the method returns False. If `check_complete` is False, the
            method assumes the task is complete regardless of child task
            statuses.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.check_complete: bool = True

    def complete(self) -> bool:
        """
        Determines whether the task and its dependencies are complete.

        This method overrides the default ``complete`` method to include a custom
        check for child tasks. It recursively checks the completion status of
        required tasks until a task with ``check_complete = False`` is encountered.

        Returns:
            bool: True if the task and all its dependencies are complete,
                  False otherwise.

        Raises:
            AttributeError: If the ``requires`` method does not return a single task
                            or an iterable of tasks.
        """
        if not super().complete():
            return False

        if not self.check_complete:
            return True

        requires = self.requires()

        try:
            if not requires.complete():
                return False
        except AttributeError:
            for task in requires:
                if not task.complete():
                    return False

        return True
