import itertools
import os
import collections
import sys
import types
from typing import Any, Dict, List, Optional, Iterator, Iterable
import shlex
import copy
import shutil
import logging

import luigi

import colorama

from b2luigi.core.settings import get_setting


def product_dict(**kwargs: Any) -> Iterator[Dict[str, Any]]:
    """
    Cross-product the given parameters and return a list of dictionaries.

    Example:

        .. code-block:: python

            >>> list(product_dict(arg_1=[1, 2], arg_2=[3, 4]))
            [{'arg_1': 1, 'arg_2': 3}, {'arg_1': 1, 'arg_2': 4}, {'arg_1': 2, 'arg_2': 3}, {'arg_1': 2, 'arg_2': 4}]

        The thus produced list can directly be used as inputs for a required tasks:

        .. code-block:: python

            def requires(self):
                for args in product_dict(arg_1=[1, 2], arg_2=[3, 4]):
                    yield some_task(**args)

    Args:
        kwargs (Any): Each keyword argument should be an iterable

    Return:
        Iterator[Dict[str, Any]]: A list of kwargs where each list of input keyword
        arguments is cross-multiplied with every other.
    """
    keys = kwargs.keys()
    vals = kwargs.values()
    for instance in itertools.product(*vals):
        yield dict(zip(keys, instance))


def fill_kwargs_with_lists(**kwargs: Any) -> Dict[str, List[Any]]:
    """
    Return the kwargs with each value mapped to [value] if not a list already.

    Example:
    .. code-block:: python

        >>> fill_kwargs_with_lists(arg_1=[1, 2], arg_2=3)
        {'arg_1': [1, 2], 'arg_2': [3]}

    Args:
        kwargs: The input keyword arguments
    Return:
        Dict[str, List[Any]]: Same as kwargs, but each value mapped to a list if not a list already
    """
    return_kwargs = {}
    for key, value in kwargs.items():
        if value is None:
            value = []
        if isinstance(value, str) or not isinstance(value, collections.abc.Iterable):
            value = [value]
        return_kwargs[key] = value

    return return_kwargs


def flatten_to_file_paths(inputs: Iterable[luigi.target.FileSystemTarget]) -> Dict[str, List[str]]:
    """
    Take in a dict of lists of luigi targets and replace each luigi target by its corresponding path.
    For dicts, it will replace the value as well as the key. The key will however only by the basename of the path.

    Args:
        inputs: A dict of lists of luigi targets
    Return:
        Dict[str, List[str]]: A dict with the keys replaced by the basename of the targets and the values by the full path
    """
    input_dict: Dict[Any, List[luigi.target.FileSystemTarget]] = flatten_to_dict_of_lists(inputs)

    return {
        os.path.basename(key.path if hasattr(key, "path") else key): [
            val.path if hasattr(val, "path") else val for val in value
        ]
        for key, value in input_dict.items()
    }


def flatten_to_dict(inputs: Iterable[Any]) -> Dict[Any, Any]:
    """
    Return a whatever input structure into a dictionary.
    If it is a dict already, return this.
    If is is an iterable of dict or dict-like objects, return the merged dictionary.
    All non-dict values will be turned into a dictionary with value -> ``{value: value}``

    Example:
    .. code-block:: python

        >>> flatten_to_dict([{"a": 1, "b": 2}, {"c": 3}, "d"])
        {'a': 1, 'b': 2, 'c': 3, 'd': 'd'}

    Args:
        inputs (Iterable[Any]): The input structure
    Return:
        Dict[Any, Any]: A dict constructed as described above.
    """
    inputs: List[Any] = _flatten(inputs)
    inputs: Dict[Any, Any] = map(_to_dict, inputs)

    joined_dict = {}
    for i in inputs:
        for key, value in i.items():
            joined_dict[key] = value
    return joined_dict


def flatten_to_dict_of_lists(inputs: Iterable[Any]) -> Dict[Any, List]:
    """
    Flattens a nested iterable structure into a dictionary of lists.

    This function takes an iterable of potentially nested structures, flattens it,
    and converts it into a dictionary where each key maps to a list of values
    associated with that key.

    Args:
        inputs (Iterable[Any]): The input iterable containing nested structures.

    Returns:
        Dict[Any, List]: A dictionary where keys are derived from the input
        and values are lists of corresponding items.
    """
    inputs: List = _flatten(inputs)
    inputs: Dict[List] = map(_to_dict, inputs)

    joined_dict: Dict[str, List] = collections.defaultdict(list)
    for i in inputs:
        for key, value in i.items():
            joined_dict[key].append(value)
    return dict(joined_dict)


def task_iterator(task, only_non_complete=False):
    """
    Iterates through a task and its dependencies in a directed acyclic graph (DAG),
    ensuring that each task is yielded only once.

    Args:
        task: The root task to start iterating from. This task should have methods
              ``complete()`` to check if the task is complete and ``deps()`` to retrieve
              its dependencies.
        only_non_complete (bool, optional): If True, only tasks that are not complete
              (as determined by the ``complete()`` method) will be yielded. Defaults to ``False``.

    Yields:
        task: Each unique task in the DAG, starting from the given root task and
              including its dependencies.

    Notes:
        - The function uses a cache (``already_seen_tasks``) to ensure that tasks
          are not yielded multiple times, even if they are dependencies of multiple
          parent tasks in the DAG.
        - The iteration is performed recursively using a nested helper function.
    """
    already_seen_tasks = set()

    # create another private function for recursion that has reference to `already_seen_tasks`
    def _unique_task_iterator(task, only_non_complete):
        if only_non_complete and task.complete():
            return

        yield task

        for dep in task.deps():
            if dep.task_id in already_seen_tasks:
                continue
            already_seen_tasks.add(dep.task_id)
            yield from _unique_task_iterator(dep, only_non_complete=only_non_complete)

    yield from _unique_task_iterator(task, only_non_complete)


def find_dependents(task_iterator, target_task):
    """
    Identifies and returns a set of tasks that are dependents of a specified target task.

    Args:
        task_iterator (iterable): An iterable of ``luigi`` task instances to search through.
        target_task (str): The name of the target task class to find dependents for.

    Returns:
        set: A set of tasks that are either instances of the target task class or depend on it.

    Notes:
        - A task is considered a dependent if it directly or indirectly requires the target task.
        - The ``requires()`` method of each task is used to determine dependencies.
    """
    dependents = set()

    def depends_on(task, target):
        for dep in luigi.task.flatten(task.requires()):
            if dep.__class__.__name__ == target:
                return True
            if depends_on(dep, target):
                return True
        return False

    for task in task_iterator:
        if task.__class__.__name__ == target_task:
            dependents.add(task)
        if depends_on(task, target_task):
            dependents.add(task)

    return dependents


def get_all_output_files_in_tree(root_module, key=None):
    """
    Recursively retrieves all output files from tasks in a given module tree.

    This function iterates through all tasks in the specified root module,
    collects their output files, and organizes them into a dictionary. If a
    specific key is provided, it returns the output files corresponding to
    that key.

    Args:
        root_module (module): The root module containing tasks to iterate over.
        key (str, optional): A specific key to filter the output files. If
            provided, only the output files corresponding to this key will
            be returned.

    Returns:
        dict: A dictionary where keys are file identifiers and values are lists
        of dictionaries containing:

            - ``exists`` (bool): Whether the file exists.
            - ``parameters`` (dict): Serialized parameters of the task.
            - ``file_name`` (str): Absolute path to the file.

    Raises:
        KeyError: If the specified key is not found in the output files.

    Notes:
        - The function uses :obj:`task_iterator` to traverse tasks in the module tree.
        - Output files are flattened and converted to absolute file paths.
    """
    if key:
        return get_all_output_files_in_tree(root_module)[key]

    all_output_files = collections.defaultdict(list)
    for task in task_iterator(root_module):
        output_dict = flatten_to_dict(task.output())
        if not output_dict:
            continue

        for target_key, target in output_dict.items():
            converted_dict = flatten_to_file_paths({target_key: target})
            file_key, file_name = converted_dict.popitem()

            all_output_files[file_key].append(
                dict(
                    exists=target.exists(),
                    parameters=get_serialized_parameters(task),
                    file_name=os.path.abspath(file_name.pop()),
                )
            )

    return all_output_files


def get_serialized_parameters(task):
    """
    Retrieve a string-typed ordered dictionary of significant parameters in the format ``key=value``.

    This function iterates over the parameters of a given task, filters out non-significant parameters,
    and serializes the significant ones. If a parameter has a custom serialization method (``serialize_hashed``),
    it will use that; otherwise, it defaults to the standard ``serialize`` method.

    Args:
        task: An object representing a task, which must implement the ``get_params`` method to
              retrieve its parameters and their metadata.

    Returns:
        collections.OrderedDict: An ordered dictionary where keys are parameter names and values
                                 are their serialized representations.
    """
    serialized_parameters = collections.OrderedDict()

    for key, parameter in task.get_params():
        if not parameter.significant:
            continue

        value = getattr(task, key)
        if hasattr(parameter, "serialize_hashed"):
            value = parameter.serialize_hashed(value)
        else:
            value = parameter.serialize(value)

        serialized_parameters[key] = value

    return serialized_parameters


def create_output_file_name(task, base_filename: str, result_dir: Optional[str] = None) -> str:
    """
    Generates an output file path based on the task's parameters, a base filename,
    and an optional result directory.

    Args:
        task: The task object containing parameters to serialize and use in the output path.
        base_filename (str): The base name of the output file.
        result_dir (Optional[str]): The directory where the output file should be saved.
            If not provided, it defaults to the ``result_dir`` setting.

    Returns:
        str: The full path to the output file.

    Raises:
        ValueError: If any parameter value contains a path separator or cannot be
            interpreted as a valid directory name.

    Notes:
        - The function ensures that the result directory is evaluated relative to the
          current executed file.
        - The parameter separator and whether to include parameter names in the output
          path are configurable via settings.
        - If ``use_parameter_name_in_output`` is enabled, the output path includes
          parameter names and values; otherwise, only parameter values are used.
        - If ``parameter_separator`` is set to a non-empty string, it will be used to separate
          parameter names and values in the output path.
    """
    serialized_parameters = get_serialized_parameters(task)

    if not result_dir:
        # Be sure to evaluate things relative to the current executed file, not to where we are now
        result_dir: str = map_folder(get_setting("result_dir", task=task, default=".", deprecated_keys=["result_path"]))

    separator = get_setting("parameter_separator", task=task, default="=")
    for key, value in serialized_parameters.items():
        # Raise error if parameter value contains path separator "/" ("\" on Windows)
        # or is not interpretable as basename due to other reasons.
        if value != os.path.basename(value):
            raise ValueError(
                f"Parameter `{key}{separator}{value}` cannot be interpreted as directory name. "
                f"Make sure it does not contain the path separators ``{os.path.sep}``. "
                "Consider using a hashed parameter (e.g. ``b2luigi.Parameter(hashed=True)``)."
            )

    use_parameter_name = get_setting("use_parameter_name_in_output", task=task, default=True)
    if use_parameter_name:
        param_list: List[str] = [f"{key}{separator}{value}" for key, value in serialized_parameters.items()]
    else:
        param_list: List[str] = [f"{value}" for value in serialized_parameters.values()]
    output_path: str = os.path.join(result_dir, *param_list)

    return os.path.join(output_path, base_filename)


def get_log_file_dir(task):
    """
    Determines the directory where log files for a given task should be stored.

    If the task has a custom method ``get_log_file_dir``, it will use that method
    to retrieve the log file directory. Otherwise, it constructs the log file
    directory path based on the task's settings and family.

    Args:
        task: The task object for which the log file directory is being determined.

    Returns:
        str: The path to the log file directory for the given task.
    """
    if hasattr(task, "get_log_file_dir"):
        log_file_dir = task.get_log_file_dir()
        return log_file_dir

    # Be sure to evaluate things relative to the current executed file, not to where we are now
    base_log_file_dir = map_folder(get_setting("log_dir", task=task, default="logs", deprecated_keys=["log_folder"]))
    log_file_dir = create_output_file_name(task, task.get_task_family() + "/", result_dir=base_log_file_dir)

    return log_file_dir


def get_task_file_dir(task):
    """
    Determines the directory path for a given task's output files.

    If the task has a method ``get_task_file_dir``, it will use that method
    to retrieve the directory path. Otherwise, it generates the directory
    path using the :obj:`create_output_file_name` function and the task's family name.

    Args:
        task: An object representing the task. It is expected to have a
              `get_task_file_dir` method or a `get_task_family` method.

    Returns:
        str: The directory path for the task's output files.
    """
    if hasattr(task, "get_task_file_dir"):
        task_file_dir = task.get_task_file_dir()
        return task_file_dir

    base_task_file_dir = map_folder(get_setting("task_file_dir", task=task, default="task_files"))
    task_file_dir = create_output_file_name(task, task.get_task_family() + "/", result_dir=base_task_file_dir)

    return task_file_dir


def get_filename():
    """
    Retrieves the absolute path of the main script being executed.

    Returns:
        str: The absolute path of the main script.
    """
    import __main__

    return os.path.abspath(__main__.__file__)


def map_folder(input_folder):
    """
    Maps a relative folder path to an absolute path based on the location of the current script.

    If the input folder path is already absolute, it is returned as-is. Otherwise, the function
    determines the directory of the current script and joins it with the relative input folder path
    to produce an absolute path.

    Args:
        input_folder (str): The folder path to map. Can be either absolute or relative.

    Returns:
        str: The absolute path corresponding to the input folder.

    Raises:
        AttributeError: If the current script location cannot be determined, typically when running
                        in an interactive shell (e.g., Jupyter Notebook). In such cases, the user
                        is advised to provide absolute paths in their settings.
    """
    if os.path.isabs(input_folder):
        return input_folder

    try:
        filename = get_filename()
    except AttributeError as ex:
        raise type(ex)(
            "Could not determine the current script location. "
            "If you are running in an interactive shell (such as jupyter notebook) "
            "make sure to only provide absolute paths in your settings. "
            f"More Info:\n{ex}"
        ).with_traceback(sys.exc_info()[2])

    filepath = os.path.dirname(filename)

    return os.path.join(filepath, input_folder)


def _to_dict(d) -> Dict:
    """
    Converts the input into a dictionary. If the input is already a dictionary, it is returned as-is.
    Otherwise, a new dictionary is created with the input as both the key and the value.

    Args:
        d: The input to be converted into a dictionary. Can be of any type.

    Returns:
        Dict: A dictionary representation of the input.
    """
    if isinstance(d, dict):
        return d

    return {d: d}


def _flatten(struct: Iterable) -> List:
    """
    Recursively flattens a nested iterable structure into a single list.

    Args:
        struct (Iterable): The input structure to flatten.

    Returns:
        List: A flattened list containing all elements from the input structure.

    Notes:
        - If the input is a dictionary or a string, it is returned as a single-element list.
        - Non-iterable inputs are also returned as a single-element list.
    """
    if isinstance(struct, dict) or isinstance(struct, str):
        return [struct]

    result = []
    try:
        iterator = iter(struct)
    except TypeError:
        return [struct]

    for f in iterator:
        result += _flatten(f)

    return result


def on_failure(task, _):
    """
    Handles the failure of a task by generating an explanation message, printing it to stdout,
    and returning it to be sent back to the scheduler.

    Args:
        task: The task instance that failed.

    Returns:
        str: A detailed explanation of the failure, including the task ID, parameters, and
        the location of the log files.
    """
    explanation = f"Failed task {task} with task_id and parameters:\n"
    explanation += f"\ttask_id={task.task_id}\n"
    for key, value in get_filled_params(task).items():
        explanation += f"\t{key}={value}\n"
    explanation += "Please have a look into the log files in:\n"
    explanation += os.path.abspath(get_log_file_dir(task))

    # First print the explanation on stdout
    print(colorama.Fore.RED)
    print(explanation)
    print(colorama.Style.RESET_ALL)

    # Then return it: it will be sent back to the scheduler
    return explanation


def add_on_failure_function(task):
    """
    Assigns a custom failure handler to the given task.

    This function dynamically binds the :obj:`on_failure` method to the provided
    task object.

    Args:
        task: The task object to which the :obj:`on_failure` method will be attached.
    """
    task.on_failure = types.MethodType(on_failure, task)


def create_cmd_from_task(task):
    """
    Constructs a command-line argument list based on the provided task and its settings.

    The executable string is made up of three key components::

        <executable_prefix> <executable> <filename> --batch-runner --task-id ExampleTask_id_123 <task_cmd_additional_args>


    Args:
        task: An object representing the task for which the command is being created.

    Returns:
        list: A list of strings representing the command-line arguments.

    Raises:
        ValueError: If any of the following conditions are met:
            - The ``task_cmd_additional_args`` setting is not a list of strings.
            - The ``executable_prefix`` setting is not a list of strings.
            - The ``executable`` setting is not a list of strings.

    Notes:
        - The ``filename`` is included in the command if the ``add_filename_to_cmd``
          setting is enabled. (Default: ``True``)
        -
    """
    filename = os.path.basename(get_filename()) if get_setting("add_filename_to_cmd", task=task, default=True) else ""
    task_cmd_additional_args = get_setting("task_cmd_additional_args", task=task, default=[])

    if isinstance(task_cmd_additional_args, str):
        raise ValueError("Your specified task_cmd_additional_args needs to be a list of strings, e.g. ['--foo', 'bar']")

    prefix = get_setting("executable_prefix", task=task, default=[], deprecated_keys=["cmd_prefix"])

    if isinstance(prefix, str):
        raise ValueError("Your specified executable_prefix needs to be a list of strings, e.g. [strace]")

    cmd = prefix

    executable = get_setting("executable", task=task, default=[sys.executable])

    if isinstance(executable, str):
        raise ValueError("Your specified executable needs to be a list of strings, e.g. [python3]")

    cmd += executable
    cmd += [filename, "--batch-runner", "--task-id", task.task_id]
    cmd += task_cmd_additional_args

    return cmd


def create_output_dirs(task):
    """
    Creates the necessary output directories for a given task.

    This function takes a task object, retrieves its outputs, and ensures that
    the directories required for those outputs exist by calling the ``makedirs``
    method on each target.

    Args:
        task: The task object whose outputs need directories to be created.
    """
    output_list = flatten_to_dict(task.output())
    output_list = output_list.values()

    for output in output_list:
        output.makedirs()


def get_filled_params(task):
    """
    Retrieve a dictionary of parameter names and their corresponding values
    from a given task.

    Args:
        task: An object representing a task, which must have a `get_params`
              method that returns an iterable of parameter name and metadata
              pairs, and attributes corresponding to the parameter names.

    Returns:
        dict: A dictionary containing parameter names as keys and their
              respective values as values.
    """
    return {key: getattr(task, key) for key, _ in task.get_params()}


def is_subdir(path, parent_dir):
    """
    Determines if a given path is a subdirectory of a specified parent directory.

    Args:
        path (str): The path to check.
        parent_dir (str): The parent directory to compare against.

    Returns:
        bool: ``True`` if the path is a subdirectory of the parent directory, ``False`` otherwise.

    Note:
        Both ``path`` and ``parent_dir`` are converted to their absolute paths before comparison.
    """
    path = os.path.abspath(path)
    parent_dir = os.path.abspath(parent_dir)
    return os.path.commonpath([path, parent_dir]) == parent_dir


def get_apptainer_or_singularity(task=None):
    """
    Determines the command to use for containerization, prioritizing Apptainer
    over Singularity. If neither is available, raises a ValueError.

    The function first checks if a custom command is set via the ``apptainer_cmd``
    setting. If not, it checks for the availability of the ``apptainer`` or
    ``singularity`` commands in the system's PATH.

    Args:
        task (optional): An optional task object that may be used to retrieve
                         task-specific settings.

    Returns:
        str: The command to use for containerization, either "apptainer" or
             "singularity".

    Raises:
        ValueError: If neither Apptainer nor Singularity is available on the
                    system and no custom command is set.
    """
    set_cmd = get_setting("apptainer_cmd", default="", task=task)
    if set_cmd:
        return set_cmd
    if shutil.which("apptainer"):
        return "apptainer"
    elif shutil.which("singularity"):
        return "singularity"
    raise ValueError(
        "Neither apptainer nor singularity is available on this system. If you know that one of them is available on the batch system you can manually set it via the `apptainer_cmd` setting."
    )


def create_apptainer_command(command, task=None):
    """
    Constructs a command to execute within an Apptainer (or Singularity) container.

    This function generates a command string that sets up the necessary environment
    and mounts for running a given command inside an Apptainer container. It ensures
    that required settings are provided and validates compatibility with the batch
    system.

    Args:
        command (str): The command to be executed inside the Apptainer container.
        task (optional): An optional task object or identifier used to retrieve
            task-specific settings.

    Returns:
        list: A list of command-line arguments representing the full Apptainer
        execution command.

    Raises:
        ValueError: If the ``env_script`` is not provided.
        ValueError: If the batch system is ``gbasf2``, as Apptainer is not supported
            for this batch system.

    Notes:
        - ``apptainer_image`` is retrieved from the task settings.
        - ``apptainer_additional_params`` is used to specify additional parameters
          for the Apptainer command. Expecting a string.
        - ``apptainer_mounts`` is used to specify additional mount points for the
          Apptainer command. Expecting a list of strings.
        - ``apptainer_mount_defaults`` determines whether to include default
          mount points (e.g., result directory and log file directory).
    """
    env_setup_script = get_setting("env_script", task=task, default="")
    if not env_setup_script:
        raise ValueError("Apptainer execution requires an environment setup script.")

    apptainer_image = get_setting("apptainer_image", task=task, default="")

    # If the batch system is gbasf2, we cannot use apptainer
    if get_setting("batch_system", default="lsf", task=task) == "gbasf2":
        raise ValueError("Invalid batch system for apptainer usage. Apptainer is not supported for gbasf2.")

    exec_command = [get_apptainer_or_singularity(task=task), "exec"]
    additional_params = get_setting("apptainer_additional_params", default="", task=task)
    exec_command += [f" {additional_params}"] if additional_params else []

    # Add apptainer mount points if given
    apptainer_mounts = get_setting("apptainer_mounts", task=task, default=None)
    if apptainer_mounts is None:
        mounts = []
    else:
        mounts = copy.copy(apptainer_mounts)

    if get_setting("apptainer_mount_defaults", task=task, default=True):
        local_mounts = [
            map_folder(get_setting("result_dir", task=task, default=".", deprecated_keys=["result_path"])),
            get_log_file_dir(task=task),
        ]
        for mount in local_mounts:
            os.makedirs(mount, exist_ok=True)
            if not is_subdir(mount, os.getcwd()):
                mounts.append(mount)

    for mount in mounts:
        exec_command += ["--bind", mount]

    exec_command += [apptainer_image]
    exec_command += ["/bin/bash", "-c"]
    exec_command += [f"'source {env_setup_script} && {command}'"]

    # Do the shlex split for correct string interpretation
    return shlex.split(" ".join(exec_command))


def get_luigi_logger():
    """
    Retrieve the logger instance used by ``luigi``.

    Returns:
        logging.Logger: The logger instance for "luigi-interface".
    """
    logger = logging.getLogger("luigi-interface")
    return logger
