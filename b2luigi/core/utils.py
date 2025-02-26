import contextlib
import importlib

import itertools
import os
import collections
import sys
import types
from typing import Any, Dict, List, Optional, Iterator, Iterable
import shlex
import copy

import luigi

import colorama

from b2luigi.core.settings import get_setting


@contextlib.contextmanager
def remember_cwd():
    """Helper contextmanager to stay in the same cwd"""
    old_cwd = os.getcwd()
    try:
        yield
    finally:
        os.chdir(old_cwd)


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

    Parameters:
        kwargs: Each keyword argument should be an iterable

    Return:
        A list of kwargs where each list of input keyword arguments is cross-multiplied with every other.
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

    :param kwargs: The input keyword arguments
    :return: Same as kwargs, but each value mapped to a list if not a list already
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

    :param inputs: A dict of lists of luigi targets
    :return: A dict with the keys replaced by the basename of the targets and the values by the full path
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
    All non-dict values will be turned into a dictionary with value -> {value: value}

    Example:
    .. code-block:: python

        >>> flatten_to_dict([{"a": 1, "b": 2}, {"c": 3}, "d"])
        {'a': 1, 'b': 2, 'c': 3, 'd': 'd'}

    :param inputs: The input structure
    :return: A dict constructed as described above.
    """
    inputs: List[Any] = _flatten(inputs)
    inputs: Dict[Any, Any] = map(_to_dict, inputs)

    joined_dict = {}
    for i in inputs:
        for key, value in i.items():
            joined_dict[key] = value
    return joined_dict


def flatten_to_dict_of_lists(inputs: Iterable[Any]) -> Dict[Any, List]:
    inputs: List = _flatten(inputs)
    inputs: Dict[List] = map(_to_dict, inputs)

    joined_dict: Dict[str, List] = collections.defaultdict(list)
    for i in inputs:
        for key, value in i.items():
            joined_dict[key].append(value)
    return dict(joined_dict)


def task_iterator(task, only_non_complete=False):
    # create cache of already seen tasks, so that when we recurse through the
    # DAG and multiple nodes have the same child, we don't return
    # the same child multiple times
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


def get_all_output_files_in_tree(root_module, key=None):
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


def filter_from_params(output_files, **kwargs):
    kwargs_list = fill_kwargs_with_lists(**kwargs)

    if not kwargs_list:
        return output_files

    file_names = []

    for kwargs in product_dict(**kwargs_list):
        for output_dict in output_files:
            parameters = output_dict["parameters"]

            not_use = False
            for key, value in kwargs.items():
                if key in parameters and str(parameters[key]) != str(value):
                    not_use = True
                    break

            if not_use:
                continue

            file_names.append(output_dict)

    return {x["file_name"]: x for x in file_names}.values()


def get_task_from_file(file_name, task_name, **kwargs):
    spec = importlib.util.spec_from_file_location("module.name", os.path.basename(file_name))
    task_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_module)

    m = getattr(task_module, task_name)(**kwargs)

    return m


def get_serialized_parameters(task):
    """Get a string-typed ordered dict of key=value for the significant parameters"""
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
    serialized_parameters = get_serialized_parameters(task)

    if not result_dir:
        # Be sure to evaluate things relative to the current executed file, not to where we are now
        result_dir: str = map_folder(get_setting("result_dir", task=task, default=".", deprecated_keys=["result_path"]))

    for key, value in serialized_parameters.items():
        # Raise error if parameter value contains path separator "/" ("\" on Windows)
        # or is not interpretable as basename due to other reasons.
        if value != os.path.basename(value):
            raise ValueError(
                f"Parameter `{key}={value}` cannot be interpreted as directory name. "
                f"Make sure it does not contain the path separators ``{os.path.sep}``. "
                "Consider using a hashed parameter (e.g. ``b2luigi.Parameter(hashed=True)``)."
            )

    use_parameter_name = get_setting("use_parameter_name_in_output", task=task, default=True)
    if use_parameter_name:
        param_list: List[str] = [f"{key}={value}" for key, value in serialized_parameters.items()]
    else:
        param_list: List[str] = [f"{value}" for value in serialized_parameters.values()]
    output_path: str = os.path.join(result_dir, *param_list)

    return os.path.join(output_path, base_filename)


def get_log_file_dir(task):
    if hasattr(task, "get_log_file_dir"):
        log_file_dir = task.get_log_file_dir()
        return log_file_dir

    # Be sure to evaluate things relative to the current executed file, not to where we are now
    base_log_file_dir = map_folder(get_setting("log_dir", task=task, default="logs", deprecated_keys=["log_folder"]))
    log_file_dir = create_output_file_name(task, task.get_task_family() + "/", result_dir=base_log_file_dir)

    return log_file_dir


def get_task_file_dir(task):
    if hasattr(task, "get_task_file_dir"):
        task_file_dir = task.get_task_file_dir()
        return task_file_dir

    task_file_dir = create_output_file_name(task, task.get_task_family() + "/")

    return task_file_dir


def get_filename():
    import __main__

    return __main__.__file__


def map_folder(input_folder):
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
    if isinstance(d, dict):
        return d

    return {d: d}


def _flatten(struct: Iterable) -> List:
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


def on_failure(self, exception):
    log_file_dir = os.path.abspath(get_log_file_dir(self))

    print(colorama.Fore.RED)
    print("Task", self.task_family, "failed!")
    print("Parameters")
    for key, value in get_filled_params(self).items():
        print("\t", key, "=", value)
    print("Please have a look into the log files in")
    print(log_file_dir)
    print(colorama.Style.RESET_ALL)


def add_on_failure_function(task):
    task.on_failure = types.MethodType(on_failure, task)


def create_cmd_from_task(task):
    filename = os.path.basename(get_filename())

    prefix = get_setting("executable_prefix", task=task, default=[], deprecated_keys=["cmd_prefix"])

    if isinstance(prefix, str):
        raise ValueError("Your specified executable_prefix needs to be a list of strings, e.g. [strace]")

    cmd = prefix

    executable = get_setting("executable", task=task, default=[sys.executable])

    if isinstance(executable, str):
        raise ValueError("Your specified executable needs to be a list of strings, e.g. [python3]")

    cmd += executable
    cmd += [filename, "--batch-runner", "--task-id", task.task_id]

    return cmd


def create_output_dirs(task):
    """Create all output dicts if needed. Normally only used internally."""
    output_list = flatten_to_dict(task.output())
    output_list = output_list.values()

    for output in output_list:
        output.makedirs()


def get_filled_params(task):
    """Helper function for getting the parameter list with each parameter set to its current value"""
    return {key: getattr(task, key) for key, _ in task.get_params()}


def is_subdir(path, parent_dir):
    path = os.path.abspath(path)
    parent_dir = os.path.abspath(parent_dir)
    return os.path.commonpath([path, parent_dir]) == parent_dir


def create_apptainer_command(command, task=None):
    env_setup_script = get_setting("env_script", task=task, default="")
    if not env_setup_script:
        raise ValueError("Apptainer execution requires an environment setup script.")

    apptainer_image = get_setting("apptainer_image", task=task, default="")

    # If the batch system is gbasf2, we cannot use apptainer
    if get_setting("batch_system", default="lsf", task=task) == "gbasf2":
        raise ValueError("Invalid batch system for apptainer usage. Apptainer is not supported for gbasf2.")

    exec_command = ["apptainer", "exec"]
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
