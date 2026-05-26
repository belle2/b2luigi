from dataclasses import dataclass
import difflib
import importlib.util
import inspect
import json
import os
from typing import Any, Dict, Generator, List, Optional, Tuple, Type

import b2luigi
from b2luigi.cli.errors import CliUserError


@dataclass(frozen=True)
class Defaults:
    task_file: str
    params_file: str


def resolve_defaults(task_file: str | None, params_file: str | None) -> Defaults:
    task = task_file or os.getenv("B2LUIGI_TASK_FILE") or "tasks.py"
    params = params_file or os.getenv("B2LUIGI_PARAMS_FILE") or "parameters.py"
    return Defaults(task_file=task, params_file=params)


def suggest(bad: str, candidates: List[str]) -> str | None:
    m = difflib.get_close_matches(bad, candidates, n=1, cutoff=0.6)
    return m[0] if m else None


def import_from_file(filename: str, module_name: str) -> Any:
    path = os.path.join(os.getcwd(), filename)  # TODO: Replace getcwd
    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} not found in the current directory")

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {filename}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def task_generator(task_file: str = "tasks.py") -> Generator[Tuple[str, Any], Any, None]:
    tasks_module = import_from_file(task_file, "TaskClasses")
    for name, obj in inspect.getmembers(tasks_module):
        yield name, obj


def is_from_task_classes(obj: Any) -> bool:
    return inspect.isclass(obj) and issubclass(obj, b2luigi.Task) and obj.__module__ == "TaskClasses"


def get_task_classnames(task_file: str = "tasks.py") -> list[str]:
    task_names = []
    for name, obj in task_generator(task_file):
        if is_from_task_classes(obj):
            task_names.append(name)

    if not task_names:
        task_names.append("NoTaskFound")
    return sorted(task_names)


def get_task_classes(task_file: str = "tasks.py") -> list[Type[b2luigi.Task]]:
    task_classes = []
    for _, obj in task_generator(task_file):
        if is_from_task_classes(obj):
            task_classes.append(obj)

    if not task_classes:
        raise ValueError("No task classes found in the specified file.")
    return sorted(task_classes, key=lambda cls: cls.__name__)


def load_parameters(filename="parameters.py") -> Dict[str, Any]:
    params_module = import_from_file(filename, "user_parameters")
    if not hasattr(params_module, "config"):
        raise AttributeError(f"{filename} must define a 'config' variable")
    return params_module.config


def load_task_class(class_name: str, filename="tasks.py") -> Type[b2luigi.Task]:
    tasks_module = import_from_file(filename, "TaskClasses")
    if not hasattr(tasks_module, class_name):
        raise AttributeError(f"Class '{class_name}' not found in {filename}")
    return getattr(tasks_module, class_name)


def get_task_instance(
    class_name: str,
    task_filename="tasks.py",
    parameters_file="parameters.py",
    overrides: Optional[Dict[str, object]] = None,
) -> b2luigi.Task:
    params = load_parameters(parameters_file)
    TaskClass = load_task_class(class_name, task_filename)
    if overrides:
        params.update(overrides)
    return TaskClass(**params)


def process_task_instance(task_instance: Any, **kwargs) -> None:
    b2luigi.process(task_instance, ignore_additional_command_line_args=True, **kwargs)


def parse_kv_params(items: List[str]) -> Dict[str, object]:
    """Parse a list of ``key=value`` strings into a dict.

    Values are interpreted as JSON when possible (covering integers, floats,
    booleans, lists, dicts, and quoted strings).  Plain strings that are not
    valid JSON are kept as-is.

    Args:
        items: List of ``"key=value"`` strings.

    Returns:
        Dict mapping parameter names to their parsed values.

    Raises:
        :class:`CliUserError`: If an item is missing ``=`` or the key is empty.
    """
    out: Dict[str, object] = {}
    for item in items:
        if "=" not in item:
            raise CliUserError(f"Invalid --param '{item}'. Use key=value.")
        key, raw = item.split("=", 1)
        key = key.strip()
        raw = raw.strip()
        if not key:
            raise CliUserError(f"Invalid --param '{item}'. Key is empty.")
        try:
            val = json.loads(raw)
        except Exception:
            val = raw
        out[key] = val
    return out
