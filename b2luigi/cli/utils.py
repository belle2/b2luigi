import importlib.util
import os
from typing import Any, Dict, Type

import b2luigi


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


def get_task_instance(class_name: str, task_filename="tasks.py", parameters_file="parameters.py") -> b2luigi.Task:
    params = load_parameters(parameters_file)
    TaskClass = load_task_class(class_name, task_filename)
    return TaskClass(**params)


def process_task_instance(task_instance: Any, **kwargs) -> None:
    b2luigi.process(task_instance, ignore_additional_command_line_args=True, **kwargs)
