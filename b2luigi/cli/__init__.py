import importlib.util
import os
import inspect
from typing import Any, Dict, Literal

import b2luigi
from cyclopts import App, Parameter

from b2luigi.cli.task_commandy import register_task_command

app = App(name="b2luigi", help="Run user-defined b2luigi tasks")


def import_from_file(filename: str, module_name: str) -> Any:
    path = os.path.join(os.getcwd(), filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} not found in the current directory")

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {filename}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_parameters() -> Dict[str, Any]:
    params_module = import_from_file("parameters.py", "user_parameters")
    if not hasattr(params_module, "config"):
        raise AttributeError(f"{filename} must define a 'config' variable")
    return params_module.config


def load_task_class(class_name: str) -> Any:
    tasks_module = import_from_file("tasks.py", "user_tasks")
    if not hasattr(tasks_module, class_name):
        raise AttributeError(f"Class '{class_name}' not found in {filename}")
    return getattr(tasks_module, class_name)


def get_task_instance(class_name: str) -> Any:
    params = load_parameters()
    TaskClass = load_task_class(class_name)
    return TaskClass(**params)


def process_task_instance(task_instance: Any, **kwargs) -> None:
    b2luigi.process(task_instance, ignore_additional_command_line_args=True, **kwargs)


def run_task(class_name: str) -> None:
    task_instance = get_task_instance(class_name)
    process_task_instance(task_instance)


def remove_task(class_name: str) -> None:
    task_instance = get_task_instance(class_name)
    process_task_instance(task_instance, remove=[class_name])


def list_all_task_classes():
    try:
        tasks_module = import_from_file("tasks.py", "user_tasks")
    except (FileNotFoundError, ImportError):
        return ("NoTasksFound",)

    tasks = []
    for name, obj in inspect.getmembers(tasks_module):
        if inspect.isclass(obj) and issubclass(obj, b2luigi.Task):
            if obj.__module__ == "user_tasks":
                tasks.append(name)

    if not tasks:
        return ("NoTasksFound",)

    return tuple(tasks)


run_app = App(name="run", help="Run a task class from tasks.py")
show_output_app = App(name="show_output", help="Run a task class and show its output")
remove_app = App(name="remove", help="Remove the output of a task class")

app.command(run_app)
app.command(show_output_app)
app.command(remove_app)


@run_app.default
def run(classname: Literal[list_all_task_classes()]):
    """Run a task class from tasks.py."""
    run_task(classname, task_filename, parameter_filename)


for task in list_all_task_classes(return_classes=True):
    register_task_command(task, app)


@remove_app.default
def remove(classname: Literal[list_all_task_classes()]):
    """Remove the output of a task class from tasks.py."""
    remove_task(classname)


def main():
    app()


if __name__ == "__main__":
    main()
