import importlib.util
import os
import inspect
from typing import Literal, Annotated

import b2luigi
from cyclopts import App, Parameter

app = App(name="b2luigi", help="Run user-defined b2luigi tasks")


def import_from_file(filename: str, module_name: str):
    path = os.path.join(os.getcwd(), filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} not found in the current directory")

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {filename}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_parameters(filename="parameters.py"):
    params_module = import_from_file(filename, "user_parameters")
    if not hasattr(params_module, "config"):
        raise AttributeError(f"{filename} must define a 'config' variable")
    return params_module.config


def load_task_class(class_name: str, filename="tasks.py"):
    tasks_module = import_from_file(filename, "user_tasks")
    if not hasattr(tasks_module, class_name):
        raise AttributeError(f"Class '{class_name}' not found in {filename}")
    return getattr(tasks_module, class_name)


def run_task(class_name: str, task_filename="tasks.py", parameters_file="parameters.py"):
    params = load_parameters(parameters_file)
    TaskClass = load_task_class(class_name, task_filename)
    task_instance = TaskClass(**params)
    b2luigi.process(task_instance, ignore_additional_command_line_args=False)


def list_all_task_classes(filename="tasks.py"):
    try:
        tasks_module = import_from_file(filename, "user_tasks")
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
app.command(run_app)


@run_app.default
def run(
    classname: str,
    task_filename: Annotated[
        str, Parameter(name=["--task-file", "-f"], help="The file containing the task definitions")
    ] = "tasks.py",
    parameter_filename: Annotated[
        str, Parameter(name=["--params-file", "-p"], help="The file containing the parameters")
    ] = "parameters.py",
):
    """Run a task class from tasks.py."""
    run_task(classname, task_filename, parameter_filename)


def main():
    app()


if __name__ == "__main__":
    main()
