from enum import Enum
import os
import inspect
from typing import Annotated

import b2luigi
from cyclopts import App, Parameter

from b2luigi.cli.task_commands import register_task_command
from b2luigi.cli.utils import get_task_instance, import_from_file, process_task_instance

app = App(name="b2luigi", help="Run user-defined b2luigi tasks")


def run_task(class_name: str, task_filename="tasks.py", parameters_file="parameters.py") -> None:
    task_instance = get_task_instance(class_name, task_filename, parameters_file)
    process_task_instance(task_instance)


def remove_task(class_name: str) -> None:
    task_instance = get_task_instance(class_name)
    process_task_instance(task_instance, remove=[class_name])


def list_all_task_classes(filename="tasks.py", return_classes=False):
    tasks_module = import_from_file(filename, "TaskClasses")

    tasks = []
    for name, obj in inspect.getmembers(tasks_module):
        if inspect.isclass(obj) and issubclass(obj, b2luigi.Task):
            if obj.__module__ == "TaskClasses":
                if return_classes:
                    tasks.append(obj)
                else:
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
    run_task(str(classname).split(".")[-1], task_filename, parameter_filename)


@remove_app.default
def remove(
    classname: Annotated[
        task_classes_enum,
        Parameter(name=["--task", "-t", "--classname", "-c"], help="The name of the task class to remove"),
    ],
    task_filename: Annotated[
        str, Parameter(name=["--task-file", "-f"], help="The file containing the task definitions")
    ] = "tasks.py",
    parameter_filename: Annotated[
        str, Parameter(name=["--params-file", "-p"], help="The file containing the parameters")
    ] = "parameters.py",
):
    """Remove a task class from the Luigi scheduler."""
    remove_task(str(classname).split(".")[-1], task_filename, parameter_filename)


for task in list_all_task_classes(return_classes=True):
    print(f"Registering command for task: {task.__name__} {type(task)}")
    register_task_command(task, app)
    print(list(app._commands.keys()))


@remove_app.default
def remove(classname: Literal[list_all_task_classes()]):
    """Remove the output of a task class from tasks.py."""
    remove_task(classname)


def main():
    app()


if __name__ == "__main__":
    main()
