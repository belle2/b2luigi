from cyclopts import App, Parameter
from typing import Annotated

from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import get_task_classes, get_task_instance, process_task_instance, resolve_defaults, suggest

show_app = App(name="show", help="Show the output of a given task")


def show_task(class_name: str, task_filename="tasks.py", parameters_file="parameters.py") -> None:
    task_instance = get_task_instance(class_name, task_filename, parameters_file)
    process_task_instance(task_instance, show_output=[class_name])


@show_app.default
def show(
    classname: Annotated[
        str,
        Parameter(name=["--task", "-t", "--classname", "-c"], help="The name of the task class to run"),
    ],
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
    parameter_filename: Annotated[
        str | None,
        Parameter(name=["--params-file", "-p"], help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
):
    d = resolve_defaults(task_filename, parameter_filename)
    tasks = get_task_classes(d.task_file)
    names = [cls.__name__ for cls in tasks]

    if classname not in names:
        suggestion = suggest(classname, names)
        msg = f"Unknown task '{classname}'."
        if suggestion:
            msg += f" Did you mean '{suggestion}'?"
        msg += " Use 'b2luigi run list' to see available tasks."
        raise CliUserError(msg)

    show_task(
        class_name=classname,
        task_filename=d.task_file,
        parameters_file=d.params_file,
    )
