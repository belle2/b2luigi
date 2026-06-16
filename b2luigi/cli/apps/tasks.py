"""CLI sub-app for listing and inspecting available task classes.

:Description: Implements ``b2luigi tasks`` (list all tasks) and
    ``b2luigi tasks info [CLASSNAME]`` (show docstring and parameters).
"""

from typing import Annotated, Optional

import typer

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    complete_task_names,
    get_task_classes,
    resolve_defaults,
    validate_classnames,
)

tasks_app = typer.Typer(
    name="tasks",
    invoke_without_command=True,
    help="List and inspect available task classes.",
)


def list_tasks(task_filename: Optional[str] = None) -> None:
    """Render a table of all available task classes.

    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    :raises CliUserError: If the task file is missing or contains no task classes.
    """
    d = resolve_defaults(task_filename, None)
    try:
        tasks = get_task_classes(d.task_file)
    except ValueError:
        raise CliUserError("No task classes found. Run 'b2luigi init' to create a starter project.")
    runner.render_task_list(tasks)


def show_task_info(classname: Optional[str] = None, task_filename: Optional[str] = None) -> None:
    """Render docstring and parameters for one task class, or all task classes.

    :param classname: Task class name, or ``None`` to show all tasks.
    :type classname: Optional[str]
    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    :raises CliUserError: If the task file is missing, contains no task classes,
        or ``classname`` does not exist in the task file.
    """
    d = resolve_defaults(task_filename, None)
    try:
        tasks = get_task_classes(d.task_file)
    except ValueError:
        raise CliUserError("No task classes found. Run 'b2luigi init' to create a starter project.")
    available = {cls.__name__: cls for cls in tasks}
    if classname is not None:
        validate_classnames([classname], available, hint_cmd="b2luigi tasks info")
        runner.render_task_help(available[classname])
    else:
        for cls in tasks:
            runner.render_task_help(cls)


@tasks_app.callback()
def tasks(
    ctx: typer.Context,
    task_filename: Annotated[
        Optional[str],
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
) -> None:
    """List all available task classes.

    :param ctx: Typer context (injected; not used directly).
    :type ctx: typer.Context
    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    """
    if ctx.invoked_subcommand is not None:
        return
    list_tasks(task_filename)


@tasks_app.command("info")
def task_info(
    classname: Annotated[
        Optional[str],
        typer.Argument(
            help="Task class name to show info for. Omit to show info for all tasks.",
            shell_complete=complete_task_names,
        ),
    ] = None,
    task_filename: Annotated[
        Optional[str],
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
) -> None:
    """Show docstring and parameters for a task class, or all task classes if omitted.

    :param classname: The task class name, or omit to show all.
    :type classname: Optional[str]
    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    """
    show_task_info(classname, task_filename)
