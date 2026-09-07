"""CLI sub-app for listing and inspecting available task classes.

:Description: Implements ``b2luigi tasks`` (list all tasks) and
    ``b2luigi tasks info [CLASSNAME]`` (show docstring and parameters).
"""

from typing import Annotated, Optional

import typer

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.options import TaskFile
from b2luigi.cli.utils import (
    Defaults,
    TaskIndex,
    build_task_index,
    cli_error_boundary,
    complete_task_names,
    resolve_defaults,
)

tasks_app = typer.Typer(
    name="tasks",
    help="List and inspect available task classes.",
)


def _load_task_index(task_filename: Optional[str]) -> tuple[TaskIndex, Defaults]:
    """Build the task index for *task_filename*, raising :class:`CliUserError` if it is empty.

    :param task_filename: Path to the task definitions file, or ``None`` for defaults.
    :type task_filename: Optional[str]
    :returns: Tuple of (task index, resolved defaults object).
    :raises CliUserError: If the file is missing or contains no ``b2luigi.Task`` subclasses.
    """
    d = resolve_defaults(task_filename, None)
    index = build_task_index(d.task_file)
    if not index.all_classes():
        raise CliUserError(
            f"No b2luigi task classes found in '{d.task_file}'. Ensure your classes subclass b2luigi.Task."
        )
    return index, d


def list_tasks(task_filename: Optional[str] = None) -> None:
    """Render a table of all available task classes.

    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    :raises CliUserError: If the task file is missing or contains no task classes.
    """
    index, _ = _load_task_index(task_filename)
    entries = [(cls, index.display_module(cls)) for cls in index.all_classes()]
    runner.render_task_list(entries)


def show_task_info(classname: Optional[str] = None, task_filename: Optional[str] = None) -> None:
    """Render docstring and parameters for one task class, or all task classes.

    :param classname: Task class name, or ``None`` to show all tasks.
    :type classname: Optional[str]
    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    :raises CliUserError: If the task file is missing, contains no task classes,
        or ``classname`` does not exist in the task file.
    """
    index, _ = _load_task_index(task_filename)
    if classname is not None:
        cls = index.resolve(classname, hint_cmd="b2luigi tasks info")
        runner.render_task_help(cls)
    else:
        for cls in index.all_classes():
            runner.render_task_help(cls)


@tasks_app.callback(invoke_without_command=True)
def tasks(
    ctx: typer.Context,
    task_filename: TaskFile = None,
) -> None:
    """List all available task classes.

    :param ctx: Typer context (injected; not used directly).
    :type ctx: typer.Context
    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    """
    if ctx.invoked_subcommand is not None:
        return
    with cli_error_boundary():
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
    task_filename: TaskFile = None,
) -> None:
    """Show docstring and parameters for a task class, or all task classes if omitted.

    :param classname: The task class name, or omit to show all.
    :type classname: Optional[str]
    :param task_filename: Path to the task definitions file.
    :type task_filename: Optional[str]
    """
    with cli_error_boundary():
        show_task_info(classname, task_filename)
