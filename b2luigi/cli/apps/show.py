from typing import Annotated, List, Optional

import typer

from b2luigi.cli import runner
from b2luigi.cli.utils import (
    complete_task_names,
    get_root_tasks,
    get_task_classes,
    load_parameters,
    parse_classnames,
    parse_kv_params,
    resolve_defaults,
    validate_classnames,
)

show_app = typer.Typer(
    name="show", help="Show output files of task(s). Without a task name shows the full dependency tree."
)


def show_task(
    classnames: Optional[str] = None,
    task_filename: Optional[str] = None,
    parameter_filename: Optional[str] = None,
    with_dependents: bool = False,
    params: Optional[List[str]] = None,
) -> None:
    """Show output files of task(s).

    Without ``classnames`` shows the full dependency tree for all tasks in
    ``tasks.py``.  With ``classnames`` shows only the named tasks (and
    optionally their dependents).

    :param classnames: Comma-separated task class names, or ``None`` to show all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param with_dependents: If ``True``, also show tasks that depend on the specified tasks.
    :param params: Key=value overrides applied on top of the parameters file.
    """
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    base_params = load_parameters(d.params_file)
    overrides = parse_kv_params(params or [])
    merged_params = {**base_params, **overrides}

    names = parse_classnames(classnames)

    if names is None:
        all_tasks = [cls(**merged_params) for cls in available.values()]
        runner.show_all_outputs(get_root_tasks(all_tasks))
        return

    validate_classnames(names, available)
    task_list = [available[name](**merged_params) for name in names]

    if with_dependents:
        all_tasks = [cls(**merged_params) for cls in available.values()]
        runner.show_dependents_outputs(get_root_tasks(all_tasks), task_list)
    else:
        runner.show_task_outputs(task_list)


@show_app.callback(invoke_without_command=True)
def show(
    ctx: typer.Context,
    classnames: Annotated[
        Optional[str],
        typer.Option(
            "--task",
            "-t",
            help="Task class name(s) to show, comma-separated (e.g. MyTask,MyOtherTask). "
            "Omit to show the full dependency tree for all tasks in tasks.py.",
            shell_complete=complete_task_names,
        ),
    ] = None,
    task_filename: Annotated[
        Optional[str],
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
    parameter_filename: Annotated[
        Optional[str],
        typer.Option("--params-file", "-p", help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
    with_dependents: Annotated[
        bool,
        typer.Option(
            "--with-dependents",
            help="Also show outputs of all tasks that depend on the specified task(s). Requires -t/--task.",
        ),
    ] = False,
    params: Annotated[
        Optional[List[str]],
        typer.Option("--param", "-P", help="Override task parameters (repeatable): key=value."),
    ] = None,
) -> None:
    """Show output files of task(s).

    Without ``-t`` shows the full dependency tree for all tasks in ``tasks.py``.
    With ``-t`` shows only the named tasks (and optionally their dependents).

    :param ctx: Typer context (injected; not used directly).
    :param classnames: Comma-separated task class names, or ``None`` to show all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param with_dependents: If ``True``, also show tasks that depend on the specified tasks.
    :param params: Key=value overrides applied on top of the parameters file.
    """
    if ctx.invoked_subcommand is not None:
        return
    show_task(
        classnames=classnames,
        task_filename=task_filename,
        parameter_filename=parameter_filename,
        with_dependents=with_dependents,
        params=params,
    )
