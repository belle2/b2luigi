from typing import Annotated, Dict, List, Optional

import typer

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    complete_task_names,
    get_task_classes,
    get_task_instance,
    parse_kv_params,
    process_task_instance,
    resolve_defaults,
    validate_classnames,
)

run_app = typer.Typer(name="run", help="Run a task class from tasks.py")


def run_task(
    class_name: str,
    task_filename: str = "tasks.py",
    parameters_file: str = "parameters.py",
    overrides: Optional[Dict[str, object]] = None,
    **kwargs,
) -> None:
    """Instantiate a task class by name and execute it via :func:`process_task_instance`.

    :param class_name: The name of the task class to run.
    :type class_name: str
    :param task_filename: Path to the Python file that defines the task classes.
    :type task_filename: str
    :param parameters_file: Path to the parameters file exposing a ``config`` dict.
    :type parameters_file: str
    :param overrides: Optional parameter overrides applied on top of the parameters file.
    :type overrides: Optional[Dict[str, object]]
    :param kwargs: Additional keyword arguments forwarded to :func:`process_task_instance`.
    """
    task_instance = get_task_instance(class_name, task_filename, parameters_file, overrides)
    process_task_instance(task_instance, **kwargs)


@run_app.callback(invoke_without_command=True)
def run(
    ctx: typer.Context,
    classname: Annotated[
        Optional[str],
        typer.Option(
            "--task",
            "-t",
            "--classname",
            "-c",
            help="The name of the task class to run",
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
    params: Annotated[
        Optional[List[str]],
        typer.Option(
            "--param",
            "-P",
            help="Override task parameters (repeatable): key=value. Values are parsed as JSON when possible.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry", "-d", help="Instead of running the task(s), write out which tasks will be executed."),
    ] = False,
    batch: Annotated[
        bool,
        typer.Option("--batch", "-b", help="Submit tasks to the configured batch system instead of running locally."),
    ] = False,
    scheduler_host: Annotated[
        Optional[str],
        typer.Option(
            "--scheduler-host", help="Host of a central luigi scheduler to connect to (instead of running locally)"
        ),
    ] = None,
    scheduler_port: Annotated[
        Optional[int],
        typer.Option(
            "--scheduler-port", help="Port of a central luigi scheduler to connect to (instead of running locally)"
        ),
    ] = None,
) -> None:
    """Run a task class from tasks.py.

    :param ctx: Typer context (injected; not used directly).
    :param classname: The name of the task class to run.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param params: Key=value parameter overrides (repeatable).
    :param dry_run: If ``True``, print which tasks would run without executing them.
    :param batch: If ``True``, submit to the configured batch system.
    :param scheduler_host: Host of a central Luigi scheduler.
    :param scheduler_port: Port of a central Luigi scheduler.
    """
    if ctx.invoked_subcommand is not None:
        return
    if classname is None:
        raise typer.BadParameter("--task / -t is required", param_hint="'--task'")
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}

    validate_classnames([classname], available)

    overrides = parse_kv_params(params or [])
    run_task(
        class_name=classname,
        task_filename=d.task_file,
        parameters_file=d.params_file,
        overrides=overrides,
        dry_run=dry_run,
        batch=batch,
        scheduler_host=scheduler_host,
        scheduler_port=scheduler_port,
    )


@run_app.command("list")
def list_tasks(
    task_filename: Annotated[
        Optional[str],
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
) -> None:
    """List available task classes."""
    d = resolve_defaults(task_filename, None)
    tasks = get_task_classes(d.task_file)
    runner.render_task_list(tasks)


@run_app.command("help")
def task_help(
    classname: Annotated[
        str,
        typer.Argument(help="Task class name to show help for."),
    ],
    task_filename: Annotated[
        Optional[str],
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
) -> None:
    """Show help for a specific task class."""
    d = resolve_defaults(task_filename, None)
    tasks = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    cls = tasks.get(classname)
    if not cls:
        raise CliUserError(f"Unknown task '{classname}'. Use 'b2luigi run list'.")
    runner.render_task_help(cls)
