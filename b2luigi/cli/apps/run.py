from cyclopts import App, Parameter
from typing import Annotated, Dict, List, Optional

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    get_task_classes,
    get_task_instance,
    parse_kv_params,
    process_task_instance,
    resolve_defaults,
    validate_classnames,
)

run_app = App(name="run", help="Run a task class from tasks.py")


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


@run_app.default
def run(
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
    params: Annotated[
        List[str],
        Parameter(
            name=["--param", "-P"],
            help="Override task parameters (repeatable): key=value. Values are parsed as JSON when possible.",
        ),
    ] = (),
    dry_run: Annotated[
        bool,
        Parameter(name=["--dry", "-d"], help="Instead of running the task(s), write out which tasks will be executed."),
    ] = False,
    batch: Annotated[
        bool,
        Parameter(
            name=["--batch", "-b"], help="Submit tasks to the configured batch system instead of running locally."
        ),
    ] = False,
    scheduler_host: Annotated[
        Optional[str],
        Parameter(
            name="--scheduler-host", help="Host of a central luigi scheduler to connect to (instead of running locally)"
        ),
    ] = None,
    scheduler_port: Annotated[
        Optional[int],
        Parameter(
            name="--scheduler-port", help="Port of a central luigi scheduler to connect to (instead of running locally)"
        ),
    ] = None,
):
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}

    validate_classnames([classname], available)

    overrides = parse_kv_params(params)
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


@run_app.command
def list(
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
):
    """List available task classes."""
    d = resolve_defaults(task_filename, None)
    tasks = get_task_classes(d.task_file)
    runner.render_task_list(tasks)


@run_app.command(name="help")
def task_help(
    classname: str,
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
):
    """Show help for a specific task class."""
    d = resolve_defaults(task_filename, None)
    tasks = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    cls = tasks.get(classname)
    if not cls:
        raise CliUserError(f"Unknown task '{classname}'. Use 'b2luigi run list'.")
    runner.render_task_help(cls)
