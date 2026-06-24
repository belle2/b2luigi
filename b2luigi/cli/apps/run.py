from typing import Annotated, Dict, List, Optional

import typer

import b2luigi

from b2luigi.cli.utils import (
    complete_task_names,
    expand_parameters,
    get_task_classes,
    load_parameters,
    load_task_class,
    parse_kv_params,
    process_task_instance,
    resolve_defaults,
    validate_classnames,
)

run_app = typer.Typer(
    name="run",
    help="Run a task class from tasks.py",
    context_settings={"allow_interspersed_args": True},
)


def _make_wrapper_task(task_class: type, param_dicts: list[dict]) -> b2luigi.WrapperTask:
    """Build a dynamic :class:`b2luigi.WrapperTask` that requires ``task_class`` for each param dict.

    :param task_class: The task class to instantiate for each combination.
    :type task_class: type
    :param param_dicts: List of concrete parameter dicts, one per combination.
    :type param_dicts: list[dict]
    :returns: An instance of the dynamically-created wrapper task.
    :rtype: b2luigi.WrapperTask
    """

    def requires(self):
        return [task_class(**p) for p in param_dicts]

    wrapper_cls = type(
        f"{task_class.__name__}Wrapper",
        (b2luigi.WrapperTask,),
        {"requires": requires},
    )
    return wrapper_cls()


def run_task(
    class_name: str,
    task_filename: str = "tasks.py",
    parameters_file: str = "parameters.py",
    overrides: Optional[Dict[str, object]] = None,
    **kwargs,
) -> None:
    """Instantiate a task class by name and execute it via :func:`process_task_instance`.

    When the ``parameters.py`` config contains
    :class:`~b2luigi.cli.parameter_generator.ParameterGenerator` or
    :class:`~b2luigi.cli.parameter_generator.ZippedParameterGenerator` values,
    the config is expanded into all combinations and a dynamic
    :class:`b2luigi.WrapperTask` is used to run them all.

    :param class_name: The name of the task class to run.
    :type class_name: str
    :param task_filename: Path to the Python file that defines the task classes.
    :type task_filename: str
    :param parameters_file: Path to the parameters file exposing a ``config`` dict.
    :type parameters_file: str
    :param overrides: Optional parameter overrides applied on top of the parameters file.
        A scalar override can pin a :class:`~b2luigi.cli.parameter_generator.ParameterGenerator`
        to a single value (e.g. via ``--param``).
    :type overrides: Optional[Dict[str, object]]
    :param kwargs: Additional keyword arguments forwarded to :func:`process_task_instance`.
    """
    task_class = load_task_class(class_name, task_filename)
    config = load_parameters(parameters_file)
    if overrides:
        config.update(overrides)

    param_dicts = expand_parameters(config)

    if len(param_dicts) == 1:
        task_instance = task_class(**param_dicts[0])
    else:
        task_instance = _make_wrapper_task(task_class, param_dicts)

    process_task_instance(task_instance, **kwargs)


@run_app.callback(invoke_without_command=True)
def run(
    classname: Annotated[
        str,
        typer.Argument(
            help="The name of the task class to run.",
            shell_complete=complete_task_names,
        ),
    ],
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

    :param classname: The name of the task class to run.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param params: Key=value parameter overrides (repeatable).
    :param dry_run: If ``True``, print which tasks would run without executing them.
    :param batch: If ``True``, submit to the configured batch system.
    :param scheduler_host: Host of a central Luigi scheduler.
    :param scheduler_port: Port of a central Luigi scheduler.
    """
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    validate_classnames([classname], available, hint_cmd="b2luigi tasks")
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
