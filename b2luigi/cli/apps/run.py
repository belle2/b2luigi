from typing import Annotated, Any, Optional

import typer

import b2luigi

from b2luigi.cli.options import Params, ParamsFile, TaskFile
from b2luigi.cli.utils import (
    build_task_index,
    check_param_applicability,
    cli_error_boundary,
    complete_task_names,
    expand_parameters,
    load_parameters,
    parse_kv_params,
    process_task_instance,
    resolve_defaults,
)

run_app = typer.Typer(
    name="run",
    help="Run a task class from tasks.py",
    context_settings={"allow_interspersed_args": True},
)


def _make_wrapper_task(task_class: type, param_dicts: list[dict[str, Any]]) -> b2luigi.WrapperTask:
    """Build a dynamic :class:`b2luigi.WrapperTask` that requires ``task_class`` for each param dict.

    :param task_class: The task class to instantiate for each combination.
    :type task_class: type
    :param param_dicts: List of concrete parameter dicts, one per combination.
    :type param_dicts: list[dict[str, Any]]
    :returns: An instance of the dynamically-created wrapper task.
    :rtype: b2luigi.WrapperTask

    The wrapper is forced to ``batch_system = "local"`` since it exists only
    as an in-memory ``type()``-created class, never importable from
    ``tasks.py``. Under ``run --batch`` a global (non-``local``) batch system
    setting would otherwise submit the wrapper itself as a batch job, and the
    worker's ``--classname`` reconstruction would fail since the class cannot
    be found in any module. It does no real work (only aggregates
    ``requires()``), so running it in-process is always correct; the actual
    per-combination task instances are real, importable classes and are
    submitted to the batch system individually as normal.

    ``apptainer_image`` is pinned empty for the same reason, and the pin is not
    redundant. ``batch_system = "local"`` with an image set means "run locally,
    but inside the container", so :mod:`b2luigi.batch.workers` routes the
    ``local`` branch to ``ApptainerProcess`` — by design, and correct for real
    tasks, which keep running in the image. The wrapper is not a real task: it
    is b2luigi's own scaffolding, does no work beyond aggregating ``requires()``,
    and is importable from nowhere, so containerising it can only fail. It fails
    with ``Unknown task 'abc.<Name>Wrapper'``, because a ``type()``-created luigi
    task reports ``__module__ == "abc"`` — luigi's ``Register`` metaclass extends
    :class:`abc.ABCMeta`, and ``type.__new__`` takes the module from the calling
    frame, which is :mod:`abc` itself.

    Both pins rely on a task attribute outranking a global setting in
    :func:`~b2luigi.core.settings.get_setting`'s cascade.
    """

    def requires(self) -> list[b2luigi.Task]:
        return [task_class(**p) for p in param_dicts]

    wrapper_cls = type(
        f"{task_class.__name__}Wrapper",
        (b2luigi.WrapperTask,),
        {"requires": requires, "batch_system": "local", "apptainer_image": ""},
    )
    return wrapper_cls()


def run_task(
    task_class: type[b2luigi.Task],
    task_filename: str = "tasks.py",
    parameters_file: str = "parameters.py",
    overrides: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """Instantiate a resolved task class and execute it via :func:`process_task_instance`.

    When the ``parameters.py`` config contains
    :class:`~b2luigi.cli.parameter_generator.ParameterGenerator` or
    :class:`~b2luigi.cli.parameter_generator.ZippedParameterGenerator` values,
    the config is expanded into all combinations and a dynamic
    :class:`b2luigi.WrapperTask` is used to run them all.

    :param task_class: The already-resolved task class to run.
    :type task_class: type[b2luigi.Task]
    :param task_filename: Path to the Python file that defines the task classes.
    :type task_filename: str
    :param parameters_file: Path to the parameters file exposing a ``config`` dict.
    :type parameters_file: str
    :param overrides: Optional parameter overrides applied on top of the parameters file.
        A scalar override can pin a :class:`~b2luigi.cli.parameter_generator.ParameterGenerator`
        to a single value (e.g. via ``--param``).
    :type overrides: dict[str, Any] | None
    :param kwargs: Additional keyword arguments forwarded to :func:`process_task_instance`.
    """
    from b2luigi.cli.errors import CliUserError

    config = load_parameters(parameters_file)
    if overrides:
        config.update(overrides)

    param_dicts = expand_parameters(config)
    # Defensive: both generators reject empty inputs at construction, so this is
    # currently unreachable — kept as a guard against future expansion changes.
    if not param_dicts:
        raise CliUserError(
            f"Parameter expansion for '{task_class.__name__}' produced zero combinations. "
            "Check your ParameterGenerator or ZippedParameterGenerator values."
        )

    # Partition AFTER expansion: config keys are not parameter names — a
    # ZippedParameterGenerator hides under a sentinel key whose real parameter
    # names only exist once expanded. Derive the dropped set from ONE
    # combination, since every combination shares a key set; that is what makes
    # the warning fire once regardless of sweep size.
    accepted = check_param_applicability(
        param_dicts[0],
        [task_class],
        frozenset(overrides or {}),
        named=True,
        target=task_class.__name__,
    )
    param_dicts = [{k: v for k, v in pd.items() if k in accepted} for pd in param_dicts]

    if len(param_dicts) == 1:
        task_instance = task_class(**param_dicts[0])
    else:
        task_instance = _make_wrapper_task(task_class, param_dicts)

    # Pass the task file through as the user gave it. b2luigi's convention is that
    # an absolute path stays absolute and a relative one is resolved against
    # working_dir, which the batch wrapper cd's into. Absolutising here would bake
    # the submission host's layout into the worker command, breaking a relocating
    # working_dir: the path either does not exist on the node, or on a shared
    # filesystem points back at a different checkout of the project.
    process_task_instance(task_instance, task_file=task_filename, **kwargs)


@run_app.callback(invoke_without_command=True)
def run(
    classname: Annotated[
        str,
        typer.Argument(
            help="The name of the task class to run.",
            shell_complete=complete_task_names,
        ),
    ],
    task_filename: TaskFile = None,
    parameter_filename: ParamsFile = None,
    params: Params = None,
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
    workers: Annotated[
        Optional[int],
        typer.Option("--workers", help="Number of parallel luigi workers to use. Overrides the 'workers' setting."),
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
    :param workers: Number of parallel luigi workers to use, or None to fall back to the 'workers' setting (default 1).
    """
    with cli_error_boundary():
        d = resolve_defaults(task_filename, parameter_filename)
        index = build_task_index(d.task_file)
        task_class = index.resolve(classname, hint_cmd="b2luigi tasks")
        overrides = parse_kv_params(params or [])
        extra_kwargs: dict[str, Any] = {}
        if workers is not None:
            extra_kwargs["workers"] = workers
        run_task(
            task_class=task_class,
            task_filename=d.task_file,
            parameters_file=d.params_file,
            overrides=overrides,
            dry_run=dry_run,
            batch=batch,
            scheduler_host=scheduler_host,
            scheduler_port=scheduler_port,
            **extra_kwargs,
        )
