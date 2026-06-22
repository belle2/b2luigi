from typing import Annotated, List, Optional

import luigi
import typer

import b2luigi
from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    complete_task_names,
    get_root_tasks,
    get_task_classes,
    load_parameters,
    parse_kv_params,
    resolve_defaults,
    try_instantiate,
    validate_classnames,
)
from b2luigi.core.settings import get_setting

show_app = typer.Typer(
    name="show",
    help="Show output files of task(s). Without a task name shows the full dependency tree.",
    context_settings={"allow_interspersed_args": True},
)


def show_task(
    classnames: Optional[List[str]] = None,
    task_filename: Optional[str] = None,
    parameter_filename: Optional[str] = None,
    with_dependents: bool = False,
    params: Optional[List[str]] = None,
    direct: bool = False,
) -> None:
    """Show output files of task(s).

    Without ``classnames`` shows the full dependency tree for all tasks in
    ``tasks.py``.  With ``classnames`` shows only the named tasks (and
    optionally their dependents).

    :param classnames: List of task class names, or ``None`` to show all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param with_dependents: If ``True``, also show tasks that depend on the specified tasks.
    :param params: Key=value overrides applied on top of the parameters file.
    :param direct: If ``True``, skip graph traversal (expert mode for large graphs).
    """
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    base_params = load_parameters(d.params_file)
    overrides = parse_kv_params(params or [])
    merged_params = {**base_params, **overrides}

    effective_direct = direct or bool(get_setting("direct_mode", default=False))
    names = classnames or None

    if names is None:
        # Show all: only instantiate classes whose required params are covered.
        # Non-root tasks are discovered via requires() during traversal.
        all_tasks = [inst for cls in available.values() if (inst := try_instantiate(cls, merged_params)) is not None]
        runner.show_all_outputs(get_root_tasks(all_tasks))
        return

    validate_classnames(names, available)

    if with_dependents:
        # show_dependents_outputs needs named task instances directly (for task_id lookup).
        # Always use all instantiatable roots for traversal, but named tasks must be resolvable.
        all_roots = [inst for cls in available.values() if (inst := try_instantiate(cls, merged_params)) is not None]
        named_instances: List[b2luigi.Task] = []
        for name in names:
            inst = try_instantiate(available[name], merged_params)
            if inst is None:
                cls = available[name]
                filtered = {k: v for k, v in merged_params.items() if k in {n for n, _ in cls.get_params()}}
                try:
                    cls(**filtered)
                except luigi.parameter.MissingParameterException as e:
                    raise CliUserError(
                        f"Cannot instantiate {name} directly — {e}\n"
                        f"--with-dependents requires a resolvable task instance.\n"
                        f"Add the missing parameter(s) with --param <key>=<value> or set in parameters.py."
                    ) from e
            else:
                named_instances.append(inst)
        runner.show_dependents_outputs(get_root_tasks(all_roots), named_instances)
        return

    # Try to directly instantiate all named tasks from the merged params.
    direct_instances: list[b2luigi.Task] = []
    unresolvable: list[str] = []
    for name in names:
        inst = try_instantiate(available[name], merged_params)
        if inst is not None:
            direct_instances.append(inst)
        else:
            unresolvable.append(name)

    if not unresolvable:
        # All named tasks resolved directly — show only those outputs, no traversal.
        runner.show_task_outputs(direct_instances)
        return

    if effective_direct:
        # Direct mode: never fall back to traversal — raise a clear error instead.
        for name in unresolvable:
            cls = available[name]
            filtered = {k: v for k, v in merged_params.items() if k in {n for n, _ in cls.get_params()}}
            try:
                cls(**filtered)
            except luigi.parameter.MissingParameterException as e:
                raise CliUserError(
                    f"Cannot instantiate {name} directly — {e}\n"
                    f"Add the missing parameter(s) with --param <key>=<value> or set in parameters.py."
                ) from e

    # Fallback: traverse from all instantiatable roots so the target can be discovered.
    all_roots = [inst for cls in available.values() if (inst := try_instantiate(cls, merged_params)) is not None]
    runner.show_all_outputs(get_root_tasks(all_roots))


@show_app.callback(invoke_without_command=True)
def show(
    ctx: typer.Context,
    classnames: Annotated[
        Optional[List[str]],
        typer.Argument(
            help="Task class name(s) to show. Omit to show the full dependency tree for all tasks.",
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
            help="Also show outputs of all tasks that depend on the specified task(s). Requires positional task name(s).",
        ),
    ] = False,
    params: Annotated[
        Optional[List[str]],
        typer.Option("--param", "-P", help="Override task parameters (repeatable): key=value."),
    ] = None,
    direct: Annotated[
        bool,
        typer.Option(
            "--direct",
            help="Skip dependency graph traversal. Requires all target task parameters to be "
            "resolvable from parameters.py or --param. Useful for large graphs.",
        ),
    ] = False,
) -> None:
    """Show output files of task(s).

    Without positional names shows the full dependency tree for all tasks in ``tasks.py``.
    With one or more names shows only those tasks (and optionally their dependents).

    :param ctx: Typer context (injected; used to detect subcommand invocation).
    :param classnames: Task class name(s) to show, or ``None`` to show all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param with_dependents: If ``True``, also show tasks that depend on the specified tasks.
    :param params: Key=value overrides applied on top of the parameters file.
    :param direct: If ``True``, skip graph traversal (expert mode for large graphs).
    """
    if ctx.invoked_subcommand is not None:
        return
    show_task(
        classnames=classnames,
        task_filename=task_filename,
        parameter_filename=parameter_filename,
        with_dependents=with_dependents,
        params=params,
        direct=direct,
    )
