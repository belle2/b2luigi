from typing import Annotated, Any

import luigi
import typer

import b2luigi
from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    complete_task_names,
    expand_parameters,
    find_tasks_in_tree,
    get_root_tasks,
    get_task_classes,
    load_parameters,
    parse_kv_params,
    resolve_defaults,
    try_instantiate,
    validate_classnames,
)
from b2luigi.core.settings import get_setting


def _raise_unresolvable_error(
    name: str, cls: type[b2luigi.Task], merged_params: dict[str, Any], hint: str = ""
) -> None:
    """Raise a :class:`CliUserError` explaining which parameter is missing.

    Attempts to instantiate ``cls`` with its filtered params to obtain the
    :class:`luigi.parameter.MissingParameterException` message, then re-raises
    as a ``CliUserError`` with an actionable hint.

    :param name: Task class name (used in the error message).
    :param cls: The task class that failed to instantiate.
    :param merged_params: Combined params dict (unfiltered).
    :param hint: Optional extra sentence inserted before the ``--param`` hint.
    """
    filtered = {k: v for k, v in merged_params.items() if k in {n for n, _ in cls.get_params()}}
    try:
        cls(**filtered)
    except luigi.parameter.MissingParameterException as e:
        msg = f"Cannot instantiate {name} directly — {e}\n"
        if hint:
            msg += hint + "\n"
        msg += "Add the missing parameter(s) with --param <key>=<value> or set in parameters.py."
        raise CliUserError(msg) from e


show_app = typer.Typer(
    name="show",
    help="Show output files of task(s). Without a task name shows the full dependency tree.",
    context_settings={"allow_interspersed_args": True},
)


def show_task(
    classnames: list[str] | None = None,
    task_filename: str | None = None,
    parameter_filename: str | None = None,
    params: list[str] | None = None,
    direct: bool = False,
    with_requirements: bool = False,
) -> None:
    """Show output files of task(s).

    Without ``classnames`` shows the full dependency tree for all tasks in
    ``tasks.py``.  With ``classnames`` shows only the named tasks.

    :param classnames: List of task class names, or ``None`` to show all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param params: Key=value overrides applied on top of the parameters file.
    :param direct: If ``True``, skip graph traversal (expert mode for large graphs).
    :param with_requirements: If ``True``, also show outputs of all tasks that the specified tasks require.
    :type with_requirements: bool
    """
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    base_params = load_parameters(d.params_file)
    overrides = parse_kv_params(params or [])
    merged_params = {**base_params, **overrides}
    param_dicts = expand_parameters(merged_params)

    effective_direct = direct or bool(get_setting("direct_mode", default=False))
    names = classnames

    def _all_instantiatable(classes) -> list[b2luigi.Task]:
        seen: set[str] = set()
        result: list[b2luigi.Task] = []
        for cls in classes:
            for pd in param_dicts:
                inst = try_instantiate(cls, pd)
                if inst is not None and inst.task_id not in seen:
                    seen.add(inst.task_id)
                    result.append(inst)
        return result

    if names is None:
        runner.show_all_outputs(get_root_tasks(_all_instantiatable(available.values())))
        return

    validate_classnames(names, available)

    direct_instances: list[b2luigi.Task] = []
    seen_ids: set[str] = set()
    unresolvable: list[str] = []
    for name in names:
        found_any = False
        for pd in param_dicts:
            inst = try_instantiate(available[name], pd)
            if inst is not None and inst.task_id not in seen_ids:
                seen_ids.add(inst.task_id)
                direct_instances.append(inst)
                found_any = True
        if not found_any:
            unresolvable.append(name)

    if not unresolvable:
        if with_requirements:
            runner.show_all_outputs(direct_instances, show_required_by=True)
        else:
            runner.show_task_outputs(direct_instances)
        return

    if effective_direct:
        for name in unresolvable:
            _raise_unresolvable_error(name, available[name], param_dicts[0])
        raise AssertionError(f"Expected CliUserError from _raise_unresolvable_error, got none for: {unresolvable!r}")

    found = find_tasks_in_tree(
        set(names),
        get_root_tasks(_all_instantiatable(available.values())),
    )
    if with_requirements:
        runner.show_all_outputs(found, show_required_by=True)
    else:
        runner.show_task_outputs(found)


@show_app.callback(invoke_without_command=True)
def show(
    ctx: typer.Context,
    classnames: Annotated[
        list[str] | None,
        typer.Argument(
            help="Task class name(s) to show. Omit to show the full dependency tree for all tasks.",
            shell_complete=complete_task_names,
        ),
    ] = None,
    task_filename: Annotated[
        str | None,
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
    parameter_filename: Annotated[
        str | None,
        typer.Option("--params-file", "-p", help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
    params: Annotated[
        list[str] | None,
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
    with_requirements: Annotated[
        bool,
        typer.Option(
            "--with-requirements",
            help="Also show outputs of all tasks that the specified task(s) require. Requires positional task name(s).",
        ),
    ] = False,
) -> None:
    """Show output files of task(s).

    Without positional names shows the full dependency tree for all tasks in ``tasks.py``.
    With one or more names shows only those tasks.

    :param ctx: Typer context (injected; used to detect subcommand invocation).
    :param classnames: Task class name(s) to show, or ``None`` to show all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param params: Key=value overrides applied on top of the parameters file.
    :param direct: If ``True``, skip graph traversal (expert mode for large graphs).
    :param with_requirements: If ``True``, also show the full requirement tree of the specified tasks.
    :type with_requirements: bool
    """
    if ctx.invoked_subcommand is not None:
        return
    show_task(
        classnames=classnames,
        task_filename=task_filename,
        parameter_filename=parameter_filename,
        params=params,
        direct=direct,
        with_requirements=with_requirements,
    )
