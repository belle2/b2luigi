from typing import Annotated, Any

import luigi
import typer

import b2luigi
from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    complete_task_names,
    expand_parameters,
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
    with_dependents: bool = False,
    params: list[str] | None = None,
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
        # Show all: only instantiate classes whose required params are covered.
        # Non-root tasks are discovered via requires() during traversal.
        runner.show_all_outputs(get_root_tasks(_all_instantiatable(available.values())))
        return

    validate_classnames(names, available)

    if with_dependents:
        # show_dependents_outputs needs named task instances directly (for task_id lookup).
        # Always use all instantiatable roots for traversal, but named tasks must be resolvable.
        all_roots = _all_instantiatable(available.values())
        named_instances: list[b2luigi.Task] = []
        named_seen: set[str] = set()
        for name in names:
            found_any = False
            for pd in param_dicts:
                inst = try_instantiate(available[name], pd)
                if inst is not None and inst.task_id not in named_seen:
                    named_seen.add(inst.task_id)
                    named_instances.append(inst)
                    found_any = True
            if not found_any:
                _raise_unresolvable_error(
                    name,
                    available[name],
                    param_dicts[0],
                    hint="--with-dependents requires a resolvable task instance.",
                )
        runner.show_dependents_outputs(get_root_tasks(all_roots), named_instances)
        return

    # Try to directly instantiate all named tasks from the expanded params.
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
        # All named tasks resolved directly — show only those outputs, no traversal.
        runner.show_task_outputs(direct_instances)
        return

    if effective_direct:
        # Direct mode: never fall back to traversal — raise a clear error instead.
        for name in unresolvable:
            _raise_unresolvable_error(name, available[name], param_dicts[0])
        # Should be unreachable: _raise_unresolvable_error raises for any unresolved name.
        raise AssertionError(f"Expected CliUserError from _raise_unresolvable_error, got none for: {unresolvable!r}")

    # Fallback: traverse from all instantiatable roots so the target can be discovered.
    runner.show_all_outputs(get_root_tasks(_all_instantiatable(available.values())))


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
    with_dependents: Annotated[
        bool,
        typer.Option(
            "--with-dependents",
            help="Also show outputs of all tasks that depend on the specified task(s). Requires positional task name(s).",
        ),
    ] = False,
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
