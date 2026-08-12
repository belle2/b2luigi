from typing import Annotated, Any

import luigi
import typer

import b2luigi
from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.options import Params, ParamsFile, TaskFile, class_names_arg
from b2luigi.cli.utils import (
    check_param_applicability,
    cli_error_boundary,
    find_tasks_in_tree,
    get_root_tasks,
    resolve_task_context,
    try_instantiate,
)
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import task_iterator


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
    details: bool = False,
    paths_only: bool = False,
    links: bool = False,
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
    :param details: If ``True``, show the ``Output`` key-name column (and, for
        multi-instance tasks, the ``Params`` column). Hidden by default.
    :type details: bool
    :param paths_only: If ``True``, print one bare output path per line instead
        of rendering a table. Suitable for piping.
    :type paths_only: bool
    :param links: If ``True``, wrap local output paths in clickable terminal
        hyperlinks. Ignored under ``paths_only``.
    :type links: bool
    """
    ctx = resolve_task_context(task_filename, parameter_filename, params)
    index, param_dicts = ctx.index, ctx.param_dicts

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
        check_param_applicability(
            param_dicts[0], index.all_classes(), ctx.override_keys, named=False, target="any task"
        )
        runner.show_all_outputs(
            get_root_tasks(_all_instantiatable(index.all_classes())),
            details=details,
            paths_only=paths_only,
            links=links,
        )
        return

    target_classes = index.resolve_many(names)

    direct_instances: list[b2luigi.Task] = []
    seen_ids: set[str] = set()
    unresolvable: list[type[b2luigi.Task]] = []
    for cls in target_classes:
        found_any = False
        for pd in param_dicts:
            inst = try_instantiate(cls, pd)
            if inst is not None and inst.task_id not in seen_ids:
                seen_ids.add(inst.task_id)
                direct_instances.append(inst)
                found_any = True
        if not found_any:
            unresolvable.append(cls)

    considered = list(target_classes)
    if with_requirements:
        for inst in direct_instances:
            for task in task_iterator(inst):
                considered.append(type(task))
    check_param_applicability(
        param_dicts[0],
        considered,
        ctx.override_keys,
        named=True,
        target=", ".join(index.qualified_name(c) for c in target_classes),
    )

    if not unresolvable:
        if with_requirements:
            runner.show_all_outputs(
                direct_instances,
                show_required_by=True,
                details=details,
                paths_only=paths_only,
                links=links,
            )
        else:
            runner.show_task_outputs(direct_instances, details=details, paths_only=paths_only, links=links)
        return

    if effective_direct:
        for cls in unresolvable:
            _raise_unresolvable_error(index.qualified_name(cls), cls, param_dicts[0])
        raise AssertionError(f"Expected CliUserError from _raise_unresolvable_error, got none for: {unresolvable!r}")

    found = find_tasks_in_tree(
        set(target_classes),
        get_root_tasks(_all_instantiatable(index.all_classes())),
    )
    if with_requirements:
        runner.show_all_outputs(found, show_required_by=True, details=details, paths_only=paths_only, links=links)
    else:
        runner.show_task_outputs(found, details=details, paths_only=paths_only, links=links)


@show_app.callback(invoke_without_command=True)
def show(
    ctx: typer.Context,
    classnames: class_names_arg(
        "Task class name(s) to show. Omit to show the full dependency tree for all tasks."
    ) = None,
    task_filename: TaskFile = None,
    parameter_filename: ParamsFile = None,
    params: Params = None,
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
    details: Annotated[
        bool,
        typer.Option(
            "--details",
            help="Show the Output key-name column, and (for tasks resolving to multiple parameter "
            "combinations) the Params column. Both are hidden by default.",
        ),
    ] = False,
    paths_only: Annotated[
        bool,
        typer.Option(
            "--paths",
            help="Print one bare output path per line, with no table or styling. Suitable for piping.",
        ),
    ] = False,
    links: Annotated[
        bool,
        typer.Option(
            "--links",
            help=(
                "Make local output paths clickable via terminal hyperlinks. Ignored for remote targets, "
                "and note the link resolves on the machine your terminal runs on — not over SSH."
            ),
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
    :param details: If ``True``, show the Output key-name column and the Params column. Hidden by default.
    :type details: bool
    :param paths_only: If ``True``, print one bare output path per line instead
        of rendering a table. Suitable for piping.
    :type paths_only: bool
    :param links: If ``True``, wrap local output paths in clickable terminal
        hyperlinks. Ignored under ``paths_only``.
    :type links: bool
    """
    if ctx.invoked_subcommand is not None:
        return
    with cli_error_boundary():
        show_task(
            classnames=classnames,
            task_filename=task_filename,
            parameter_filename=parameter_filename,
            params=params,
            direct=direct,
            with_requirements=with_requirements,
            details=details,
            paths_only=paths_only,
            links=links,
        )
