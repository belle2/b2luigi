"""CLI app for the ``b2luigi graph`` subcommand.

:Description: Renders the task dependency graph as a Rich terminal tree or
    Graphviz DOT output to stdout.
"""

from collections.abc import Iterable
from typing import Annotated

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


graph_app = typer.Typer(
    name="graph",
    help="Render the task dependency graph as a tree (terminal) or DOT (Graphviz).",
    context_settings={"allow_interspersed_args": True},
)


def graph_task(
    classnames: list[str] | None = None,
    task_filename: str | None = None,
    parameter_filename: str | None = None,
    params: list[str] | None = None,
    output_format: str = "tree",
    with_params: bool = False,
    show_status: bool = False,
) -> None:
    """Render the task dependency graph in the requested format.

    Resolves tasks from ``task_filename`` and ``parameter_filename``, scopes
    to ``classnames`` if provided, then delegates rendering to
    :func:`b2luigi.cli.runner.render_graph_tree` or
    :func:`b2luigi.cli.runner.render_graph_dot`.

    :param classnames: Task class names to scope the graph to, or ``None`` for
        the full graph.
    :type classnames: list[str] | None
    :param task_filename: Path to the task definitions file.
    :type task_filename: str | None
    :param parameter_filename: Path to the parameters file.
    :type parameter_filename: str | None
    :param params: Key=value parameter overrides applied on top of the
        parameters file.
    :type params: list[str] | None
    :param output_format: Output format — ``"tree"`` for Rich terminal tree or
        ``"dot"`` for Graphviz DOT to stdout.
    :type output_format: str
    :param with_params: If ``True``, include parameter values on each node.
    :type with_params: bool
    :param show_status: If ``True``, check and display output completion status
        on each node.
    :type show_status: bool
    """
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    base_params = load_parameters(d.params_file)
    overrides = parse_kv_params(params or [])
    merged_params = {**base_params, **overrides}
    param_dicts = expand_parameters(merged_params)

    def _all_instantiatable(classes: Iterable[type[b2luigi.Task]]) -> list[b2luigi.Task]:
        seen: set[str] = set()
        result: list[b2luigi.Task] = []
        for cls in classes:
            for pd in param_dicts:
                inst = try_instantiate(cls, pd)
                if inst is not None and inst.task_id not in seen:
                    seen.add(inst.task_id)
                    result.append(inst)
        return result

    names = classnames

    if names is None:
        root_tasks = get_root_tasks(_all_instantiatable(available.values()))
    else:
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

        if unresolvable:
            root_tasks = find_tasks_in_tree(
                set(names),
                get_root_tasks(_all_instantiatable(available.values())),
            )
        else:
            root_tasks = direct_instances

    if output_format == "dot":
        raise CliUserError("DOT format is not yet implemented. Use --format tree (or omit --format).")
    runner.render_graph_tree(root_tasks, show_params=with_params, show_status=show_status)


@graph_app.callback(invoke_without_command=True)
def graph(
    ctx: typer.Context,
    classnames: Annotated[
        list[str] | None,
        typer.Argument(
            help="Task class name(s) to scope the graph to. Omit for the full graph.",
            shell_complete=complete_task_names,
        ),
    ] = None,
    task_filename: Annotated[
        str | None,
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)."),
    ] = None,
    parameter_filename: Annotated[
        str | None,
        typer.Option("--params-file", "-p", help="Parameters file (or $B2LUIGI_PARAMS_FILE)."),
    ] = None,
    params: Annotated[
        list[str] | None,
        typer.Option("--param", "-P", help="Override task parameters (repeatable): key=value."),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            help="Output format: 'tree' (Rich terminal, default) or 'dot' (Graphviz DOT to stdout).",
        ),
    ] = "tree",
    with_params: Annotated[
        bool,
        typer.Option("--params", help="Include parameter values on each node."),
    ] = False,
    show_status: Annotated[
        bool,
        typer.Option("--status", "-s", help="Check and display output completion status on each node."),
    ] = False,
) -> None:
    """Render the task dependency graph.

    Without positional names renders the full dependency graph. With one or
    more names scopes the graph to those tasks' subtrees.

    :param ctx: Typer context (injected; used to detect subcommand invocation).
    :param classnames: Task class name(s) to scope the graph to, or ``None``
        for the full graph.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param params: Key=value overrides applied on top of the parameters file.
    :param output_format: Output format — ``"tree"`` or ``"dot"``.
    :param with_params: If ``True``, include parameter values on each node.
    :param show_status: If ``True``, display output completion status on each
        node.
    :type ctx: typer.Context
    :type classnames: list[str] | None
    :type task_filename: str | None
    :type parameter_filename: str | None
    :type params: list[str] | None
    :type output_format: str
    :type with_params: bool
    :type show_status: bool
    """
    if ctx.invoked_subcommand is not None:
        return
    graph_task(
        classnames=classnames,
        task_filename=task_filename,
        parameter_filename=parameter_filename,
        params=params,
        output_format=output_format,
        with_params=with_params,
        show_status=show_status,
    )
