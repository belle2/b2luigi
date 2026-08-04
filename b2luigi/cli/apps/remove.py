from typing import Annotated, Optional

import luigi
import typer

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.options import Params, ParamsFile, TaskFile, class_names_arg
from b2luigi.cli.utils import (
    build_task_list,
    parse_classnames,
    resolve_task_context,
    validate_classnames,
)
from b2luigi.core.settings import get_setting

remove_app = typer.Typer(
    name="remove",
    help="Remove output files of task(s).",
    context_settings={"allow_interspersed_args": True},
)


@remove_app.callback(invoke_without_command=True)
def remove(
    classnames: class_names_arg(
        "Task class name(s) to remove. Omit to remove outputs for all tasks in tasks.py."
    ) = None,
    task_filename: TaskFile = None,
    parameter_filename: ParamsFile = None,
    yes: Annotated[
        bool,
        typer.Option("-y", "--yes", help="Skip confirmation prompt."),
    ] = False,
    keep: Annotated[
        Optional[str],
        typer.Option("--keep", help="Comma-separated task class names whose outputs should NOT be removed."),
    ] = None,
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
            help="Also remove outputs of all tasks that the named task(s) require.",
        ),
    ] = False,
) -> None:
    """Remove output files of the named task(s).

    Without positional names removes outputs for all tasks in ``tasks.py``.
    By default removes only the named tasks; pass ``--with-requirements`` to also remove their transitive requirements.

    :param classnames: Task class name(s) to remove, or ``None`` to target all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param yes: If ``True``, skip the confirmation prompt.
    :param keep: Comma-separated task class names whose outputs should be preserved.
    :param params: Key=value overrides applied on top of the parameters file.
    :param direct: If ``True``, skip graph traversal (expert mode for large graphs).
    :param with_requirements: If ``True``, also remove the full requirement tree of the named tasks.
    :type with_requirements: bool

    .. note::
        Task names are passed as positional arguments. The ``-t``/``--task``
        option that existed in earlier versions has been removed.
    """
    ctx = resolve_task_context(task_filename, parameter_filename, params)
    available, merged_params, param_dicts = ctx.available, ctx.merged_params, ctx.param_dicts

    names = classnames
    keep_tasks = parse_classnames(keep)

    if names is None:
        target_names = list(available.keys())
    else:
        validate_classnames(names, available)
        target_names = names

    effective_direct = direct or bool(get_setting("direct_mode", default=False))

    task_list, unresolved = build_task_list(target_names, available, param_dicts, effective_direct)

    if unresolved:
        for name in sorted(unresolved):
            cls = available[name]
            filtered = {k: v for k, v in merged_params.items() if k in {n for n, _ in cls.get_params()}}
            try:
                cls(**filtered)
            except luigi.parameter.MissingParameterException as e:
                raise CliUserError(
                    f"Cannot instantiate {name} directly — {e}\n"
                    f"Add the missing parameter(s) with --param <key>=<value> or set in parameters.py."
                ) from e
        # Should be unreachable: build_task_list only populates unresolved when
        # try_instantiate returned None, which means MissingParameterException will
        # reproduce above. Guard against future exception hierarchy changes.
        raise CliUserError(
            f"Cannot instantiate task(s) {sorted(unresolved)!r} in direct mode. "
            f"Add missing parameters with --param <key>=<value> or set in parameters.py."
        )

    if with_requirements and names is not None:
        runner.remove_requirement_outputs(task_list, auto_confirm=yes, keep_tasks=keep_tasks)
    else:
        runner.remove_outputs(
            task_list,
            target_tasks=target_names,
            auto_confirm=yes,
            keep_tasks=keep_tasks,
        )
