from typing import Annotated, Optional

import typer

from b2luigi.cli import runner
from b2luigi.cli.options import Params, ParamsFile, TaskFile, class_names_arg
from b2luigi.cli.utils import cli_error_boundary, resolve_removal_targets

remove_app = typer.Typer(
    name="remove",
    help="Remove output files of task(s).",
    context_settings={"allow_interspersed_args": True},
)


@remove_app.callback(invoke_without_command=True)
def remove(
    classnames: class_names_arg(
        "Task class name(s) to remove. Omit to remove outputs for all project tasks "
        "(everything b2luigi tasks lists, not just names in tasks.py)."
    ) = None,
    task_filename: TaskFile = None,
    parameter_filename: ParamsFile = None,
    yes: Annotated[
        bool,
        typer.Option("-y", "--yes", help="Skip confirmation prompt."),
    ] = False,
    keep: Annotated[
        Optional[str],
        typer.Option(
            "--keep",
            help="Comma-separated task class names whose outputs should NOT be removed. "
            "Names are validated; dotted module.Class names are accepted.",
        ),
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

    Without positional names removes outputs for every project task — not just those named
    in ``tasks.py``, but every transitively-discovered task too (see
    ``b2luigi tasks`` and the task-discovery docs). By default removes only the named tasks;
    pass ``--with-requirements`` to also remove their transitive requirements.

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

    .. note::
        After resolution, ``--keep`` filtering in the runner matches by class
        *name* — with ambiguous names it protects every same-named class,
        which is the safe direction.
    """
    with cli_error_boundary():
        targets = resolve_removal_targets(
            task_filename, parameter_filename, params, classnames, keep, direct, with_requirements
        )
        if with_requirements and targets.named:
            runner.remove_requirement_outputs(targets.task_list, auto_confirm=yes, keep_tasks=targets.keep_names)
        else:
            runner.remove_outputs(
                targets.task_list,
                target_tasks=targets.target_names,
                auto_confirm=yes,
                keep_tasks=targets.keep_names,
            )
