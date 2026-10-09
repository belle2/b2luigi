from typing import Annotated, Optional

import typer

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.options import Params, ParamsFile, TaskFile, class_names_arg
from b2luigi.cli.utils import cli_error_boundary, resolve_removal_targets

cleanup_app = typer.Typer(
    name="cleanup",
    help="Remove log files and batch submission files of task(s).",
    context_settings={"allow_interspersed_args": True},
)


def cleanup_methods(logs_only: bool, task_files_only: bool) -> tuple[tuple[str, ...], str]:
    """Translate the ``--logs-only`` / ``--task-files-only`` flags into runner arguments.

    :param logs_only: Only remove the log directories.
    :type logs_only: bool
    :param task_files_only: Only remove the task-file directories.
    :type task_files_only: bool
    :returns: The ``Task`` method names to call and the noun for the messages.
    :rtype: tuple[tuple[str, ...], str]
    :raises CliUserError: If both flags are given.
    """
    if logs_only and task_files_only:
        raise CliUserError("--logs-only and --task-files-only exclude each other; omit both to remove both.")
    if logs_only:
        return ("remove_log_files",), "log files"
    if task_files_only:
        return ("remove_task_files",), "task files"
    return ("remove_log_files", "remove_task_files"), "log and task files"


@cleanup_app.callback(invoke_without_command=True)
def cleanup(
    classnames: class_names_arg(
        "Task class name(s) to clean up. Omit to clean up all project tasks "
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
            help="Comma-separated task class names whose files should NOT be removed. "
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
            help="Also clean up all tasks that the named task(s) require.",
        ),
    ] = False,
    logs_only: Annotated[
        bool,
        typer.Option("--logs-only", help="Only remove the log directories (stdout, stderr, batch job logs)."),
    ] = False,
    task_files_only: Annotated[
        bool,
        typer.Option("--task-files-only", help="Only remove the task-file directories (submission files, wrappers)."),
    ] = False,
) -> None:
    """Remove the log and batch submission files of the named task(s).

    Calls :meth:`b2luigi.Task.remove_log_files` and :meth:`b2luigi.Task.remove_task_files`
    on every selected task; task outputs are never touched (see ``b2luigi remove`` for
    those). Task selection works exactly as for ``remove``: without positional names every
    project task is cleaned up, ``--with-requirements`` extends the named tasks to their
    requirement tree, ``--keep`` protects classes by name.

    :param classnames: Task class name(s) to clean up, or ``None`` to target all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param yes: If ``True``, skip the confirmation prompt.
    :param keep: Comma-separated task class names whose files should be preserved.
    :param params: Key=value overrides applied on top of the parameters file.
    :param direct: If ``True``, skip graph traversal (expert mode for large graphs).
    :param with_requirements: If ``True``, also clean up the full requirement tree of the named tasks.
    :param logs_only: Only remove log directories.
    :param task_files_only: Only remove task-file directories.
    :raises CliUserError: If ``--logs-only`` and ``--task-files-only`` are both given.
    """
    with cli_error_boundary():
        methods, noun = cleanup_methods(logs_only, task_files_only)
        targets = resolve_removal_targets(
            task_filename, parameter_filename, params, classnames, keep, direct, with_requirements
        )
        if with_requirements and targets.named:
            runner.remove_requirement_outputs(
                targets.task_list, auto_confirm=yes, keep_tasks=targets.keep_names, methods=methods, noun=noun
            )
        else:
            runner.remove_outputs(
                targets.task_list,
                target_tasks=targets.target_names,
                auto_confirm=yes,
                keep_tasks=targets.keep_names,
                methods=methods,
                noun=noun,
            )
