from typing import Annotated, List, Optional

import typer

from b2luigi.cli.utils import load_task_class, parse_kv_params, process_task_instance, resolve_defaults

batch_runner_app = typer.Typer(
    name="batch-runner",
    help="Execute a single task by task ID (used internally by the batch system).",
)


@batch_runner_app.callback(invoke_without_command=True)
def batch_runner(
    ctx: typer.Context,
    classname: Annotated[
        str,
        typer.Option("--classname", "-c", help="The task class name (task family)."),
    ],
    task_filename: Annotated[
        Optional[str],
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)."),
    ] = None,
    params: Annotated[
        Optional[List[str]],
        typer.Option(
            "--param",
            "-P",
            help=(
                "Serialised task parameter as key=value (repeatable). "
                "Generated automatically by the batch submission; do not set by hand."
            ),
        ),
    ] = None,
) -> None:
    """Execute a specific task by class name and parameters.

    This command is called automatically by the b2luigi batch system when a task is
    submitted to a remote batch scheduler (HTCondor, SLURM, LSF, ...).  The batch worker
    node runs this command to execute the task locally.

    **Do not invoke this command manually.**  Use ``b2luigi run`` instead.

    :param ctx: Typer context (injected; not used directly).
    :param classname: The fully-qualified task class name to instantiate.
    :param task_filename: Path to the task definitions file.
    :param params: Serialised ``key=value`` parameter strings.
    """
    if ctx.invoked_subcommand is not None:
        return
    d = resolve_defaults(task_filename, None)

    TaskClass = load_task_class(classname, d.task_file)

    str_params = {k: str(v) for k, v in parse_kv_params(params or []).items()}
    task_instance = TaskClass.from_str_params(str_params)

    process_task_instance(task_instance, batch_runner=True)
