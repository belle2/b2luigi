from typing import Annotated, List, Optional

import typer

import b2luigi
from b2luigi.cli.options import ParamsFile, TaskFile
from b2luigi.cli.runner import _build_fast_req_task, _build_fast_task
from b2luigi.cli.utils import (
    cli_error_boundary,
    load_parameters,
    load_task_class,
    process_task_instance,
    resolve_defaults,
    split_kv_params,
)

batch_runner_app = typer.Typer(
    name="batch-runner",
    help="Execute a single task by task ID (used internally by the batch system).",
)


@batch_runner_app.callback(invoke_without_command=True)
def batch_runner(
    classname: Annotated[
        Optional[str],
        typer.Option("--classname", "-c", help="The task class name (task family)."),
    ] = None,
    task_filename: TaskFile = None,
    params_filename: ParamsFile = None,
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
    script: Annotated[
        Optional[str],
        typer.Option("--script", "-s", help="Path to the Python script to execute (test mode)."),
    ] = None,
    output_file: Annotated[
        Optional[str],
        typer.Option("--output-file", "-o", help="Output filename key (test mode)."),
    ] = None,
    input_file: Annotated[
        Optional[str],
        typer.Option("--input-file", "-i", help="Optional input filename key (test mode)."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Always re-run even if output exists (test mode)."),
    ] = False,
    literal_path: Annotated[
        bool,
        typer.Option(
            "--literal-path/--no-literal-path",
            help="Write -o to the literal path given (test mode). The submission host "
            "always sends one of the two flags explicitly; this default is never relied on.",
        ),
    ] = True,
    executable: Annotated[
        Optional[str],
        typer.Option(
            "--executable",
            help=(
                "Command used to run the script (test mode). Sent by the submission host only "
                "when it was given explicitly; otherwise the worker uses its own interpreter."
            ),
        ),
    ] = None,
    extra_arg: Annotated[
        Optional[List[str]],
        typer.Option(
            "--extra-arg",
            help="Extra argument forwarded to the script subprocess (test mode, repeatable).",
        ),
    ] = None,
) -> None:
    """Execute a specific task as a batch worker.

    In normal mode, supply ``--classname`` (and optionally ``--param`` / ``--task-file``)
    to load a task class from the task definitions file and run it locally.

    In test mode, supply ``--script`` and ``--output-file`` (mirroring ``b2luigi test``)
    to reconstruct and execute a ``FastTask`` without importing it from a module.  This is
    how ``b2luigi test --batch`` runs the task on the worker node.

    **Do not invoke this command manually.**  Use ``b2luigi run`` or ``b2luigi test`` instead.

    :param classname: The fully-qualified task class name to instantiate (normal mode).
    :param task_filename: Path to the task definitions file.
    :param params_filename: Path to the parameters file (normal mode).  Imported for its side
        effects only, so ``set_setting`` calls made there apply on the worker as they do on
        the submission host; its ``config`` is not used, the task is fully specified by
        ``--param``.  Optional, like on the submission host.
    :param params: Serialised ``key=value`` parameter strings (normal mode).
    :param script: Path to the Python script to execute (test mode).
    :param output_file: Output filename key passed to :meth:`add_to_output` (test mode).
    :param input_file: Optional input filename key (test mode).
    :param force: When ``True``, omit ``output()`` so the task always runs (test mode).
    :param literal_path: When True, -o writes to the literal path given (test mode).
    :param executable: Command used to run the script (test mode); defaults to the worker's own interpreter.
    :param extra_arg: Extra CLI arguments forwarded verbatim to the script subprocess (test mode).
    """
    if script is not None:
        if output_file is None:
            raise typer.BadParameter("--output-file is required in test mode", param_hint="'--output-file'")
        FastTask = _build_fast_task(
            exec_script=script,
            output=output_file,
            input_file=input_file,
            force=force,
            batch=False,  # worker node always runs locally; must never re-batch
            extra_args=extra_arg or [],
            literal_path=literal_path,
            executable=executable,
        )
        if input_file is not None:
            # Same wiring test_task applies on the submission host. FastReqTask is an
            # ExternalTask, so this never re-runs anything on the worker — it only
            # populates self.input() so get_input_file_name(input_file) can resolve.
            # Without it the worker raises KeyError on the input file name.
            FastTask = b2luigi.requires(_build_fast_req_task(input_file))(FastTask)
        process_task_instance(FastTask(), batch_runner=True)
    elif classname is not None:
        with cli_error_boundary():
            d = resolve_defaults(task_filename, params_filename)
            TaskClass = load_task_class(classname, d.task_file)
            # The submission host imported this file before instantiating the task; the
            # worker must too, or any set_setting() it makes (result_dir, ...) is silently
            # missing here. The config dict itself is irrelevant: --param carries the values.
            load_parameters(d.params_file)
        task_instance = TaskClass.from_str_params(split_kv_params(params or []))
        process_task_instance(task_instance, batch_runner=True)
    else:
        raise typer.BadParameter(
            "Provide either --classname (normal task) or --script (test mode).",
            param_hint="'--classname' / '--script'",
        )
