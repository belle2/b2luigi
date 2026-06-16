from typing import Annotated, List

from cyclopts import App, Parameter

from b2luigi.cli.utils import load_task_class, parse_kv_params, process_task_instance, resolve_defaults

batch_runner_app = App(
    name="batch-runner",
    help="Execute a single task by task ID (used internally by the batch system).",
    show=False,
)


@batch_runner_app.default
def batch_runner(
    classname: Annotated[
        str,
        Parameter(name=["--classname", "-c"], help="The task class name (task family)."),
    ],
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)."),
    ] = None,
    params: Annotated[
        List[str],
        Parameter(
            name=["--param", "-P"],
            help=(
                "Serialised task parameter as key=value (repeatable). "
                "Generated automatically by the batch submission; do not set by hand."
            ),
        ),
    ] = (),
):
    """Execute a specific task by class name and parameters.

    This command is called automatically by the b2luigi batch system when a task is
    submitted to a remote batch scheduler (HTCondor, SLURM, LSF, …).  The batch worker
    node runs this command to execute the task locally.

    **Do not invoke this command manually.**  Use ``b2luigi run`` instead.
    """
    d = resolve_defaults(task_filename, None)

    # Load the task class — this imports the module so any b2luigi.Task
    # subclasses defined there get registered in Luigi's global task registry.
    TaskClass = load_task_class(classname, d.task_file)

    # Reconstruct the exact task instance from the serialised parameter strings.
    # parse_kv_params applies JSON coercion; str() converts back to string so
    # Task.from_str_params() can apply each parameter's own .parse() method.
    str_params = {k: str(v) for k, v in parse_kv_params(params).items()}
    task_instance = TaskClass.from_str_params(str_params)

    # process_task_instance → b2luigi.process(..., batch_runner=True)
    # process() dispatches to runner.run_batch_worker() which sets
    # _dispatch_local_execution, creates output dirs, and handles callbacks.
    process_task_instance(task_instance, batch_runner=True)
