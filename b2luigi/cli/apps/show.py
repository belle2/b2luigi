from cyclopts import App, Parameter
from typing import Annotated, List

from b2luigi.cli import runner
from b2luigi.cli.utils import (
    get_root_tasks,
    get_task_classes,
    load_parameters,
    parse_classnames,
    parse_kv_params,
    resolve_defaults,
    validate_classnames,
)

show_app = App(name="show", help="Show output files of task(s). Without a task name shows the full dependency tree.")


@show_app.default
def show(
    classnames: Annotated[
        str | None,
        Parameter(
            name=["--task", "-t"],
            help="Task class name(s) to show, comma-separated (e.g. MyTask,MyOtherTask). "
            "Omit to show the full dependency tree for all tasks in tasks.py.",
        ),
    ] = None,
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
    parameter_filename: Annotated[
        str | None,
        Parameter(name=["--params-file", "-p"], help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
    with_dependents: Annotated[
        bool,
        Parameter(
            name="--with-dependents",
            help="Also show outputs of all tasks that depend on the specified task(s). Requires -t/--task.",
        ),
    ] = False,
    params: Annotated[
        List[str],
        Parameter(
            name=["--param", "-P"],
            help="Override task parameters (repeatable): key=value. Values are parsed as JSON when possible.",
        ),
    ] = (),
):
    """Show output files of task(s).

    Without ``-t`` shows the full dependency tree for all tasks in ``tasks.py``.
    With ``-t`` shows only the named tasks (and optionally their dependents).

    :param classnames: Comma-separated task class names, or ``None`` to show all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param with_dependents: If ``True``, also show tasks that depend on the specified tasks.
    :param params: Key=value overrides applied on top of the parameters file.
    """
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    base_params = load_parameters(d.params_file)
    overrides = parse_kv_params(params)
    merged_params = {**base_params, **overrides}

    names = parse_classnames(classnames)

    if names is None:
        all_tasks = [cls(**merged_params) for cls in available.values()]
        runner.show_all_outputs(get_root_tasks(all_tasks))
        return

    validate_classnames(names, available)
    task_list = [available[name](**merged_params) for name in names]

    if with_dependents:
        all_tasks = [cls(**merged_params) for cls in available.values()]
        runner.show_dependents_outputs(get_root_tasks(all_tasks), task_list)
    else:
        runner.show_task_outputs(task_list)
