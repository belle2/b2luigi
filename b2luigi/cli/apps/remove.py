from cyclopts import App, Parameter
from typing import Annotated, List

from b2luigi.cli import runner
from b2luigi.cli.utils import (
    get_task_classes,
    load_parameters,
    parse_classnames,
    parse_kv_params,
    resolve_defaults,
    validate_classnames,
)

remove_app = App(name="remove", help="Remove output files of task(s).")


@remove_app.default
def remove(
    classnames: Annotated[
        str | None,
        Parameter(
            name=["--task", "-t"],
            help="Task class name(s) to remove, comma-separated (e.g. MyTask,MyOtherTask). "
            "Omit to remove outputs for all tasks in tasks.py.",
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
    yes: Annotated[
        bool,
        Parameter(name=["-y", "--yes"], help="Skip confirmation prompt."),
    ] = False,
    with_dependents: Annotated[
        bool,
        Parameter(name="--with-dependents", help="Also remove outputs of tasks that depend on the named task(s)."),
    ] = False,
    keep: Annotated[
        str | None,
        Parameter(name="--keep", help="Comma-separated task class names whose outputs should NOT be removed."),
    ] = None,
    params: Annotated[
        List[str],
        Parameter(
            name=["--param", "-P"],
            help="Override task parameters (repeatable): key=value. Values are parsed as JSON when possible.",
        ),
    ] = (),
):
    """Remove output files of the named task(s).

    Without ``-t`` removes outputs for all tasks in ``tasks.py``.
    By default only the named tasks are removed (not their dependents); pass
    ``--with-dependents`` to also remove tasks that depend on the named ones.

    :param classnames: Comma-separated task class names, or ``None`` to target all.
    :param task_filename: Path to the task definitions file.
    :param parameter_filename: Path to the parameters file.
    :param yes: If ``True``, skip the confirmation prompt.
    :param with_dependents: If ``True``, also remove dependent tasks' outputs.
    :param keep: Comma-separated task class names whose outputs should be preserved.
    :param params: Key=value overrides applied on top of the parameters file.
    """
    d = resolve_defaults(task_filename, parameter_filename)
    available = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    base_params = load_parameters(d.params_file)
    overrides = parse_kv_params(params)
    merged_params = {**base_params, **overrides}

    names = parse_classnames(classnames)
    keep_tasks = parse_classnames(keep)

    if names is None:
        target_names = list(available.keys())
    else:
        validate_classnames(names, available)
        target_names = names

    # All task instances are needed so remove_outputs can traverse the full dependency graph.
    task_list = [cls(**merged_params) for cls in available.values()]

    runner.remove_outputs(
        task_list,
        target_tasks=target_names,
        only=not with_dependents,
        auto_confirm=yes,
        keep_tasks=keep_tasks,
    )
