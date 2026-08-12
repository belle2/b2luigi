from typing import Annotated, Optional

import luigi
import typer

from b2luigi.cli import runner
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.options import Params, ParamsFile, TaskFile, class_names_arg
from b2luigi.cli.utils import (
    build_task_list,
    check_param_applicability,
    cli_error_boundary,
    parse_classnames,
    resolve_task_context,
)
from b2luigi.core.settings import get_setting
from b2luigi.core.utils import task_iterator

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
        ctx = resolve_task_context(task_filename, parameter_filename, params)
        index, merged_params, param_dicts = ctx.index, ctx.merged_params, ctx.param_dicts

        names = classnames
        keep_names = parse_classnames(keep)
        keep_classes = index.resolve_many(keep_names) if keep_names else None

        if names is None:
            target_classes = index.all_classes()
        else:
            target_classes = index.resolve_many(names)

        effective_direct = direct or bool(get_setting("direct_mode", default=False))

        task_list, unresolved = build_task_list(target_classes, index, param_dicts, effective_direct)

        if unresolved:
            for cls in sorted(unresolved, key=index.qualified_name):
                filtered = {k: v for k, v in merged_params.items() if k in {n for n, _ in cls.get_params()}}
                try:
                    cls(**filtered)
                except luigi.parameter.MissingParameterException as e:
                    raise CliUserError(
                        f"Cannot instantiate {index.qualified_name(cls)} directly — {e}\n"
                        f"Add the missing parameter(s) with --param <key>=<value> or set in parameters.py."
                    ) from e
            # Should be unreachable: build_task_list only populates unresolved when
            # try_instantiate returned None, which means MissingParameterException will
            # reproduce above. Guard against future exception hierarchy changes.
            raise CliUserError(
                f"Cannot instantiate task(s) {sorted(index.qualified_name(c) for c in unresolved)!r} in direct mode. "
                f"Add missing parameters with --param <key>=<value> or set in parameters.py."
            )

        # The considered set must cover everything this invocation will delete.
        # Under --with-requirements that is the whole requirement tree, exactly
        # as `show --with-requirements` considers it — otherwise a config key
        # only a requirement declares would be reported as inapplicable here
        # while `show` stays silent on the identical project.
        considered = list(target_classes)
        if with_requirements and names is not None:
            for task in task_list:
                for required in task_iterator(task):
                    considered.append(type(required))
        check_param_applicability(
            param_dicts[0],
            considered,
            ctx.override_keys,
            named=names is not None,
            target=", ".join(index.qualified_name(cls) for cls in target_classes) if names is not None else "any task",
            strict=True,
        )

        keep_tasks = [cls.__name__ for cls in keep_classes] if keep_classes else None
        if with_requirements and names is not None:
            runner.remove_requirement_outputs(task_list, auto_confirm=yes, keep_tasks=keep_tasks)
        else:
            runner.remove_outputs(
                task_list,
                target_tasks=[cls.__name__ for cls in target_classes],
                auto_confirm=yes,
                keep_tasks=keep_tasks,
            )
