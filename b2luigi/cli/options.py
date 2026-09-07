"""Shared CLI parameter definitions for the b2luigi Typer sub-apps.

:Description: Several subcommands expose the same options (``--task-file``,
    ``--params-file``, ``--param``) and the same task-class-name argument. Defining them
    once here stops their flag names, help text and completion behaviour from drifting
    apart across ``b2luigi/cli/apps/*.py``.

    ``Annotated[T, typer.Option(...)]`` is an ordinary type alias, so reusing one of these
    in a signature is identical to writing it out — the CLI surface is unchanged.

.. warning::
    Never add ``from __future__ import annotations`` to a module that uses
    :func:`class_names_arg`. The factory is a call in annotation position and must be
    evaluated eagerly; under PEP 563 the annotation becomes a string Typer cannot resolve.
"""

from typing import Annotated, Any

import typer

from b2luigi.cli.utils import complete_task_names

#: Path to the file defining the task classes. Used by ``run``, ``show``, ``remove``,
#: ``graph``, ``tasks`` and ``batch-runner``.
TaskFile = Annotated[
    str | None,
    typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
]

#: Path to the file exposing the ``config`` dict. Used by ``run``, ``show``, ``remove``
#: and ``graph``.
ParamsFile = Annotated[
    str | None,
    typer.Option("--params-file", "-p", help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
]

#: User-facing repeatable task parameter override, JSON-typed via
#: :func:`~b2luigi.cli.utils.parse_kv_params`. Deliberately distinct from
#: ``batch-runner``'s ``--param``, which carries raw serialised strings.
Params = Annotated[
    list[str] | None,
    typer.Option(
        "--param",
        "-P",
        help="Override task parameters (repeatable): key=value. Values are parsed as JSON when possible.",
    ),
]


def class_names_arg(help_text: str) -> Any:
    """Build the annotation for an optional list-of-task-class-names positional argument.

    The help text is genuinely per-command ("to remove", "to show", "to scope the graph
    to"), so this is a factory rather than a plain alias: callers keep their own wording
    while sharing the type and the task-name completion hook.

    :param help_text: The command-specific help string for this argument.
    :type help_text: str
    :returns: An ``Annotated`` type suitable for a Typer parameter annotation.
    :rtype: Any
    """
    return Annotated[
        list[str] | None,
        typer.Argument(help=help_text, shell_complete=complete_task_names),
    ]
