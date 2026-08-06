"""Drift-prevention tests for the shared CLI parameter definitions.

:Description: Several subcommands expose the same options. Before
    :mod:`b2luigi.cli.options` existed, each declared them independently and they drifted
    (``--task-file`` gained a trailing period in some commands but not others). These tests
    assert the shared definitions stay shared, and that the one deliberate exception —
    ``batch-runner``'s ``--param`` — stays different.
"""

from unittest import TestCase

import typer.main

from b2luigi.cli import app

#: ``batch-runner``'s ``--param`` carries raw serialised strings for the worker and is
#: intentionally not the user-facing JSON-typed option.
EXCLUDED_FROM_PARAM_SHARING = "batch-runner"


def _walk_commands(command, prefix=""):
    """Yield ``(qualified_name, click_command)`` for a command and all its subcommands.

    :param command: The click command to walk.
    :param prefix: Accumulated command path, used for readable assertion messages.
    :returns: Generator of ``(str, click.Command)`` pairs.
    """
    for name, sub in getattr(command, "commands", {}).items():
        qualified = f"{prefix}{name}"
        yield qualified, sub
        yield from _walk_commands(sub, prefix=f"{qualified} ")


def _declarations(flag: str):
    """Collect every declaration of ``flag`` across the whole command tree.

    :param flag: The long flag to look for, e.g. ``"--task-file"``.
    :returns: Mapping of qualified command name to the resolved click parameter.
    """
    root = typer.main.get_command(app)
    found = {}
    for name, command in _walk_commands(root):
        for param in command.params:
            if flag in param.opts:
                found[name] = param
    return found


class TestSharedOptionsDoNotDrift(TestCase):
    """Every command declaring a shared option must declare it identically."""

    def _assert_consistent(self, flag: str, *, exclude: frozenset[str] = frozenset()) -> None:
        found = {n: p for n, p in _declarations(flag).items() if n not in exclude}
        self.assertGreater(len(found), 1, f"Expected {flag} on more than one command")

        helps = {name: param.help for name, param in found.items()}
        self.assertEqual(
            len(set(helps.values())),
            1,
            f"Help text for {flag} differs across commands: {helps}",
        )

        opts = {name: tuple(param.opts) for name, param in found.items()}
        self.assertEqual(
            len(set(opts.values())),
            1,
            f"Flag aliases for {flag} differ across commands: {opts}",
        )

    def test_task_file_is_consistent(self) -> None:
        self._assert_consistent("--task-file")

    def test_params_file_is_consistent(self) -> None:
        self._assert_consistent("--params-file")

    def test_user_facing_param_is_consistent(self) -> None:
        self._assert_consistent("--param", exclude={EXCLUDED_FROM_PARAM_SHARING})

    def test_task_file_reaches_every_expected_command(self) -> None:
        """Guards against a command silently dropping the shared option."""
        self.assertEqual(
            set(_declarations("--task-file")),
            {"run", "show", "remove", "graph", "tasks", "tasks info", "batch-runner"},
        )


class TestBatchRunnerParamStaysDistinct(TestCase):
    """``batch-runner``'s ``--param`` must not be merged into the shared definition."""

    def test_help_differs_from_the_user_facing_option(self) -> None:
        found = _declarations("--param")
        self.assertIn(EXCLUDED_FROM_PARAM_SHARING, found)

        worker_help = found[EXCLUDED_FROM_PARAM_SHARING].help
        user_helps = {n: p.help for n, p in found.items() if n != EXCLUDED_FROM_PARAM_SHARING}

        self.assertNotIn(
            worker_help,
            set(user_helps.values()),
            "batch-runner's --param was merged with the user-facing one; they have different semantics "
            "(raw serialised strings vs JSON-typed user input).",
        )

    def test_help_warns_against_manual_use(self) -> None:
        found = _declarations("--param")
        self.assertIn("do not set by hand", found[EXCLUDED_FROM_PARAM_SHARING].help)
