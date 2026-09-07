"""Unit tests for the shared CLI parameter definitions in b2luigi.cli.options."""

from typing import Any
from unittest import TestCase

import typer
import typer.main

from b2luigi.cli.options import Params, ParamsFile, TaskFile, class_names_arg


def _resolve(annotation: Any):
    """Mount an annotation on a throwaway command and return its resolved click parameter.

    Typer decides option-vs-argument from the ``Annotated`` metadata itself, so the same
    helper works for both.

    :param annotation: The ``Annotated`` alias under test.
    :returns: The click ``Parameter`` Typer built from the annotation.
    """
    app = typer.Typer()

    @app.command()
    def _cmd(value: annotation = None) -> None:  # type: ignore[valid-type]
        pass

    command = typer.main.get_command(app)
    return next(p for p in command.params if p.name == "value")


class TestTaskFile(TestCase):
    def test_flags(self) -> None:
        self.assertEqual(_resolve(TaskFile).opts, ["--task-file", "-f"])

    def test_help(self) -> None:
        self.assertEqual(_resolve(TaskFile).help, "Task definitions file (or $B2LUIGI_TASK_FILE)")


class TestParamsFile(TestCase):
    def test_flags(self) -> None:
        self.assertEqual(_resolve(ParamsFile).opts, ["--params-file", "-p"])

    def test_help(self) -> None:
        self.assertEqual(_resolve(ParamsFile).help, "Parameters file (or $B2LUIGI_PARAMS_FILE)")


class TestParams(TestCase):
    def test_flags(self) -> None:
        self.assertEqual(_resolve(Params).opts, ["--param", "-P"])

    def test_help_mentions_json(self) -> None:
        self.assertEqual(
            _resolve(Params).help,
            "Override task parameters (repeatable): key=value. Values are parsed as JSON when possible.",
        )

    def test_is_repeatable(self) -> None:
        self.assertTrue(_resolve(Params).multiple)


class TestClassNamesArg(TestCase):
    def test_help_is_per_call(self) -> None:
        """The factory keeps each command's own wording."""
        self.assertEqual(_resolve(class_names_arg("Remove these.")).help, "Remove these.")
        self.assertEqual(_resolve(class_names_arg("Show these.")).help, "Show these.")

    def test_completion_is_attached(self) -> None:
        """All calls share the task-name completion hook."""
        from b2luigi.cli.utils import complete_task_names

        param = _resolve(class_names_arg("Anything."))
        self.assertIs(param._custom_shell_complete, complete_task_names)

    def test_accepts_multiple_values(self) -> None:
        param = _resolve(class_names_arg("Anything."))
        self.assertEqual(param.nargs, -1)
