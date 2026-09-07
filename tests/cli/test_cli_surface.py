"""Tests for the top-level CLI command surface.

:Description: These assertions used to live in ``test_typer_directive.py``, which tested the
    custom ``docs/_ext/typer_cli.py`` Sphinx directive. That directive was replaced by
    ``sphinxcontrib-typer``, so its formatting tests (RST heading underlines, etc.) are gone.
    The two invariants worth keeping are ours, not the docs generator's: which commands the
    app exposes, and that ``batch-runner`` stays hidden. They are asserted here against the
    resolved click command, which is exactly what any docs generator consumes.
"""

from unittest import TestCase

import typer.main

from b2luigi.cli import app

EXPECTED_VISIBLE_COMMANDS = [
    "about",
    "graph",
    "init",
    "remove",
    "run",
    "self-update",
    "show",
    "tasks",
    "test",
    "version",
]


class TestCommandSurface(TestCase):
    """The set of commands the CLI exposes to users."""

    def setUp(self) -> None:
        self.click_app = typer.main.get_command(app)

    def test_all_expected_commands_are_exposed(self) -> None:
        """Every user-facing command is registered on the root app."""
        for name in EXPECTED_VISIBLE_COMMANDS:
            self.assertIn(name, self.click_app.commands, f"Expected '{name}' to be a registered command")

    def test_no_unexpected_visible_commands(self) -> None:
        """Adding a command without updating this list is a deliberate decision, not an accident."""
        visible = {name for name, cmd in self.click_app.commands.items() if not getattr(cmd, "hidden", False)}
        self.assertEqual(visible, set(EXPECTED_VISIBLE_COMMANDS))

    def test_batch_runner_is_hidden(self) -> None:
        """``batch-runner`` is invoked by the batch system, never by users, so it stays out of --help."""
        self.assertIn("batch-runner", self.click_app.commands)
        self.assertTrue(self.click_app.commands["batch-runner"].hidden)

    def test_tasks_info_is_nested_under_tasks(self) -> None:
        """``b2luigi tasks info`` is a subcommand of ``tasks``, not a top-level command."""
        self.assertIn("info", self.click_app.commands["tasks"].commands)
        self.assertNotIn("info", self.click_app.commands)
