import os
import platform
import subprocess
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as get_version
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.panel import Panel

from b2luigi.cli.apps.batch_runner import batch_runner_app
from b2luigi.cli.apps.remove import remove_app
from b2luigi.cli.apps.run import run_app
from b2luigi.cli.apps.show import show_app
from b2luigi.cli.apps.tasks import tasks_app
from b2luigi.cli.apps.test import test_app
from b2luigi.cli.errors import CliUserError, _render_cli_error
from b2luigi.cli.templates import PARAMS_TEMPLATE, TASKS_TEMPLATE


def get_b2luigi_version() -> str:
    """Return the installed b2luigi version string, or ``"0.0.0"`` as fallback.

    :returns: Version string in semver format.
    :rtype: str
    """
    try:
        return get_version("b2luigi")
    except PackageNotFoundError:
        return "0.0.0"


def _version_callback(value: bool) -> None:
    """Eager callback for --version/-V/-v: print version and exit immediately."""
    if value:
        typer.echo(get_b2luigi_version())
        raise typer.Exit()


app = typer.Typer(
    name="b2luigi",
    help="Run user-defined b2luigi tasks",
    add_completion=True,
    no_args_is_help=True,
)

app.add_typer(run_app, name="run")
app.add_typer(show_app, name="show")
app.add_typer(remove_app, name="remove")
app.add_typer(test_app, name="test")
app.add_typer(tasks_app, name="tasks")
app.add_typer(batch_runner_app, name="batch-runner", hidden=True)

console = Console()


@app.callback()
def main_callback(
    version: Annotated[
        Optional[bool],
        typer.Option(
            "--version",
            "-V",
            "-v",
            callback=_version_callback,
            is_eager=True,
            help="Show the version and exit.",
        ),
    ] = None,
) -> None:
    """b2luigi — Belle II extension of the Luigi workflow management framework."""


@app.command("about")
def about() -> None:
    """Show environment and b2luigi installation info."""
    v = get_b2luigi_version()

    lines = [
        f"b2luigi: {v}",
        f"python: {sys.version.split()[0]}",
        f"platform: {platform.platform()}",
        f"cwd: {os.getcwd()}",
        f"B2LUIGI_TASK_FILE: {os.getenv('B2LUIGI_TASK_FILE', '(unset)')}",
        f"B2LUIGI_PARAMS_FILE: {os.getenv('B2LUIGI_PARAMS_FILE', '(unset)')}",
    ]
    console.print(Panel.fit("\n".join(lines), title="About", border_style="cyan"))


@app.command("init")
def init(
    force: Annotated[bool, typer.Option("--force", help="Overwrite existing files.")] = False,
) -> None:
    """Create starter tasks.py, parameters.py, and optional config."""
    files = {
        "tasks.py": TASKS_TEMPLATE,
        "parameters.py": PARAMS_TEMPLATE,
    }
    for name, content in files.items():
        p = Path(name)
        if p.exists() and not force:
            console.print(f"[yellow]Skip[/yellow] {name} (already exists)")
            continue
        p.write_text(content, encoding="utf-8")
        console.print(f"[green]Wrote[/green] {name}")


@app.command("version")
def version() -> None:
    """Print version and exit."""
    console.print(get_b2luigi_version())


@app.command("self-update")
def self_update() -> None:
    """Update b2luigi to the latest version in the current environment."""
    old = get_b2luigi_version()

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "b2luigi"])

    new = get_version("b2luigi")
    if new == old:
        console.print(f"[yellow]b2luigi is already up-to-date ({new}).[/yellow]")
    else:
        console.print(f"[green]b2luigi updated: {old} → {new}[/green]")


@app.command("status")
def status(
    task_filename: Annotated[
        Optional[str],
        typer.Option("--task-file", "-f", help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
    parameter_filename: Annotated[
        Optional[str],
        typer.Option("--params-file", "-p", help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
) -> None:
    """Show the output status of the full dependency tree.

    Equivalent to ``b2luigi show`` with no ``-t`` flag — displays every task in
    the dependency tree together with whether its outputs exist.

    :param task_filename: Path to the task definitions file (or ``$B2LUIGI_TASK_FILE``).
    :param parameter_filename: Path to the parameters file (or ``$B2LUIGI_PARAMS_FILE``).
    """
    from b2luigi.cli.apps.show import show_task

    show_task(task_filename=task_filename, parameter_filename=parameter_filename)


def main() -> None:
    """Entry point for the ``b2luigi`` CLI binary.

    :raises SystemExit: With the error exit code when a :class:`CliUserError` is raised.
    """
    try:
        app()
    except CliUserError as e:
        _render_cli_error(str(e))
        raise SystemExit(e.exit_code)


if __name__ == "__main__":
    main()
