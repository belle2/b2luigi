import subprocess
from typing import Annotated, Literal
from cyclopts import App, Parameter
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as get_version
import os
import platform
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
import sys

from b2luigi.cli.apps import list_of_apps
from b2luigi.cli.errors import CliUserError, _render_cli_error
from b2luigi.cli.templates import PARAMS_TEMPLATE, TASKS_TEMPLATE


def get_b2luigi_version() -> str:
    # If you're installed as a package, this is the most robust.
    try:
        return get_version("b2luigi")
    except PackageNotFoundError:
        return "0.0.0"


# TODO: Improve help message by a lot
app = App(
    name="b2luigi",
    help="Run user-defined b2luigi tasks",
    version=get_b2luigi_version(),
    version_flags=["--version", "-V", "-v"],
)
app.register_install_completion_command()

# Register the user commands
for app_i in list_of_apps:
    app.command(app_i)


console = Console()


@app.command
def info():
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
    console.print(Panel.fit("\n".join(lines), title="Info", border_style="cyan"))


@app.command
def help(*args: str):
    """Show help for b2luigi or a subcommand."""
    sys.argv = [sys.argv[0], *args, "--help"]
    app()


@app.command
def init(force: bool = False):
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


@app.command
def version():
    """Print version and exit."""
    console.print(get_b2luigi_version())


@app.command(name="self-update")
def self_update():
    """Update b2luigi to the latest version in the current environment."""
    old = get_b2luigi_version()

    # (Optional but recommended in the cookbook) keep pip fresh
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "b2luigi"])

    new = get_version("b2luigi")
    if new == old:
        console.print(f"[yellow]b2luigi is already up-to-date ({new}).[/yellow]")
    else:
        console.print(f"[green]b2luigi updated: {old} → {new}[/green]")


@app.command
def completion(
    shell: Annotated[
        Literal["bash", "zsh", "fish"],
        Parameter(help="Shell type to generate completion for."),
    ],
    install: Annotated[
        bool,
        Parameter(name="--install", help="Install completion to the default shell-specific location."),
    ] = False,
    output: Annotated[
        Path | None,
        Parameter(name=["--output", "-o"], help="Write completion script to this path instead of stdout."),
    ] = None,
):
    """Generate or install shell completion for b2luigi."""
    if install:
        # Installs to the default location for the shell (or to `output` if provided).
        installed_path = app.install_completion(shell=shell, output=output)
        print(installed_path)  # raw stdout: shell scripts must not contain ANSI escape codes
        return

    script = app.generate_completion(shell=shell)

    if output is not None:
        output.write_text(script, encoding="utf-8")
        print(output)  # raw stdout: shell scripts must not contain ANSI escape codes
    else:
        print(script)  # raw stdout: shell scripts must not contain ANSI escape codes


@app.command
def status(
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
    parameter_filename: Annotated[
        str | None,
        Parameter(name=["--params-file", "-p"], help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
):
    """Show the output status of the full dependency tree.

    Equivalent to ``b2luigi show`` with no ``-t`` flag — displays every task in
    the dependency tree together with whether its outputs exist.

    :param task_filename: Path to the task definitions file (or ``$B2LUIGI_TASK_FILE``).
    :param parameter_filename: Path to the parameters file (or ``$B2LUIGI_PARAMS_FILE``).
    """
    from b2luigi.cli.apps.show import show_task

    show_task(task_filename=task_filename, parameter_filename=parameter_filename)


def main() -> None:
    try:
        app()
    except CliUserError as e:
        _render_cli_error(str(e))
        raise SystemExit(e.exit_code)


if __name__ == "__main__":
    main()
