from cyclopts import App, Parameter
import inspect
import json
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from typing import Annotated, Dict, List, Optional

from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import get_task_classes, get_task_instance, process_task_instance, resolve_defaults, suggest

run_app = App(name="run", help="Run a task class from tasks.py")
console = Console()


def run_task(
    class_name: str,
    task_filename="tasks.py",
    parameters_file="parameters.py",
    overrides: Optional[Dict[str, object]] = None,
) -> None:
    task_instance = get_task_instance(class_name, task_filename, parameters_file, overrides)
    process_task_instance(task_instance)


def parse_kv_params(items: List[str]) -> Dict[str, object]:
    out: Dict[str, object] = {}
    for item in items:
        if "=" not in item:
            raise CliUserError(f"Invalid --param '{item}'. Use key=value.")
        key, raw = item.split("=", 1)
        key = key.strip()
        raw = raw.strip()
        if not key:
            raise CliUserError(f"Invalid --param '{item}'. Key is empty.")

        # Try JSON (covers: 1, 1.2, true, null, ["a"], {"x":1}, "string")
        try:
            val = json.loads(raw)
        except Exception:
            val = raw  # fallback as plain string

        out[key] = val
    return out


@run_app.default
def run(
    classname: Annotated[
        str,
        Parameter(name=["--task", "-t", "--classname", "-c"], help="The name of the task class to run"),
    ],
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
    parameter_filename: Annotated[
        str | None,
        Parameter(name=["--params-file", "-p"], help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
    params: Annotated[
        List[str],
        Parameter(
            name=["--param", "-P"],
            help="Override task parameters (repeatable): key=value. Values are parsed as JSON when possible.",
        ),
    ] = [],
    dry_run: Annotated[
        bool,
        Parameter(name="--dry-run", help="Instead of running the task(s), write out which tasks will be executed."),
    ] = False,
    scheduler_host: Annotated[
        Optional[str],
        Parameter(
            name="--scheduler-host", help="Host of a central luigi scheduler to connect to (instead of running locally)"
        ),
    ] = None,
    scheduler_port: Annotated[
        Optional[int],
        Parameter(
            name="--scheduler-port", help="Port of a central luigi scheduler to connect to (instead of running locally)"
        ),
    ] = None,
):
    d = resolve_defaults(task_filename, parameter_filename)
    tasks = get_task_classes(d.task_file)
    names = [cls.__name__ for cls in tasks]

    if classname not in names:
        suggestion = suggest(classname, names)
        msg = f"Unknown task '{classname}'."
        if suggestion:
            msg += f" Did you mean '{suggestion}'?"
        msg += " Use 'b2luigi run list' to see available tasks."
        raise CliUserError(msg)

    overrides = parse_kv_params(params)
    run_task(class_name=classname, task_filename=d.task_file, parameters_file=d.params_file, overrides=overrides)


@run_app.command
def list(
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
):
    """List available task classes."""
    d = resolve_defaults(task_filename, None)
    tasks = get_task_classes(d.task_file)

    table = Table(title="Available Tasks", show_lines=False)
    table.add_column("Task", style="bold")
    table.add_column("Description")

    for cls in tasks:
        doc = (getattr(cls, "__doc__", "") or "").strip().splitlines()
        short = doc[0].strip() if doc else ""
        table.add_row(cls.__name__, short)

    console.print(Panel.fit(table, title="b2luigi", border_style="cyan"))


@run_app.command(name="help")
def task_help(
    classname: str,
    task_filename: Annotated[
        str | None,
        Parameter(name=["--task-file", "-f"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ] = None,
):
    """Show help for a specific task class."""
    d = resolve_defaults(task_filename, None)
    tasks = {cls.__name__: cls for cls in get_task_classes(d.task_file)}
    cls = tasks.get(classname)
    if not cls:
        raise CliUserError(f"Unknown task '{classname}'. Use 'b2luigi run list'.")

    doc = inspect.getdoc(cls) or "(No docstring provided.)"
    console.print(Panel.fit(Markdown(doc), title=f"{classname}", border_style="cyan"))
