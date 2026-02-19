from dataclasses import dataclass
from rich.console import Console
from rich.panel import Panel


@dataclass
class CliUserError(Exception):
    message: str
    exit_code: int = 2

    def __str__(self) -> str:
        return self.message


console = Console()


def _render_cli_error(msg: str) -> None:
    # Cyclopts uses Rich; this fits right in visually.
    console.print(
        Panel.fit(
            f"[bold red]Error:[/bold red] {msg}",
            border_style="red",
            title="b2luigi",
        )
    )
