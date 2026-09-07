from dataclasses import dataclass
from rich.console import Console
from rich.panel import Panel
from rich.text import Text


@dataclass
class CliUserError(Exception):
    message: str
    exit_code: int = 2

    def __str__(self) -> str:
        return self.message


console = Console()


def _render_cli_error(msg: str) -> None:
    """Render *msg* as a bordered error panel.

    *msg* is built as a :class:`rich.text.Text` from literal segments rather than
    interpolated into a markup string. Error messages carry user-controlled text —
    a mistyped ``--param`` key, an unparsable ``--executable`` value, a path holding
    a serialised list — and Rich's markup parser would treat a bracketed fragment as
    a style tag: ``[bold]`` is silently deleted, while ``[/foo]`` raises
    :class:`rich.errors.MarkupError` and replaces the error report with a traceback.
    ``rich.markup.escape`` is not a sufficient fix here, so do not substitute it.

    :param msg: The error message to display, which may contain arbitrary user input.
    :type msg: str
    """
    body = Text("Error: ", style="bold red")
    body.append(msg)
    console.print(
        Panel.fit(
            body,
            border_style="red",
            title="b2luigi",
        )
    )
