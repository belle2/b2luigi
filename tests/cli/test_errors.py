"""Tests for CLI error rendering.

Error messages routinely carry user-controlled text — a mistyped ``--param`` key, an
unparsable ``--executable`` value. Rendering them through Rich's markup parser makes
that text executable: a bracketed fragment is either parsed as a style tag (silently
deleting it) or rejected outright with ``MarkupError``, turning a clean error report
into a traceback. These tests pin the rendering as literal.
"""

import io
from unittest import TestCase, mock

from rich.console import Console

from b2luigi.cli.errors import CliUserError, _render_cli_error


def _render(message: str) -> str:
    """Render *message* through :func:`_render_cli_error` and return the output text.

    :param message: The error message to render.
    :type message: str
    :returns: Everything the error console emitted.
    :rtype: str
    """
    buffer = io.StringIO()
    console = Console(file=buffer, force_terminal=False, width=200)
    with mock.patch("b2luigi.cli.errors.console", console):
        _render_cli_error(message)
    return buffer.getvalue()


class TestRenderCliError(TestCase):
    """User-controlled text must reach the panel as literal characters."""

    def test_plain_message_is_rendered(self) -> None:
        """An ordinary message still renders with its Error: prefix."""
        rendered = _render(name := "something went wrong")
        self.assertIn(name, rendered)
        self.assertIn("Error:", rendered)

    def test_unmatched_closing_tag_does_not_raise(self) -> None:
        """A closing-tag-like fragment must not abort rendering.

        ``rich.markup`` raises ``MarkupError`` for a closing tag with no opening tag,
        which previously escaped ``_render_cli_error`` and replaced the error report
        with a traceback.
        """
        rendered = _render('--executable "[/foo]\'x" could not be parsed')
        self.assertIn("[/foo]", rendered)

    def test_style_like_fragment_is_not_swallowed(self) -> None:
        """A valid-looking style tag must survive as literal text, not be consumed.

        ``[bold]`` parses cleanly as markup, so it would vanish from the output
        entirely rather than raising — silent loss of the very text the user needs
        in order to see what b2luigi objected to.
        """
        rendered = _render("--param [bold]=1 does not apply to any task")
        self.assertIn("[bold]", rendered)

    def test_square_brackets_in_a_path_survive(self) -> None:
        """Serialised list parameters put brackets in paths; they must render intact."""
        rendered = _render("no such file: /results/items=[1, 2, 3]/out.root")
        self.assertIn("items=[1, 2, 3]", rendered)


class TestCliUserErrorContract(TestCase):
    """The exception carries the message and the CLI exit code."""

    def test_str_returns_the_message(self) -> None:
        """str() yields the bare message, which is what the renderers receive."""
        self.assertEqual(str(CliUserError("bad input")), "bad input")

    def test_default_exit_code_is_two(self) -> None:
        """Usage errors exit 2, matching the convention used across the CLI."""
        self.assertEqual(CliUserError("bad input").exit_code, 2)
