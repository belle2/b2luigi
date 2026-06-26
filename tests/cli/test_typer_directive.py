"""Unit tests for the docs/_ext/typer_cli.py custom Sphinx directive."""
import sys
import os

import pytest

# Add docs/_ext to sys.path so typer_cli can be imported directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "docs", "_ext"))

from typer_cli import _build_rst  # noqa: E402
from b2luigi.cli import _click_app  # noqa: E402


EXPECTED_VISIBLE_COMMANDS = [
    "b2luigi remove",
    "b2luigi run",
    "b2luigi show",
    "b2luigi tasks",
    "b2luigi test",
]


class TestBuildRst:
    def test_all_visible_commands_present(self):
        rst = _build_rst("b2luigi", _click_app)
        for cmd in EXPECTED_VISIBLE_COMMANDS:
            assert cmd in rst, f"Expected '{cmd}' in generated RST"

    def test_batch_runner_hidden(self):
        rst = _build_rst("b2luigi", _click_app)
        assert "batch-runner" not in rst

    def test_tasks_info_nested(self):
        rst = _build_rst("b2luigi", _click_app)
        assert "b2luigi tasks info" in rst

    def test_nested_heading_uses_tilde_underline(self):
        rst = _build_rst("b2luigi", _click_app)
        # Find the line after "b2luigi tasks info" and check it uses ~~~
        lines = rst.splitlines()
        for i, line in enumerate(lines):
            if line == "b2luigi tasks info":
                assert lines[i + 1] == "~" * len("b2luigi tasks info")
                break
        else:
            pytest.fail("'b2luigi tasks info' heading not found")

    def test_top_level_heading_uses_dash_underline(self):
        rst = _build_rst("b2luigi", _click_app)
        lines = rst.splitlines()
        for i, line in enumerate(lines):
            if line == "b2luigi run":
                assert lines[i + 1] == "-" * len("b2luigi run")
                break
        else:
            pytest.fail("'b2luigi run' heading not found")
