"""Custom Sphinx directive: generates CLI reference from a Typer app."""
from __future__ import annotations

import importlib
from typing import Any

from docutils import nodes
from docutils.parsers.rst import directives
from docutils.statemachine import ViewList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles
from typer.main import get_command


def _synopsis(prog: str, cmd_name: str, cmd: Any) -> str:
    parts = [prog] if not cmd_name else [prog, cmd_name]
    parts.append("[OPTIONS]")
    for p in cmd.params:
        if p.param_type_name != "argument" or getattr(p, "hidden", False):
            continue
        name = p.name.upper()
        if p.nargs == -1:
            parts.append(f"[{name}]...")
        elif not p.required:
            parts.append(f"[{name}]")
        else:
            parts.append(name)
    # Groups dispatch to subcommands; mirror click's own usage line for them.
    if getattr(cmd, "commands", None):
        parts += ["COMMAND", "[ARGS]..."]
    return " ".join(parts)


def _command_rst(prog: str, cmd_name: str, cmd: Any, level: int) -> str:
    heading = f"{prog} {cmd_name}".strip()
    underline = ("-" if level == 2 else "~") * len(heading)
    help_text = (cmd.help or "").strip()
    synopsis = _synopsis(prog, cmd_name, cmd)

    lines: list[str] = [
        heading,
        underline,
        "",
        help_text,
        "",
        ".. code-block:: text",
        "",
        f"   {synopsis}",
        "",
    ]

    args = [p for p in cmd.params if p.param_type_name == "argument" and not getattr(p, "hidden", False)]
    opts = [p for p in cmd.params if p.param_type_name == "option" and not getattr(p, "hidden", False)]

    if args:
        lines += ["**Arguments**", ""]
        for p in args:
            required = " *(required)*" if p.required else " *(optional)*"
            lines += [p.name.upper(), f"   {(p.help or '').strip()}{required}", ""]

    if opts:
        lines += ["**Options**", ""]
        for p in opts:
            names = ", ".join(f"``{o}``" for o in p.opts)
            if not getattr(p, "is_flag", False):
                try:
                    mv = p.make_metavar()
                except TypeError:
                    try:
                        mv = p.make_metavar(None)
                    except TypeError:
                        mv = p.type.name.upper() if hasattr(p, "type") and hasattr(p.type, "name") else None
                if mv:
                    names += f" {mv}"
            lines += [names, f"   {(p.help or '').strip()}", ""]

    return "\n".join(lines)


def _build_rst(prog: str, click_group: Any) -> str:
    # The root group carries its own help text and global options (``--version``,
    # ``--install-completion``, …). Without this section they are undocumented,
    # since the loop below only walks the registered subcommands.
    sections: list[str] = [_command_rst(prog, "", click_group, level=2)]
    for cmd_name, cmd in sorted(click_group.commands.items()):
        if getattr(cmd, "hidden", False):
            continue
        sections.append(_command_rst(prog, cmd_name, cmd, level=2))
        if hasattr(cmd, "commands"):
            for sub_name, sub_cmd in sorted(cmd.commands.items()):
                if getattr(sub_cmd, "hidden", False):
                    continue
                sections.append(_command_rst(f"{prog} {cmd_name}", sub_name, sub_cmd, level=3))
    return "\n\n".join(sections)


class TyperCliDirective(SphinxDirective):
    """Auto-generate CLI reference RST from a Typer app.

    Usage in RST::

        .. typer:: b2luigi.cli:app
           :prog: b2luigi
    """

    required_arguments = 1
    optional_arguments = 0
    option_spec = {"prog": directives.unchanged}
    has_content = False

    def run(self) -> list[nodes.Node]:
        arg = self.arguments[0]
        if ":" not in arg:
            msg = self.state_machine.reporter.error(
                f"'.. typer::' argument must be 'module:attr' (e.g. 'b2luigi.cli:app'), got: {arg!r}",
                line=self.lineno,
            )
            return [msg]
        module_path, attr_name = arg.rsplit(":", 1)
        mod = importlib.import_module(module_path)
        typer_app = getattr(mod, attr_name)

        click_app = get_command(typer_app)

        prog = self.options.get("prog", click_app.name or "b2luigi")
        rst_text = _build_rst(prog, click_app)

        vl = ViewList()
        source = self.get_source_info()[0]
        for i, line in enumerate(rst_text.splitlines()):
            vl.append(line, source, i)

        container = nodes.section()
        container.document = self.state.document
        nested_parse_with_titles(self.state, vl, container)
        return container.children


def setup(app: Any) -> dict[str, Any]:
    app.add_directive("typer", TyperCliDirective)
    return {"version": "1.0", "parallel_read_safe": True, "parallel_write_safe": True}
