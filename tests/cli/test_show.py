"""Integration tests for b2luigi show CLI command.

:Description: Tests the ``b2luigi show`` command end-to-end, covering task
    listing, dependency tree display, parameter overrides, and error handling.
"""

import contextlib
import io
import os
import pathlib
import re
import shutil
from unittest import TestCase
from unittest.mock import patch

from rich.console import Console

from b2luigi.cli import runner

from .helpers import CLITestCase


class TestShow(CLITestCase):
    """Integration tests for the show command."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_show_all_tasks(self) -> None:
        """Verify that ``b2luigi show`` with no args renders all tasks."""
        returncode, stdout, stderr = self._run_cli("show")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_show_named_task(self) -> None:
        """Verify that ``b2luigi show LeafTask`` renders that task."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("Location", stdout)

    def test_show_multiple_named_tasks(self) -> None:
        """Verify that ``b2luigi show LeafTask RootTask`` renders both tasks."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "RootTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)

    def test_show_unknown_task_error(self) -> None:
        """Verify that ``b2luigi show Nonexistent`` exits with error."""
        returncode, stdout, stderr = self._run_cli("show", ["Nonexistent"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_show_unknown_task_with_typo_suggestion(self) -> None:
        """Verify that error message suggests a close task name."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTsk"])
        self.assertNotEqual(returncode, 0)
        error_msg = stdout + stderr
        self.assertIn("Unknown task", error_msg)
        self.assertTrue("LeafTask" in error_msg or "Did you mean" in error_msg)

    def test_show_with_param_override(self) -> None:
        """Verify that ``--param split=99`` is accepted without error."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "--param", "split=99"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)

    def test_show_with_requirements_leaf_task(self) -> None:
        """show LeafTask --with-requirements shows only LeafTask (it has no requirements)."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "--with-requirements"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)

    def test_show_with_requirements_root_task(self) -> None:
        """show RootTask --with-requirements shows RootTask AND its requirement LeafTask."""
        returncode, stdout, stderr = self._run_cli("show", ["RootTask", "--with-requirements"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("RootTask", stdout)
        self.assertIn("LeafTask", stdout)

    def test_show_with_custom_task_file(self) -> None:
        """Verify that ``--task-file`` overrides the default tasks.py."""
        returncode, stdout, stderr = self._run_cli("show", ["-f", "tasks.py"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)

    def test_show_with_custom_params_file(self) -> None:
        """Verify that ``--params-file`` overrides the default parameters.py."""
        returncode, stdout, stderr = self._run_cli("show", ["-p", "parameters.py"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertTrue("LeafTask" in stdout or "RootTask" in stdout)

    def test_show_missing_tasks_file(self) -> None:
        """Verify that missing tasks.py produces a helpful error."""
        os.remove(os.path.join(self.tmp_dir, "tasks.py"))
        returncode, stdout, stderr = self._run_cli("show")
        self.assertNotEqual(returncode, 0)
        self.assertTrue("tasks.py" in (stdout + stderr) or "not found" in (stdout + stderr))

    def test_show_missing_parameters_file(self) -> None:
        """Verify that show succeeds without parameters.py (it is optional)."""
        os.remove(os.path.join(self.tmp_dir, "parameters.py"))
        # Must provide --param for required task parameters since parameters.py is missing
        returncode, stdout, stderr = self._run_cli("show", ["--param", "split=5"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_show_paths_flag_prints_bare_paths(self) -> None:
        """``b2luigi show LeafTask --paths`` prints paths and nothing else."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "--paths"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        lines = [line for line in stdout.splitlines() if line.strip()]
        self.assertTrue(lines, "no paths were printed")
        for line in lines:
            self.assertTrue(os.path.isabs(line), f"not an absolute path: {line!r}")
        self.assertNotIn("Location", stdout)
        self.assertNotIn("│", stdout)


class TestShowMultiParam(CLITestCase):
    """Integration tests for show with tasks that have different parameters."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_multi_project_files()

    def test_show_root_task_does_not_crash(self) -> None:
        """show ParentTask must not raise UnknownParameterException."""
        returncode, stdout, stderr = self._run_cli("show", ["ParentTask"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ParentTask", stdout)

    def test_show_all_tasks_does_not_crash(self) -> None:
        """show (no args) must not raise UnknownParameterException."""
        returncode, stdout, stderr = self._run_cli("show")
        self.assertEqual(returncode, 0, f"stderr: {stderr}")

    def test_show_non_root_task_via_discovery(self) -> None:
        """show ChildTask (no --param) discovers it via graph traversal."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ChildTask", stdout)

    def test_show_non_root_task_with_explicit_param(self) -> None:
        """show ChildTask --param child_param=6 uses direct path."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask", "--param", "child_param=6"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ChildTask", stdout)

    def test_show_direct_flag_errors_when_params_missing(self) -> None:
        """show ChildTask --direct fails with a clear error when params absent."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask", "--direct"])
        self.assertNotEqual(returncode, 0)
        combined = stdout + stderr
        self.assertIn("ChildTask", combined)
        self.assertIn("child_param", combined)


class TestShowNonRootTaskCorrectness(CLITestCase):
    """Verify that show only displays the named non-root task, not its parent."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_multi_project_files()

    def test_show_non_root_task_does_not_show_parent(self) -> None:
        """show ChildTask should render ChildTask panel but not ParentTask panel."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ChildTask", stdout)
        self.assertNotIn("ParentTask", stdout)

    def test_show_non_root_task_with_requirements_does_not_show_parent(self) -> None:
        """show ChildTask --with-requirements should not show ParentTask (it is not a requirement)."""
        returncode, stdout, stderr = self._run_cli("show", ["ChildTask", "--with-requirements"])
        self.assertEqual(returncode, 0, f"stderr: {stderr}")
        self.assertIn("ChildTask", stdout)
        self.assertNotIn("ParentTask", stdout)


class TestShowRequiredByAnnotation(CLITestCase):
    """Verify 'required by' subtitle appears on requirement panels."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()  # RootTask requires LeafTask

    def test_requirement_shows_required_by_subtitle(self) -> None:
        """LeafTask panel shows 'required by: RootTask' when shown via RootTask --with-requirements."""
        returncode, stdout, stderr = self._run_cli("show", ["RootTask", "--with-requirements"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("required by: RootTask", stdout)

    def test_root_task_has_no_required_by_subtitle(self) -> None:
        """RootTask panel (the named task) must not show a 'required by' subtitle."""
        returncode, stdout, stderr = self._run_cli("show", ["RootTask", "--with-requirements"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        # 'required by' must appear exactly once (for LeafTask), not for RootTask
        self.assertEqual(stdout.count("required by:"), 1)

    def test_no_annotation_without_flag(self) -> None:
        """show RootTask without --with-requirements must not include 'required by'."""
        returncode, stdout, stderr = self._run_cli("show", ["RootTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertNotIn("required by", stdout)

    def test_show_all_no_annotation(self) -> None:
        """show (no task name, full tree) must not include 'required by'."""
        returncode, stdout, stderr = self._run_cli("show")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertNotIn("required by", stdout)


class TestShowRequiredByMultiParent(CLITestCase):
    """Verify 'required by' lists multiple parents when a child has more than one."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_shared_child_files()  # ParentA + ParentB both require SharedChild

    def test_shared_child_shows_both_parents(self) -> None:
        """SharedChild panel lists both ParentA and ParentB in its subtitle."""
        returncode, stdout, stderr = self._run_cli("show", ["ParentA", "ParentB", "--with-requirements"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("ParentA", stdout)
        self.assertIn("ParentB", stdout)
        # Both parent names must appear in the 'required by' annotation
        required_by_line = next((line for line in stdout.splitlines() if "required by:" in line), "")
        self.assertIn("ParentA", required_by_line)
        self.assertIn("ParentB", required_by_line)


class TestShowParameterGeneratorGrouping(CLITestCase):
    """Integration tests for consolidated multi-instance show panels."""

    def setUp(self) -> None:
        super().setUp()
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_generator_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )
        pathlib.Path(self.tmp_dir, "parameters.py").write_text(
            "from b2luigi import ParameterGenerator\n" "config = {'value': ParameterGenerator([1, 2, 3])}\n"
        )

    def test_multiple_instances_render_as_single_panel(self) -> None:
        """show SimpleTask with 3 ParameterGenerator values renders one panel, not three."""
        returncode, stdout, stderr = self._run_cli("show", ["SimpleTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertEqual(stdout.count("SimpleTask"), 1)

    def test_multiple_instances_hide_params_column_by_default(self) -> None:
        """Without --details, the Params column must not appear even for multi-instance tasks."""
        returncode, stdout, stderr = self._run_cli("show", ["SimpleTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertNotIn("Params", stdout)

    def test_multiple_instances_with_details_shows_params_column(self) -> None:
        """--details restores the Params column (and its values) for multi-instance tasks."""
        returncode, stdout, stderr = self._run_cli("show", ["SimpleTask", "--details"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("Params", stdout)
        for v in (1, 2, 3):
            self.assertIn(f"value={v}", stdout)

    def test_single_instance_with_details_omits_params_column(self) -> None:
        """A --param override pinning the generator to one value stays free of Params even with --details."""
        returncode, stdout, stderr = self._run_cli("show", ["SimpleTask", "--param", "value=1", "--details"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertNotIn("Params", stdout)


class TestShowDetailsFlag(CLITestCase):
    """Integration tests for the --details flag gating Params/Output columns."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_default_hides_output_column(self) -> None:
        """Without --details, the Output key-name column must not be rendered."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertNotIn("Output", stdout)

    def test_details_flag_shows_output_column(self) -> None:
        """--details must render the Output key-name column."""
        returncode, stdout, stderr = self._run_cli("show", ["LeafTask", "--details"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("Output", stdout)

    def test_details_flag_works_on_full_tree(self) -> None:
        """--details with no task name (full tree) also renders the Output column."""
        returncode, stdout, stderr = self._run_cli("show", ["--details"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("Output", stdout)

    def test_details_flag_works_with_requirements(self) -> None:
        """--details combined with --with-requirements still renders the Output column."""
        returncode, stdout, stderr = self._run_cli("show", ["RootTask", "--with-requirements", "--details"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("Output", stdout)


class _StubTask:
    """Minimal stand-in for a task instance: the renderer only reads the class
    name and ``task_id``."""

    task_id = "_StubTask_abcdef"


def _render(file_names: list[str], width: int = 100, **kwargs) -> str:
    """Render the given output paths through ``_render_task_outputs``.

    :param file_names: Output paths to place in a single task's output dict.
    :type file_names: list[str]
    :param width: Console width to render at, fixed so results are deterministic.
    :type width: int
    :returns: Everything the renderer wrote to the console.
    :rtype: str
    """
    buffer = io.StringIO()
    test_console = Console(file=buffer, width=width)
    outputs = {
        f"key{index}": [{"file_name": name, "exists": True, "parameters": {}}] for index, name in enumerate(file_names)
    }
    with patch.object(runner, "console", test_console):
        runner._render_task_outputs([(_StubTask(), outputs)], **kwargs)
    return buffer.getvalue()


def _long_path() -> str:
    """Build a 25-parameter output path, the case that motivated this feature."""
    return "results/" + "/".join(f"p{index}=v{index}" for index in range(25)) + "/out.root"


def _has_row_rules(rendered: str) -> bool:
    """True if the rendered panel contains interior horizontal rules."""
    body = [line for line in rendered.splitlines() if line.startswith("│")]
    return any(set(line[1:-1].strip()) == {"─"} for line in body)


class TestRenderTaskOutputsFolding(TestCase):
    """The Location column must never discard characters of a path."""

    def test_long_path_is_not_truncated(self) -> None:
        """Every character of a 25-parameter path survives rendering.

        Folding breaks the path at the column width, which can split a
        ``pN=vN`` segment across two lines, so the assertion reconstructs the
        cell by stripping panel borders, the status glyph and whitespace rather
        than looking for individual segments.
        """
        path = _long_path()
        rendered = _render([path])

        self.assertNotIn("…", rendered, "path was ellipsized instead of folded")
        flattened = re.sub(r"[\s│╭╮╰╯─✓✗]", "", rendered)
        self.assertIn(path, flattened, "the folded path did not reconstruct to the original")

    def test_long_path_gets_row_rules(self) -> None:
        """A wrapped panel draws rules so folded rows stay distinguishable."""
        rendered = _render([_long_path(), _long_path()])
        self.assertTrue(_has_row_rules(rendered))

    def test_short_path_keeps_the_compact_layout(self) -> None:
        """A panel with no wrapping renders exactly as before — no rules."""
        rendered = _render(["results/a=1/out.root"])
        self.assertNotIn("…", rendered)
        self.assertFalse(_has_row_rules(rendered))

    def test_markup_like_path_segment_survives_rendering(self) -> None:
        """A path containing a bracketed segment that looks like Rich markup must
        not be silently swallowed, even outside ``--links``.

        The ``Location`` cell used to be built as a plain markup string, so Rich
        parsed ``[bold]`` as an (unbalanced) tag and dropped it rather than
        raising — the same class of character-loss bug this whole feature exists
        to eliminate.
        """
        path = "/results/style=[bold]/out.root"
        rendered = _render([path])
        flattened = re.sub(r"[\s│╭╮╰╯─✓✗]", "", rendered)
        self.assertIn(path, flattened, "the bracketed segment was dropped instead of rendered literally")


def _render_paths(file_names: list[str], width: int = 100, **kwargs) -> str:
    """Render output paths in ``paths_only`` mode and capture stdout.

    ``paths_only`` deliberately bypasses the Rich console and writes with
    ``print()``, so this helper captures stdout rather than a console buffer. The
    console is still patched to a fixed width to prove nothing leaks through it.

    :param file_names: Output paths to place in a single task's output dict.
    :type file_names: list[str]
    :param width: Console width for the patched console.
    :type width: int
    :returns: Everything written to stdout.
    :rtype: str
    """
    console_buffer = io.StringIO()
    stdout_buffer = io.StringIO()
    test_console = Console(file=console_buffer, width=width)
    outputs = {
        f"key{index}": [{"file_name": name, "exists": True, "parameters": {}}] for index, name in enumerate(file_names)
    }
    with patch.object(runner, "console", test_console):
        with contextlib.redirect_stdout(stdout_buffer):
            runner._render_task_outputs([(_StubTask(), outputs)], paths_only=True, **kwargs)
    return stdout_buffer.getvalue()


class TestRenderTaskOutputsPathsOnly(TestCase):
    """``--paths`` output must be bare enough to pipe and paste."""

    def test_prints_one_bare_line_per_path(self) -> None:
        """Each path is on its own line with no decoration."""
        rendered = _render_paths(["results/a=1/out.root", "results/a=1/log.txt"])
        self.assertEqual(
            rendered.splitlines(),
            ["results/a=1/out.root", "results/a=1/log.txt"],
        )

    def test_long_path_stays_on_one_line(self) -> None:
        """A long path is never wrapped, even at a narrow width — wrapping would
        break shell substitution and piping."""
        path = _long_path()
        rendered = _render_paths([path], width=40)
        self.assertEqual(rendered.splitlines(), [path])

    def test_no_panel_or_status_decoration(self) -> None:
        """No box drawing, status glyphs, or headers leak into the output."""
        rendered = _render_paths([_long_path()])
        for decoration in ("│", "╭", "╰", "─", "✓", "✗", "Location"):
            self.assertNotIn(decoration, rendered)

    def test_details_is_ignored(self) -> None:
        """``--details`` adds columns to the table view; it must not add anything
        to bare path output."""
        rendered = _render_paths(["results/a=1/out.root"], details=True)
        self.assertEqual(rendered.splitlines(), ["results/a=1/out.root"])


def _render_links(entries: list[dict], width: int = 200) -> str:
    """Render pre-built output entries with links enabled, on a console that
    reports itself as a terminal so Rich actually emits OSC 8 sequences.

    :param entries: Output-entry dicts, each with ``file_name``, ``exists`` and
        ``is_local``.
    :type entries: list[dict]
    :param width: Console width, wide enough that nothing folds.
    :type width: int
    :returns: Everything the renderer wrote, including escape sequences.
    :rtype: str
    """
    buffer = io.StringIO()
    test_console = Console(file=buffer, width=width, force_terminal=True)
    outputs = {f"key{index}": [entry] for index, entry in enumerate(entries)}
    with patch.object(runner, "console", test_console):
        runner._render_task_outputs([(_StubTask(), outputs)], links=True)
    return buffer.getvalue()


class TestRenderTaskOutputsLinks(TestCase):
    """``--links`` emits OSC 8 hyperlinks, but only where they mean something."""

    def test_local_target_is_linked(self) -> None:
        """A local file gets a file:// hyperlink."""
        rendered = _render_links(
            [{"file_name": "/tmp/results/out.root", "exists": True, "parameters": {}, "is_local": True}]
        )
        self.assertIn("\x1b]8;", rendered)
        self.assertIn("file:///tmp/results/out.root", rendered)

    def test_remote_target_is_not_linked(self) -> None:
        """A remote target gets no hyperlink — a file:// URL would be meaningless."""
        rendered = _render_links(
            [{"file_name": "/store/user/out.root", "exists": True, "parameters": {}, "is_local": False}]
        )
        self.assertNotIn("\x1b]8;", rendered)

    def test_bracket_containing_path_does_not_crash(self) -> None:
        """A path containing brackets (e.g. a serialized list parameter) must not
        raise ``rich.errors.MarkupError`` and must still be linked correctly."""
        path = "/results/items=[1, 2, 3]/out.root"
        rendered = _render_links([{"file_name": path, "exists": True, "parameters": {}, "is_local": True}])
        self.assertIn("\x1b]8;", rendered)
        self.assertIn(f"file://{path}", rendered)

    def test_links_are_suppressed_under_paths_only(self) -> None:
        """Bare path output stays bare even when links are requested."""
        console_buffer = io.StringIO()
        stdout_buffer = io.StringIO()
        test_console = Console(file=console_buffer, width=200, force_terminal=True)
        outputs = {"key0": [{"file_name": "/tmp/out.root", "exists": True, "parameters": {}, "is_local": True}]}
        with patch.object(runner, "console", test_console):
            with contextlib.redirect_stdout(stdout_buffer):
                runner._render_task_outputs([(_StubTask(), outputs)], paths_only=True, links=True)

        self.assertEqual(stdout_buffer.getvalue().splitlines(), ["/tmp/out.root"])
        self.assertNotIn("\x1b]8;", stdout_buffer.getvalue())
        # Nothing should reach the console at all in paths_only mode.
        self.assertEqual(console_buffer.getvalue(), "")


class TestGetTaskOutputsIsLocal(TestCase):
    """``get_task_outputs`` must record whether each target is a local file.

    Note: ``add_to_output`` resolves paths through the ``result_dir`` setting and
    the current working directory. If this test proves sensitive to where it runs
    (a path-resolution error, or leakage from another test in the same process),
    give it a ``tempfile.mkdtemp()`` working directory in ``setUp`` with
    ``os.chdir`` and restore the original directory in ``tearDown`` — do not
    weaken the assertion to make it pass.
    """

    def test_local_target_is_marked_local(self) -> None:
        """A b2luigi LocalTarget is reported as local."""
        import b2luigi

        class _LocalOutputTask(b2luigi.Task):
            def output(self):
                yield self.add_to_output("out.txt")

        entries = runner.get_task_outputs(_LocalOutputTask())
        flat = [entry for entry_list in entries.values() for entry in entry_list]
        self.assertTrue(flat)
        for entry in flat:
            self.assertIn("is_local", entry)
            self.assertTrue(entry["is_local"])
