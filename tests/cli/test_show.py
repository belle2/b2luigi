"""Integration tests for b2luigi show CLI command.

:Description: Tests the ``b2luigi show`` command end-to-end, covering task
    listing, dependency tree display, parameter overrides, and error handling.
"""

import os
import pathlib
import shutil

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
