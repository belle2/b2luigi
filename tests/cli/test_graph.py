"""Integration tests for b2luigi graph CLI command.

:Description: Tests the ``b2luigi graph`` command end-to-end, covering full
    graph rendering, task scoping, and error handling.
"""

import os

from .helpers import CLITestCase


class TestGraph(CLITestCase):
    """Integration tests for the graph command (tree format)."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_graph_full_tree(self) -> None:
        """graph (no args) renders both RootTask and LeafTask."""
        returncode, stdout, stderr = self._run_cli("graph")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("RootTask", stdout)
        self.assertIn("LeafTask", stdout)

    def test_graph_scoped_to_root_task(self) -> None:
        """graph RootTask renders RootTask and its requirement LeafTask."""
        returncode, stdout, stderr = self._run_cli("graph", ["RootTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("RootTask", stdout)
        self.assertIn("LeafTask", stdout)

    def test_graph_scoped_to_leaf_task(self) -> None:
        """graph LeafTask renders LeafTask only — not its parent RootTask."""
        returncode, stdout, stderr = self._run_cli("graph", ["LeafTask"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertNotIn("RootTask", stdout)

    def test_graph_unknown_task_error(self) -> None:
        """graph Nonexistent exits with non-zero status and error message."""
        returncode, stdout, stderr = self._run_cli("graph", ["Nonexistent"])
        self.assertNotEqual(returncode, 0)
        self.assertIn("Unknown task", stdout + stderr)

    def test_graph_shared_child_appears(self) -> None:
        """graph with shared-child fixture shows SharedChild in output."""
        self._setup_shared_child_files()
        returncode, stdout, stderr = self._run_cli("graph")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("SharedChild", stdout)


class TestGraphOptions(CLITestCase):
    """Integration tests for graph --params, --status, and --format dot."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def test_graph_with_params(self) -> None:
        """graph --params includes parameter values in the output."""
        returncode, stdout, stderr = self._run_cli("graph", ["--params"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("split", stdout)

    def test_graph_with_status(self) -> None:
        """graph --status includes a completion indicator in the output."""
        returncode, stdout, stderr = self._run_cli("graph", ["--status"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertTrue("✓" in stdout or "✗" in stdout, f"No status indicator in: {stdout}")

    def test_graph_format_dot(self) -> None:
        """graph --format dot emits valid DOT with digraph keyword and edges."""
        returncode, stdout, stderr = self._run_cli("graph", ["--format", "dot"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("digraph", stdout)
        self.assertIn("->", stdout)

    def test_graph_format_dot_with_params(self) -> None:
        """graph --format dot --params includes label= in node definitions."""
        returncode, stdout, stderr = self._run_cli("graph", ["--format", "dot", "--params"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("label=", stdout)

    def test_graph_format_dot_shared_child_single_node(self) -> None:
        """graph --format dot with shared-child fixture: SharedChild appears as one DOT node."""
        self._setup_shared_child_files()
        returncode, stdout, stderr = self._run_cli("graph", ["--format", "dot"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("ParentA", stdout)
        self.assertIn("ParentB", stdout)
        # Exactly one DOT node definition for SharedChild (not an edge line)
        node_lines = [line for line in stdout.splitlines() if "SharedChild" in line and "->" not in line]
        self.assertEqual(len(node_lines), 1, f"Expected exactly one SharedChild node, got: {node_lines}")

        # Exactly 2 edges whose target is SharedChild
        edge_to_child = [line for line in stdout.splitlines() if "SharedChild" in line and "->" in line]
        self.assertEqual(len(edge_to_child), 2, f"Expected exactly 2 edges to SharedChild, got: {edge_to_child}")


class TestGraphSummary(CLITestCase):
    """Integration tests for graph --summary."""

    def setUp(self) -> None:
        super().setUp()
        self._setup_project_files()

    def _touch(self, filename: str) -> None:
        """Create *filename* in the project directory so its target exists."""
        with open(os.path.join(self.tmp_dir, filename), "w") as f:
            f.write("x")

    def test_summary_counts_per_class(self) -> None:
        """With one of two outputs present, each class reports its own count."""
        self._touch("leaf_1.txt")
        returncode, stdout, stderr = self._run_cli("graph", ["--summary"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertIn("RootTask", stdout)
        self.assertIn("1/2", stdout)
        self.assertIn("50%", stdout)

    def test_summary_replaces_the_tree(self) -> None:
        """--summary prints no tree; the tree's branch glyphs must be absent."""
        returncode, stdout, stderr = self._run_cli("graph", ["--summary"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        for glyph in ("└──", "├──"):
            self.assertNotIn(glyph, stdout)

    def test_summary_goes_to_stdout(self) -> None:
        """The summary is the command's primary output and belongs on stdout.

        Asserted against stdout alone, never `stdout + stderr` — the combined idiom
        passes either way and would not catch a stream regression.
        """
        returncode, stdout, stderr = self._run_cli("graph", ["--summary"])
        self.assertEqual(returncode, 0)
        self.assertIn("Task Graph Summary", stdout)
        self.assertNotIn("Task Graph Summary", stderr)

    def test_summary_scoped_to_one_task(self) -> None:
        """graph LeafTask --summary counts only that subtree."""
        returncode, stdout, stderr = self._run_cli("graph", ["LeafTask", "--summary"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("LeafTask", stdout)
        self.assertNotIn("RootTask", stdout)

    def test_summary_shared_child_counted_once(self) -> None:
        """The shared child contributes one instance, not one per parent."""
        self._setup_shared_child_files()
        returncode, stdout, stderr = self._run_cli("graph", ["--summary"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("SharedChild", stdout)
        self.assertIn("0/3", stdout)
        self.assertNotIn("0/4", stdout)

    def test_summary_with_status_is_accepted(self) -> None:
        """--status is redundant with --summary, not contradictory, so it is accepted."""
        returncode, stdout, stderr = self._run_cli("graph", ["--summary", "--status"])
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")
        self.assertIn("Task Graph Summary", stdout)

    def test_summary_with_format_dot_is_an_error(self) -> None:
        """--summary and --format dot cannot both apply; the error names both."""
        returncode, stdout, stderr = self._run_cli("graph", ["--summary", "--format", "dot"])
        self.assertEqual(returncode, 2)
        combined = stdout + stderr
        self.assertIn("--summary", combined)
        self.assertIn("--format dot", combined)
        self.assertNotIn("digraph", combined)

    def test_summary_with_params_is_an_error(self) -> None:
        """--summary and --params cannot both apply; the error names both."""
        returncode, stdout, stderr = self._run_cli("graph", ["--summary", "--params"])
        self.assertEqual(returncode, 2)
        combined = stdout + stderr
        self.assertIn("--summary", combined)
        self.assertIn("--params", combined)
