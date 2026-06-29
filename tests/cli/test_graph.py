"""Integration tests for b2luigi graph CLI command.

:Description: Tests the ``b2luigi graph`` command end-to-end, covering full
    graph rendering, task scoping, and error handling.
"""

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
        self.assertIn("SharedChild", stdout)
        self.assertIn("ParentA", stdout)
        self.assertIn("ParentB", stdout)
        # DOT has two edges (ParentA->SharedChild and ParentB->SharedChild)
        self.assertGreaterEqual(stdout.count("->"), 2)
