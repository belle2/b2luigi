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
