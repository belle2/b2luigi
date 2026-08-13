"""Unit tests for the graph summary renderer.

:Description: Exercises :func:`b2luigi.cli.runner.render_graph_summary` directly with
    locally-built task classes, so the counting, dedup and display-name rules can be
    tested without a project fixture on disk.

.. note::
    Classes built here via ``type()`` persist in luigi's task registry for the life of
    the pytest process, and their ``__module__`` (``"summary_fixtures"``, ``"mod_one"``,
    ``"mod_two"``) is deliberately not a real importable module. This is safe for
    project-task discovery: ``b2luigi/cli/utils.py:315-318`` catches the ``TypeError``
    that ``inspect.getfile(cls)`` raises for such a class, and
    ``b2luigi/cli/utils.py:494`` independently skips any class whose ``__module__``
    isn't in ``sys.modules``.
"""

import os
import shutil
import tempfile
from unittest import TestCase

import b2luigi
from rich.console import Console

from b2luigi.cli import runner


def _make_task_class(name: str, filename: str, module: str = "summary_fixtures") -> type:
    """Build a task class writing one output at *filename*, relative to the cwd.

    :param name: The class ``__name__``.
    :type name: str
    :param filename: Output filename, resolved relative to the current directory.
    :type filename: str
    :param module: The class ``__module__``, used by the display-name fallback.
    :type module: str
    :returns: A dynamically created :class:`b2luigi.Task` subclass.
    :rtype: type
    """

    def output(self):
        return b2luigi.LocalTarget(filename)

    return type(name, (b2luigi.Task,), {"__module__": module, "output": output})


def _make_no_output_class(name: str, requires_instance) -> type:
    """Build a task class declaring no outputs, requiring *requires_instance*.

    :param name: The class ``__name__``.
    :type name: str
    :param requires_instance: The single task instance this class requires.
    :returns: A dynamically created :class:`b2luigi.Task` subclass.
    :rtype: type
    """

    def output(self):
        return []

    def requires(self):
        return requires_instance

    return type(name, (b2luigi.Task,), {"__module__": "summary_fixtures", "output": output, "requires": requires})


class SummaryRenderTestCase(TestCase):
    """Base providing a temp cwd and a captured Rich console."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self._old_cwd = os.getcwd()
        os.chdir(self.tmp_dir)
        self._real_console = runner.console
        runner.console = Console(force_terminal=False, width=200, record=True)

    def tearDown(self) -> None:
        runner.console = self._real_console
        os.chdir(self._old_cwd)
        shutil.rmtree(self.tmp_dir)

    def _render(self, task_list: list) -> str:
        """Render *task_list* and return the captured console text."""
        runner.render_graph_summary(task_list)
        return runner.console.export_text()

    def _touch(self, filename: str) -> None:
        """Create *filename* in the temp cwd so its target reports as existing."""
        with open(filename, "w") as f:
            f.write("x")


class TestSummaryCounts(SummaryRenderTestCase):
    """Per-class counting, totals and the percentage."""

    def test_all_complete(self) -> None:
        """A class whose every instance has its output reports complete."""
        Leaf = _make_task_class("Leaf", "leaf.txt")
        self._touch("leaf.txt")
        rendered = self._render([Leaf()])
        self.assertIn("Leaf", rendered)
        self.assertIn("1/1", rendered)
        self.assertIn("complete", rendered)

    def test_incomplete_when_output_missing(self) -> None:
        """A class with a missing output reports incomplete."""
        Leaf = _make_task_class("Leaf", "leaf.txt")
        rendered = self._render([Leaf()])
        self.assertIn("0/1", rendered)
        self.assertIn("incomplete", rendered)

    def test_total_and_floored_percentage(self) -> None:
        """The total row reports instance counts and a floored integer percentage."""
        Leaf = _make_task_class("Leaf", "leaf.txt")
        Mid = _make_task_class("Mid", "mid.txt")
        Top = _make_task_class("Top", "top.txt")
        self._touch("leaf.txt")
        self._touch("mid.txt")
        rendered = self._render([Leaf(), Mid(), Top()])
        # 2 of 3 complete -> 66%, never 67 and never 66.6
        self.assertIn("2/3", rendered)
        self.assertIn("66%", rendered)
        self.assertNotIn("67%", rendered)

    def test_no_output_tasks_are_excluded(self) -> None:
        """A task declaring no outputs contributes no row and is absent from the total."""
        Leaf = _make_task_class("Leaf", "leaf.txt")
        self._touch("leaf.txt")
        Wrapper = _make_no_output_class("Wrapper", Leaf())
        rendered = self._render([Wrapper()])
        self.assertNotIn("Wrapper", rendered)
        self.assertIn("Leaf", rendered)
        self.assertIn("1/1", rendered)

    def test_empty_graph_reports_no_tasks(self) -> None:
        """A graph with no output-bearing tasks says so instead of dividing by zero.

        ``requires()`` returning ``None`` is valid — luigi resolves ``deps()`` to
        ``[]`` — so this is a graph of exactly one output-less task.
        """
        Solo = _make_no_output_class("Solo", None)
        rendered = self._render([Solo()])
        self.assertIn("No tasks with outputs to summarise.", rendered)

    def test_rows_follow_first_seen_traversal_order(self) -> None:
        """Rows appear in the order the walk first meets each class, roots first."""
        Leaf = _make_task_class("Leaf", "leaf.txt")
        Parent = _make_task_class("Parent", "parent.txt")
        Parent.requires = lambda self: Leaf()
        rendered = self._render([Parent()])
        self.assertLess(rendered.index("Parent"), rendered.index("Leaf"))


class TestSummaryDedup(SummaryRenderTestCase):
    """A shared dependency is counted once, however many parents require it."""

    def test_shared_child_counted_once(self) -> None:
        """Two roots requiring the same child yield one child instance in the counts."""
        Child = _make_task_class("Child", "child.txt")
        child = Child()
        ParentA = _make_no_output_class("ParentA", child)
        ParentB = _make_no_output_class("ParentB", child)
        rendered = self._render([ParentA(), ParentB()])
        # Child appears once: 0/1, not 0/2.
        self.assertIn("0/1", rendered)
        self.assertNotIn("0/2", rendered)


class TestSummaryDisplayNames(SummaryRenderTestCase):
    """Grouping is by class identity; names qualify only when they collide."""

    def test_distinct_names_stay_bare(self) -> None:
        """With no collision, rows show the bare class name."""
        Leaf = _make_task_class("Leaf", "leaf.txt")
        rendered = self._render([Leaf()])
        self.assertIn("Leaf", rendered)
        self.assertNotIn("summary_fixtures.Leaf", rendered)

    def test_colliding_names_are_qualified_and_both_counted(self) -> None:
        """Two same-named classes from different modules are counted separately.

        luigi builds ``task_id`` from the class NAME plus a parameter hash, with no
        module component, so these two instances share a ``task_id``. Deduping on
        ``task_id`` alone would drop one of them entirely — this test is what pins the
        ``(type(task), task_id)`` key.
        """
        First = _make_task_class("Collide", "first.txt", module="mod_one")
        Second = _make_task_class("Collide", "second.txt", module="mod_two")
        first, second = First(), Second()
        self.assertEqual(first.task_id, second.task_id, "precondition: luigi collides these task_ids")
        self._touch("first.txt")
        rendered = self._render([first, second])
        self.assertIn("mod_one.Collide", rendered)
        self.assertIn("mod_two.Collide", rendered)
        self.assertIn("1/2", rendered)
