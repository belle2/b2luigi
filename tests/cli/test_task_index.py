"""Tests for the CLI task index: membership discovery and name resolution.

:Description: The index unions the manifest (tasks.py namespace) with
    project-directory discovery, resolves bare and dotted names, and errors
    on ambiguity instead of silently picking a class.
"""

import gc
import os
import sys
import tempfile
import textwrap
from unittest import TestCase

from typer.testing import CliRunner

from b2luigi.cli import app
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import build_task_index


class TaskIndexTestBase(TestCase):
    """Builds the shared fixture project in a unique temp dir."""

    def setUp(self) -> None:
        self.proj = tempfile.mkdtemp()
        self.libdir = tempfile.mkdtemp()
        self._old_cwd = os.getcwd()
        self._added_paths: list[str] = []
        self._module_names = [
            "analysis_ti",
            "analysis_ti.skim",
            "analysis_ti.deep",
            "caliba_ti",
            "caliba_ti.prep",
            "fakelib_ti",
            "TaskClasses",
        ]

        os.makedirs(os.path.join(self.proj, "analysis_ti"))
        os.makedirs(os.path.join(self.proj, "caliba_ti"))
        self._write("analysis_ti/__init__.py", "")
        self._write("caliba_ti/__init__.py", "")
        self._write(
            "analysis_ti/deep.py",
            """
            import b2luigi

            class DeepTask(b2luigi.Task):
                number = b2luigi.IntParameter(default=1)
        """,
        )
        self._write(
            "analysis_ti/skim.py",
            """
            import b2luigi
            from analysis_ti.deep import DeepTask

            class SkimTask(b2luigi.Task):
                number = b2luigi.IntParameter(default=1)

                def requires(self):
                    return DeepTask(number=self.number)
        """,
        )
        self._write(
            "caliba_ti/prep.py",
            """
            import b2luigi

            class DeepTask(b2luigi.Task):
                label = b2luigi.Parameter(default="x")
        """,
        )
        with open(os.path.join(self.libdir, "fakelib_ti.py"), "w") as f:
            f.write("import b2luigi\n\nclass LibraryTask(b2luigi.Task):\n    pass\n")
        self._write(
            "tasks.py",
            """
            import b2luigi  # noqa: F401
            from analysis_ti.skim import SkimTask  # noqa: F401
            import caliba_ti.prep  # noqa: F401
            import fakelib_ti  # noqa: F401
        """,
        )
        sys.path.insert(0, self.libdir)
        self._added_paths.append(self.libdir)
        os.chdir(self.proj)

    def _write(self, rel: str, content: str) -> None:
        with open(os.path.join(self.proj, rel), "w") as f:
            f.write(textwrap.dedent(content))

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        for name in self._module_names:
            sys.modules.pop(name, None)
        real_added = {os.path.realpath(p) for p in self._added_paths + [self.proj]}
        sys.path[:] = [p for p in sys.path if os.path.realpath(p) not in real_added]
        gc.collect()


class TestTaskIndexMembership(TaskIndexTestBase):
    def test_manifest_contains_only_namespace_classes(self) -> None:
        index = build_task_index("tasks.py")
        self.assertEqual(set(index.manifest), {"SkimTask"})

    def test_project_set_contains_transitive_classes(self) -> None:
        index = build_task_index("tasks.py")
        self.assertIn("DeepTask", index.project)
        self.assertEqual(len(index.project["DeepTask"]), 2)

    def test_library_task_outside_project_dir_is_excluded(self) -> None:
        index = build_task_index("tasks.py")
        names = {cls.__name__ for cls in index.all_classes()}
        self.assertNotIn("LibraryTask", names)

    def test_manifest_class_not_duplicated_in_project_set(self) -> None:
        index = build_task_index("tasks.py")
        self.assertNotIn("SkimTask", index.project)
        self.assertEqual(sum(1 for cls in index.all_classes() if cls.__name__ == "SkimTask"), 1)


class TestTaskIndexResolution(TaskIndexTestBase):
    def test_bare_manifest_name_resolves(self) -> None:
        index = build_task_index("tasks.py")
        self.assertEqual(index.resolve("SkimTask").__name__, "SkimTask")

    def test_bare_ambiguous_name_raises_with_candidates(self) -> None:
        index = build_task_index("tasks.py")
        with self.assertRaises(CliUserError) as ctx:
            index.resolve("DeepTask")
        self.assertIn("Ambiguous", str(ctx.exception))
        self.assertIn("analysis_ti.deep.DeepTask", str(ctx.exception))
        self.assertIn("caliba_ti.prep.DeepTask", str(ctx.exception))

    def test_dotted_name_resolves_specific_class(self) -> None:
        index = build_task_index("tasks.py")
        cls = index.resolve("analysis_ti.deep.DeepTask")
        self.assertEqual(cls.__module__, "analysis_ti.deep")

    def test_qualified_name_of_manifest_class_resolves(self) -> None:
        index = build_task_index("tasks.py")
        # SkimTask is manifest; its qualified name must also resolve
        cls = index.resolve("analysis_ti.skim.SkimTask")
        self.assertEqual(cls.__name__, "SkimTask")

    def test_unknown_name_raises_with_hint(self) -> None:
        index = build_task_index("tasks.py")
        with self.assertRaises(CliUserError) as ctx:
            index.resolve("NoSuchTask")
        self.assertIn("Unknown task 'NoSuchTask'", str(ctx.exception))
        self.assertIn("b2luigi tasks", str(ctx.exception))

    def test_completion_names_qualify_only_ambiguous(self) -> None:
        index = build_task_index("tasks.py")
        names = index.completion_names()
        self.assertIn("SkimTask", names)
        self.assertNotIn("DeepTask", names)
        self.assertIn("analysis_ti.deep.DeepTask", names)
        self.assertIn("caliba_ti.prep.DeepTask", names)

    def test_display_module_never_leaks_synthetic_name(self) -> None:
        index = build_task_index("tasks.py")
        labels = {index.display_module(cls) for cls in index.all_classes()}
        self.assertNotIn("TaskClasses", labels)


class TestTaskIndexStaleClassFiltering(TaskIndexTestBase):
    def test_stale_classes_from_reimport_are_excluded(self) -> None:
        """Verify the identity check filters stale class objects from __subclasses__().

        When a module is re-imported in the same Python process, old class objects
        linger in Task.__subclasses__() even though new class objects are created.
        This test manufactures that scenario and verifies stale classes are excluded.
        """
        import importlib.util

        # Step 1: Build index and capture old class objects
        index1 = build_task_index("tasks.py")
        old_deep_task = next(c for c in index1.project["DeepTask"] if c.__module__ == "analysis_ti.deep")

        # Step 2: Manually create a NEW class object with the same module/name
        # by reloading the analysis_ti.deep module from disk
        analysis_ti_deep_path = os.path.join(self.proj, "analysis_ti", "deep.py")
        spec = importlib.util.spec_from_file_location("analysis_ti.deep", analysis_ti_deep_path)
        new_module = importlib.util.module_from_spec(spec)
        sys.modules["analysis_ti.deep"] = new_module
        spec.loader.exec_module(new_module)

        # Step 3: Verify we created a truly different class object
        new_cls_obj = new_module.DeepTask
        self.assertIsNot(
            old_deep_task,
            new_cls_obj,
            "Staleness was not manufactured: old and new class objects are identical",
        )

        # Step 4: Build index again
        # Now __subclasses__() contains BOTH old and new DeepTask objects:
        # - old_deep_task (the one from index1, no longer in sys.modules)
        # - new_cls_obj (the one currently in sys.modules[analysis_ti.deep])
        index2 = build_task_index("tasks.py")

        # Step 5: Verify the stale object is excluded
        self.assertNotIn(
            old_deep_task,
            index2.project["DeepTask"],
            "Stale class object from __subclasses__() was not filtered out",
        )

        # Step 6: Verify only the live class was included
        live_deep_tasks = [c for c in index2.project["DeepTask"] if c.__module__ == "analysis_ti.deep"]
        self.assertEqual(len(live_deep_tasks), 1, "Should have exactly 1 live analysis_ti.deep.DeepTask candidate")
        self.assertIs(live_deep_tasks[0], new_cls_obj, "The live class should be the one in sys.modules")


class TestTransitiveAddressability(TaskIndexTestBase):
    def setUp(self) -> None:
        super().setUp()
        with open(os.path.join(self.proj, "settings.json"), "w") as f:
            f.write('{"result_dir": "results"}\n')
        self.runner = CliRunner()

    def test_show_accepts_dotted_transitive_name(self) -> None:
        result = self.runner.invoke(app, ["show", "analysis_ti.deep.DeepTask"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("DeepTask", result.output)

    def test_show_bare_ambiguous_name_errors(self) -> None:
        result = self.runner.invoke(app, ["show", "DeepTask"])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Ambiguous", result.output)

    def test_graph_scopes_to_dotted_name(self) -> None:
        result = self.runner.invoke(app, ["graph", "analysis_ti.skim.SkimTask"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("SkimTask", result.output)

    def test_remove_keep_unknown_name_errors(self) -> None:
        result = self.runner.invoke(app, ["remove", "SkimTask", "-y", "--keep", "TypoTask"])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Unknown task 'TypoTask'", result.output)

    def test_remove_keep_accepts_dotted_transitive_name(self) -> None:
        result = self.runner.invoke(
            app, ["remove", "SkimTask", "-y", "--with-requirements", "--keep", "analysis_ti.deep.DeepTask"]
        )
        self.assertEqual(result.exit_code, 0, result.output)


class TestTasksListingAndRun(TaskIndexTestBase):
    def setUp(self) -> None:
        super().setUp()
        with open(os.path.join(self.proj, "settings.json"), "w") as f:
            f.write('{"result_dir": "results"}\n')
        self.runner = CliRunner()

    def test_tasks_lists_transitive_classes_with_module_column(self) -> None:
        result = self.runner.invoke(app, ["tasks"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("SkimTask", result.output)
        self.assertIn("analysis_ti.deep", result.output)
        self.assertIn("caliba_ti.prep", result.output)
        self.assertNotIn("TaskClasses", result.output)
        self.assertNotIn("LibraryTask", result.output)

    def test_tasks_info_accepts_dotted_name(self) -> None:
        result = self.runner.invoke(app, ["tasks", "info", "analysis_ti.deep.DeepTask"])
        self.assertEqual(result.exit_code, 0, result.output)

    def test_run_accepts_dotted_transitive_name(self) -> None:
        result = self.runner.invoke(app, ["run", "analysis_ti.deep.DeepTask", "--dry"])
        self.assertEqual(result.exit_code, 0, result.output)

    def test_run_bare_ambiguous_name_errors(self) -> None:
        result = self.runner.invoke(app, ["run", "DeepTask", "--dry"])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("Ambiguous", result.output)

    def test_completion_offers_qualified_names(self) -> None:
        from b2luigi.cli.utils import build_task_index

        names = build_task_index("tasks.py").completion_names()
        self.assertIn("analysis_ti.deep.DeepTask", names)
