"""Unit tests for b2luigi.cli.utils helper functions.

:Description: Tests the utility functions used by CLI apps, focusing on
    parameter parsing, classname validation, and task discovery.
"""

import os
import shutil
import sys
import tempfile
from unittest import TestCase

import b2luigi
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    TaskIndex,
    build_task_index,
    parse_classnames,
    parse_kv_params,
    split_kv_params,
    try_instantiate,
    build_task_list,
    find_tasks_in_tree,
    load_parameters,
    load_task_class,
    resolve_task_context,
)


class TestParseClassnames(TestCase):
    """Tests for the parse_classnames utility function."""

    def test_parse_classnames_normal(self) -> None:
        """Verify that parse_classnames splits comma-separated names correctly."""
        self.assertEqual(parse_classnames("A,B,C"), ["A", "B", "C"])

    def test_parse_classnames_with_whitespace(self) -> None:
        """Verify that parse_classnames strips leading/trailing whitespace."""
        self.assertEqual(parse_classnames("A, B , C"), ["A", "B", "C"])

    def test_parse_classnames_none(self) -> None:
        """Verify that parse_classnames returns None for None input."""
        self.assertIsNone(parse_classnames(None))

    def test_parse_classnames_empty_string(self) -> None:
        """Verify that parse_classnames returns None for empty string."""
        self.assertIsNone(parse_classnames(""))

    def test_parse_classnames_whitespace_only(self) -> None:
        """Verify that parse_classnames returns None for whitespace-only input."""
        self.assertIsNone(parse_classnames("  ,  ,  "))

    def test_parse_classnames_single_name(self) -> None:
        """Verify that parse_classnames handles a single name correctly."""
        self.assertEqual(parse_classnames("SingleTask"), ["SingleTask"])


class TestParseKvParams(TestCase):
    """Tests for the parse_kv_params utility function."""

    def test_parse_kv_params_normal(self) -> None:
        """Verify that parse_kv_params parses simple key=value pairs."""
        self.assertEqual(parse_kv_params(["name=value", "count=5"]), {"name": "value", "count": 5})

    def test_parse_kv_params_json_parsing(self) -> None:
        """Verify that parse_kv_params parses JSON values correctly."""
        self.assertEqual(
            parse_kv_params(["enabled=true", "items=[1,2,3]", "ratio=1.5"]),
            {"enabled": True, "items": [1, 2, 3], "ratio": 1.5},
        )

    def test_parse_kv_params_quoted_strings(self) -> None:
        """Verify that parse_kv_params handles quoted strings as JSON."""
        self.assertEqual(parse_kv_params(['text="hello world"']), {"text": "hello world"})

    def test_parse_kv_params_plain_strings(self) -> None:
        """Verify that parse_kv_params keeps non-JSON strings as plain text."""
        self.assertEqual(parse_kv_params(["path=/some/path"]), {"path": "/some/path"})

    def test_parse_kv_params_empty_list(self) -> None:
        """Verify that parse_kv_params handles an empty list."""
        self.assertEqual(parse_kv_params([]), {})

    def test_parse_kv_params_missing_equals(self) -> None:
        """Verify that parse_kv_params raises CliUserError for missing equals sign."""
        with self.assertRaisesRegex(CliUserError, r"Invalid --param.*Use key=value"):
            parse_kv_params(["invalid"])

    def test_parse_kv_params_empty_key(self) -> None:
        """Verify that parse_kv_params raises CliUserError for empty key."""
        with self.assertRaisesRegex(CliUserError, "Key is empty"):
            parse_kv_params(["=value"])

    def test_parse_kv_params_with_whitespace(self) -> None:
        """Verify that parse_kv_params strips whitespace around key and value."""
        self.assertEqual(parse_kv_params(["  name  =  value  "]), {"name": "value"})


class TestTryInstantiate(TestCase):
    """Tests for the try_instantiate helper."""

    def test_returns_instance_when_params_sufficient(self) -> None:
        """Returns a task instance when all required params are provided."""

        class MyTask(b2luigi.Task):
            value = b2luigi.IntParameter()

        result = try_instantiate(MyTask, {"value": 7})
        self.assertIsInstance(result, MyTask)
        self.assertEqual(result.value, 7)

    def test_filters_out_unknown_params(self) -> None:
        """Ignores keys in params that the task class does not declare."""

        class MyTask(b2luigi.Task):
            value = b2luigi.IntParameter()

        result = try_instantiate(MyTask, {"value": 7, "other": 99})
        self.assertIsInstance(result, MyTask)

    def test_returns_none_when_required_param_missing(self) -> None:
        """Returns None when a required parameter (no default) is absent."""

        class MyTask(b2luigi.Task):
            value = b2luigi.IntParameter()

        result = try_instantiate(MyTask, {})
        self.assertIsNone(result)

    def test_returns_instance_when_missing_param_has_default(self) -> None:
        """Returns an instance when the only absent param has a default value."""

        class MyTask(b2luigi.Task):
            value = b2luigi.IntParameter(default=42)

        result = try_instantiate(MyTask, {})
        self.assertIsInstance(result, MyTask)
        self.assertEqual(result.value, 42)


class _ParentTask(b2luigi.Task):
    parent_param = b2luigi.IntParameter()

    def requires(self):
        return _ChildTask(child_param=self.parent_param * 2)

    def output(self):
        return b2luigi.LocalTarget(f"parent_{self.parent_param}.txt")


class _ChildTask(b2luigi.Task):
    child_param = b2luigi.IntParameter()

    def output(self):
        return b2luigi.LocalTarget(f"child_{self.child_param}.txt")


_AVAILABLE = {"_ParentTask": _ParentTask, "_ChildTask": _ChildTask}
_INDEX = TaskIndex(manifest=_AVAILABLE, project={}, task_file="tasks.py")
_PARAMS = [{"parent_param": 3}]  # child_param intentionally absent; wrapped as expand_parameters() output


class TestBuildTaskList(TestCase):
    """Tests for build_task_list path-selection logic."""

    def test_direct_path_when_params_sufficient(self) -> None:
        """Returns only the named target when its params are fully provided."""
        task_list, unresolved = build_task_list([_ParentTask], _INDEX, _PARAMS, direct_mode=False)
        self.assertEqual(len(task_list), 1)
        self.assertIsInstance(task_list[0], _ParentTask)
        self.assertEqual(unresolved, [])

    def test_discovery_path_when_params_missing(self) -> None:
        """Falls back to tree traversal and returns only the named target, not all roots."""
        task_list, unresolved = build_task_list([_ChildTask], _INDEX, _PARAMS, direct_mode=False)
        self.assertEqual(len(task_list), 1)
        self.assertIsInstance(task_list[0], _ChildTask)
        self.assertEqual(unresolved, [])

    def test_direct_mode_returns_unresolved_when_params_missing(self) -> None:
        """In direct mode, returns unresolved classes instead of falling back."""
        task_list, unresolved = build_task_list([_ChildTask], _INDEX, _PARAMS, direct_mode=True)
        self.assertEqual(task_list, [])
        self.assertIn(_ChildTask, unresolved)


class TestFindTasksInTree(TestCase):
    """Tests for the find_tasks_in_tree helper."""

    def test_returns_only_target_class_instances(self) -> None:
        """Returns only instances matching target_classes, not the roots themselves."""
        # _ParentTask(parent_param=3) requires _ChildTask(child_param=6)
        roots = [_ParentTask(parent_param=3)]
        result = find_tasks_in_tree({_ChildTask}, roots)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], _ChildTask)

    def test_returns_empty_list_when_no_match(self) -> None:
        """Returns [] when no task in the tree matches target_classes."""

        class _UnrelatedTask(b2luigi.Task):
            pass

        roots = [_ParentTask(parent_param=3)]
        result = find_tasks_in_tree({_UnrelatedTask}, roots)
        self.assertEqual(result, [])


class TestImportedTaskCollection(TestCase):
    """Tasks imported into tasks.py are as visible to the CLI as tasks defined there."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self) -> None:
        os.chdir(self.original_dir)
        shutil.rmtree(self.tmp_dir)
        for mod in ("analysis", "analysis.skim"):
            sys.modules.pop(mod, None)
        if self.tmp_dir in sys.path:
            sys.path.remove(self.tmp_dir)

    def _write(self, relpath: str, content: str) -> None:
        path = os.path.join(self.tmp_dir, relpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)

    def test_imported_tasks_are_collected(self) -> None:
        """A task imported into tasks.py from a sibling package is found alongside locally defined ones."""
        self._write("analysis/__init__.py", "")
        self._write(
            "analysis/skim.py",
            "import b2luigi\n\n\nclass SkimTask(b2luigi.Task):\n    pass\n",
        )
        self._write(
            "tasks.py",
            "from analysis.skim import SkimTask\n\nimport b2luigi\n\n\nclass LocalTask(b2luigi.Task):\n    pass\n",
        )
        index = build_task_index("tasks.py")
        self.assertEqual({cls.__name__ for cls in index.all_classes()}, {"LocalTask", "SkimTask"})

    def test_b2luigi_internals_are_not_collected(self) -> None:
        """Importing b2luigi's own classes into tasks.py does not make them CLI tasks."""
        self._write(
            "tasks.py",
            "from b2luigi import Task, WrapperTask\n\nimport b2luigi\n\n\nclass LocalTask(b2luigi.Task):\n    pass\n",
        )
        index = build_task_index("tasks.py")
        self.assertEqual({cls.__name__ for cls in index.all_classes()}, {"LocalTask"})

    def test_load_task_class_rejects_non_task_names(self) -> None:
        """A namespace member that is not a manifest task cannot be loaded for execution."""
        self._write(
            "tasks.py",
            "from b2luigi import Task\n\nimport b2luigi\n\n\nclass LocalTask(b2luigi.Task):\n    pass\n",
        )
        with self.assertRaises(AttributeError):
            load_task_class("Task", "tasks.py")


class TestLoadParametersMissingFile(TestCase):
    """Tests for load_parameters with missing file."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self) -> None:
        os.chdir(self.original_dir)
        shutil.rmtree(self.tmp_dir)

    def test_returns_empty_dict_when_file_absent(self) -> None:
        """Verify load_parameters returns {} when the file does not exist."""
        result = load_parameters("parameters.py")
        self.assertEqual(result, {})

    def test_still_raises_when_file_exists_but_has_no_config(self) -> None:
        """Verify load_parameters raises AttributeError when config variable is missing."""
        path = os.path.join(self.tmp_dir, "parameters.py")
        with open(path, "w") as f:
            f.write("# no config variable here\n")
        with self.assertRaises(AttributeError):
            load_parameters("parameters.py")


class TestResolveTaskContext(TestCase):
    """resolve_task_context merges --param over parameters.py."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self.original_dir = os.getcwd()
        os.chdir(self.tmp_dir)
        with open(os.path.join(self.tmp_dir, "tasks.py"), "w") as f:
            f.write(
                "import b2luigi\n"
                "import luigi\n"
                "\n"
                "class CtxTask(b2luigi.Task):\n"
                "    my_parameter = luigi.IntParameter()\n"
                "    other_parameter = luigi.Parameter()\n"
            )

    def tearDown(self) -> None:
        os.chdir(self.original_dir)
        shutil.rmtree(self.tmp_dir)

    def _write_params(self, body: str) -> None:
        with open(os.path.join(self.tmp_dir, "parameters.py"), "w") as f:
            f.write(body)

    def test_overrides_win_and_untouched_keys_survive(self) -> None:
        """A --param value replaces its config counterpart; other keys are kept."""
        self._write_params('config = {"my_parameter": 1, "other_parameter": "a"}\n')

        ctx = resolve_task_context(None, None, ["my_parameter=2"])

        self.assertIn("CtxTask", ctx.index.manifest)
        self.assertEqual(ctx.merged_params["my_parameter"], 2)
        self.assertEqual(ctx.merged_params["other_parameter"], "a")
        self.assertEqual(ctx.param_dicts, [{"my_parameter": 2, "other_parameter": "a"}])

    def test_scalar_override_collapses_a_generator_sweep(self) -> None:
        """Overriding a ParameterGenerator pins it to a single combination."""
        self._write_params(
            "from b2luigi import ParameterGenerator\n"
            'config = {"my_parameter": ParameterGenerator([1, 2, 3]), "other_parameter": "a"}\n'
        )

        without_override = resolve_task_context(None, None, [])
        self.assertEqual(len(without_override.param_dicts), 3)

        with_override = resolve_task_context(None, None, ["my_parameter=2"])
        self.assertEqual(with_override.param_dicts, [{"my_parameter": 2, "other_parameter": "a"}])

    def test_json_list_override_is_one_value_not_a_sweep(self) -> None:
        """A JSON list from --param is a single list value, not multiple combinations."""
        self._write_params('config = {"my_parameter": 1, "other_parameter": "a"}\n')

        ctx = resolve_task_context(None, None, ["my_parameter=[1,2,3]"])

        self.assertEqual(len(ctx.param_dicts), 1)
        self.assertEqual(ctx.param_dicts[0]["my_parameter"], [1, 2, 3])

    def test_works_without_a_parameters_file(self) -> None:
        """A missing parameters.py leaves --param as the only source of values."""
        ctx = resolve_task_context(None, None, ["my_parameter=7"])

        self.assertEqual(ctx.merged_params, {"my_parameter": 7})
        self.assertEqual(ctx.param_dicts, [{"my_parameter": 7}])


class TestSplitKvParams(TestCase):
    """split_kv_params returns raw strings and leaves typing to the caller."""

    def test_values_are_returned_as_raw_strings(self) -> None:
        """No JSON coercion: every value stays exactly as written."""
        self.assertEqual(
            split_kv_params(["n=5", "flag=true", "xs=[1,2]", "s=1.50"]),
            {"n": "5", "flag": "true", "xs": "[1,2]", "s": "1.50"},
        )

    def test_splits_on_first_equals_only(self) -> None:
        """Values may legitimately contain '='."""
        self.assertEqual(split_kv_params(["expr=a=b"]), {"expr": "a=b"})

    def test_missing_equals_is_an_error(self) -> None:
        """Validation matches parse_kv_params."""
        with self.assertRaises(CliUserError):
            split_kv_params(["noequals"])

    def test_empty_key_is_an_error(self) -> None:
        """Validation matches parse_kv_params."""
        with self.assertRaises(CliUserError):
            split_kv_params(["=value"])

    def test_value_whitespace_is_preserved(self) -> None:
        """Unlike parse_kv_params, values are returned byte-exact (no strip)."""
        self.assertEqual(split_kv_params(["  name  =  value  "]), {"name": "  value  "})

    def test_padded_parameter_round_trips_to_the_same_task_id(self) -> None:
        """A Parameter with legitimate leading/trailing whitespace must reconstruct
        to the same task_id on the worker as it had on the submitter — the exact
        failure this helper exists to prevent.
        """

        class PaddedTask(b2luigi.Task):
            some_parameter = b2luigi.Parameter()

        submitter = PaddedTask(some_parameter=" padded value ")
        params = [f"{k}={v}" for k, v in submitter.to_str_params().items()]
        worker = PaddedTask.from_str_params(split_kv_params(params))
        self.assertEqual(worker.task_id, submitter.task_id)


class TestParseKvParamsStillTypes(TestCase):
    """Regression guard: the user-facing --param path keeps JSON semantics."""

    def test_json_typing_is_unchanged(self) -> None:
        """parse_kv_params must still coerce, since its values reach constructors directly."""
        self.assertEqual(
            parse_kv_params(["n=5", "flag=true", "xs=[1,2]", "s=plain"]),
            {"n": 5, "flag": True, "xs": [1, 2], "s": "plain"},
        )
