"""Unit tests for b2luigi.cli.utils helper functions.

:Description: Tests the utility functions used by CLI apps, focusing on
    parameter parsing, classname validation, and task discovery.
"""

import os
import shutil
import tempfile
from unittest import TestCase

import b2luigi
from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import (
    parse_classnames,
    parse_kv_params,
    validate_classnames,
    try_instantiate,
    build_task_list,
    find_tasks_in_tree,
    load_parameters,
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


class TestValidateClassnames(TestCase):
    """Tests for the validate_classnames utility function."""

    def test_validate_classnames_passes(self) -> None:
        """Verify that validate_classnames accepts valid class names."""
        available = {"Task1": None, "Task2": None, "Task3": None}
        validate_classnames(["Task1", "Task2"], available)  # must not raise

    def test_validate_classnames_raises_on_unknown(self) -> None:
        """Verify that validate_classnames raises CliUserError for unknown names."""
        available = {"Task1": None, "Task2": None}
        with self.assertRaisesRegex(CliUserError, "Unknown task 'Nonexistent'"):
            validate_classnames(["Nonexistent"], available)

    def test_validate_classnames_suggestion(self) -> None:
        """Verify that validate_classnames provides typo suggestions."""
        available = {"MyTask": None, "OtherTask": None}
        with self.assertRaisesRegex(CliUserError, "Did you mean 'MyTask'"):
            validate_classnames(["MyTsk"], available)

    def test_validate_classnames_no_suggestion_far_away(self) -> None:
        """Verify that validate_classnames skips suggestion for very different names."""
        available = {"TaskA": None, "TaskB": None}
        with self.assertRaisesRegex(CliUserError, "Unknown task 'xyz'"):
            validate_classnames(["xyz"], available)

    def test_validate_classnames_custom_hint_cmd(self) -> None:
        """Verify that validate_classnames uses custom hint command in error."""
        available = {"Task1": None}
        with self.assertRaisesRegex(CliUserError, "custom-command"):
            validate_classnames(["Unknown"], available, hint_cmd="custom-command")


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
_PARAMS = [{"parent_param": 3}]  # child_param intentionally absent; wrapped as expand_parameters() output


class TestBuildTaskList(TestCase):
    """Tests for build_task_list path-selection logic."""

    def test_direct_path_when_params_sufficient(self) -> None:
        """Returns only the named target when its params are fully provided."""
        task_list, unresolved = build_task_list(["_ParentTask"], _AVAILABLE, _PARAMS, direct_mode=False)
        self.assertEqual(len(task_list), 1)
        self.assertIsInstance(task_list[0], _ParentTask)
        self.assertEqual(unresolved, set())

    def test_discovery_path_when_params_missing(self) -> None:
        """Falls back to tree traversal and returns only the named target, not all roots."""
        task_list, unresolved = build_task_list(["_ChildTask"], _AVAILABLE, _PARAMS, direct_mode=False)
        self.assertEqual(len(task_list), 1)
        self.assertIsInstance(task_list[0], _ChildTask)
        self.assertEqual(unresolved, set())

    def test_direct_mode_returns_unresolved_when_params_missing(self) -> None:
        """In direct mode, returns unresolved names instead of falling back."""
        task_list, unresolved = build_task_list(["_ChildTask"], _AVAILABLE, _PARAMS, direct_mode=True)
        self.assertEqual(task_list, [])
        self.assertIn("_ChildTask", unresolved)


class TestFindTasksInTree(TestCase):
    """Tests for the find_tasks_in_tree helper."""

    def test_returns_only_target_class_instances(self) -> None:
        """Returns only instances matching target_names, not the roots themselves."""
        # _ParentTask(parent_param=3) requires _ChildTask(child_param=6)
        roots = [_ParentTask(parent_param=3)]
        result = find_tasks_in_tree({"_ChildTask"}, roots)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], _ChildTask)

    def test_returns_empty_list_when_no_match(self) -> None:
        """Returns [] when no task in the tree matches target_names."""
        roots = [_ParentTask(parent_param=3)]
        result = find_tasks_in_tree({"NonExistent"}, roots)
        self.assertEqual(result, [])


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
        result = load_parameters("parameters.py")
        self.assertEqual(result, {})

    def test_still_raises_when_file_exists_but_has_no_config(self) -> None:
        path = os.path.join(self.tmp_dir, "parameters.py")
        with open(path, "w") as f:
            f.write("# no config variable here\n")
        with self.assertRaises(AttributeError):
            load_parameters("parameters.py")
