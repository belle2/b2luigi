"""Unit tests for b2luigi.cli.utils helper functions.

:Description: Tests the utility functions used by CLI apps, focusing on
    parameter parsing, classname validation, and task discovery.
"""

from unittest import TestCase

from b2luigi.cli.errors import CliUserError
from b2luigi.cli.utils import parse_classnames, parse_kv_params, validate_classnames


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
