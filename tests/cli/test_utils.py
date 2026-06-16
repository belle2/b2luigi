"""Unit tests for b2luigi.cli.utils helper functions.

:Description: Tests the utility functions used by CLI apps, focusing on
    parameter parsing, classname validation, and task discovery.
"""

import pytest

from b2luigi.cli.utils import (
    parse_classnames,
    parse_kv_params,
    validate_classnames,
)
from b2luigi.cli.errors import CliUserError


class TestParseClassnames:
    """Tests for the parse_classnames utility function.

    :Description: Verifies correct behavior of classname parsing under various conditions.
    """

    def test_parse_classnames_normal(self) -> None:
        """Verify that parse_classnames splits comma-separated names correctly."""
        result = parse_classnames("A,B,C")
        assert result == ["A", "B", "C"]

    def test_parse_classnames_with_whitespace(self) -> None:
        """Verify that parse_classnames strips leading/trailing whitespace."""
        result = parse_classnames("A, B , C")
        assert result == ["A", "B", "C"]

    def test_parse_classnames_none(self) -> None:
        """Verify that parse_classnames returns None for None input."""
        result = parse_classnames(None)
        assert result is None

    def test_parse_classnames_empty_string(self) -> None:
        """Verify that parse_classnames returns None for empty string."""
        result = parse_classnames("")
        assert result is None

    def test_parse_classnames_whitespace_only(self) -> None:
        """Verify that parse_classnames returns None for whitespace-only input."""
        result = parse_classnames("  ,  ,  ")
        assert result is None

    def test_parse_classnames_single_name(self) -> None:
        """Verify that parse_classnames handles a single name correctly."""
        result = parse_classnames("SingleTask")
        assert result == ["SingleTask"]


class TestParseKvParams:
    """Tests for the parse_kv_params utility function.

    :Description: Verifies correct parsing of key=value parameter strings.
    """

    def test_parse_kv_params_normal(self) -> None:
        """Verify that parse_kv_params parses simple key=value pairs."""
        result = parse_kv_params(["name=value", "count=5"])
        assert result == {"name": "value", "count": 5}

    def test_parse_kv_params_json_parsing(self) -> None:
        """Verify that parse_kv_params parses JSON values correctly."""
        result = parse_kv_params(["enabled=true", "items=[1,2,3]", "ratio=1.5"])
        assert result == {"enabled": True, "items": [1, 2, 3], "ratio": 1.5}

    def test_parse_kv_params_quoted_strings(self) -> None:
        """Verify that parse_kv_params handles quoted strings as JSON."""
        result = parse_kv_params(['text="hello world"'])
        assert result == {"text": "hello world"}

    def test_parse_kv_params_plain_strings(self) -> None:
        """Verify that parse_kv_params keeps non-JSON strings as plain text."""
        result = parse_kv_params(["path=/some/path"])
        assert result == {"path": "/some/path"}

    def test_parse_kv_params_empty_list(self) -> None:
        """Verify that parse_kv_params handles an empty list."""
        result = parse_kv_params([])
        assert result == {}

    def test_parse_kv_params_missing_equals(self) -> None:
        """Verify that parse_kv_params raises CliUserError for missing equals sign."""
        with pytest.raises(CliUserError, match="Invalid --param.*Use key=value"):
            parse_kv_params(["invalid"])

    def test_parse_kv_params_empty_key(self) -> None:
        """Verify that parse_kv_params raises CliUserError for empty key."""
        with pytest.raises(CliUserError, match="Key is empty"):
            parse_kv_params(["=value"])

    def test_parse_kv_params_with_whitespace(self) -> None:
        """Verify that parse_kv_params strips whitespace around key and value."""
        result = parse_kv_params(["  name  =  value  "])
        assert result == {"name": "value"}


class TestValidateClassnames:
    """Tests for the validate_classnames utility function.

    :Description: Verifies correct behavior of classname validation with error handling.
    """

    def test_validate_classnames_passes(self) -> None:
        """Verify that validate_classnames accepts valid class names."""
        available = {"Task1": None, "Task2": None, "Task3": None}
        # Should not raise
        validate_classnames(["Task1", "Task2"], available)

    def test_validate_classnames_raises_on_unknown(self) -> None:
        """Verify that validate_classnames raises CliUserError for unknown names."""
        available = {"Task1": None, "Task2": None}
        with pytest.raises(CliUserError, match="Unknown task 'Nonexistent'"):
            validate_classnames(["Nonexistent"], available)

    def test_validate_classnames_suggestion(self) -> None:
        """Verify that validate_classnames provides typo suggestions."""
        available = {"MyTask": None, "OtherTask": None}
        # "MyTsk" is close to "MyTask" and should get a suggestion
        with pytest.raises(CliUserError, match="Did you mean 'MyTask'"):
            validate_classnames(["MyTsk"], available)

    def test_validate_classnames_no_suggestion_far_away(self) -> None:
        """Verify that validate_classnames skips suggestion for very different names."""
        available = {"TaskA": None, "TaskB": None}
        with pytest.raises(CliUserError, match="Unknown task 'xyz'"):
            validate_classnames(["xyz"], available)

    def test_validate_classnames_custom_hint_cmd(self) -> None:
        """Verify that validate_classnames uses custom hint command in error."""
        available = {"Task1": None}
        with pytest.raises(CliUserError, match="custom-command"):
            validate_classnames(["Unknown"], available, hint_cmd="custom-command")
