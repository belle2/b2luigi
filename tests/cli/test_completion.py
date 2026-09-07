"""Unit tests for the complete_task_names shell completion callback."""
import unittest
from unittest.mock import MagicMock, patch

from click import Context, Command


class _FakeIndex:
    """Minimal stand-in for TaskIndex, exposing only completion_names()."""

    def __init__(self, names: list[str]) -> None:
        self._names = names

    def completion_names(self) -> list[str]:
        return self._names


class TestCompleteTaskNames(unittest.TestCase):
    """Tests for complete_task_names completion callback."""

    def _make_ctx(self, params: dict | None = None) -> Context:
        cmd = Command("test")
        ctx = Context(cmd)
        if params:
            ctx.params.update(params)
        return ctx

    def test_returns_matching_names(self) -> None:
        """Names starting with the incomplete string are returned."""
        from b2luigi.cli.utils import complete_task_names

        ctx = self._make_ctx()
        param = MagicMock()

        with patch(
            "b2luigi.cli.utils.build_task_index", return_value=_FakeIndex(["MyTask", "MyOtherTask", "LeafTask"])
        ):
            result = complete_task_names(ctx, param, "My")

        self.assertEqual(len(result), 2)
        values = [item.value for item in result]
        self.assertIn("MyTask", values)
        self.assertIn("MyOtherTask", values)

    def test_empty_incomplete_returns_all(self) -> None:
        """Empty incomplete string returns all task names."""
        from b2luigi.cli.utils import complete_task_names

        ctx = self._make_ctx()
        param = MagicMock()

        with patch("b2luigi.cli.utils.build_task_index", return_value=_FakeIndex(["TaskA", "TaskB"])):
            result = complete_task_names(ctx, param, "")

        self.assertEqual(len(result), 2)

    def test_no_match_returns_empty_list(self) -> None:
        """Returns empty list when no name matches the prefix."""
        from b2luigi.cli.utils import complete_task_names

        ctx = self._make_ctx()
        param = MagicMock()

        with patch("b2luigi.cli.utils.build_task_index", return_value=_FakeIndex(["MyTask"])):
            result = complete_task_names(ctx, param, "ZZZ")

        self.assertEqual(result, [])

    def test_import_error_returns_empty_list(self) -> None:
        """Exceptions from build_task_index are swallowed; empty list is returned."""
        from b2luigi.cli.utils import complete_task_names

        ctx = self._make_ctx()
        param = MagicMock()

        with patch("b2luigi.cli.utils.build_task_index", side_effect=Exception("no tasks.py")):
            result = complete_task_names(ctx, param, "My")

        self.assertEqual(result, [])

    def test_uses_task_file_from_ctx_params(self) -> None:
        """task_filename from ctx.params is forwarded to build_task_index."""
        from b2luigi.cli.utils import complete_task_names

        ctx = self._make_ctx(params={"task_filename": "custom_tasks.py"})
        param = MagicMock()

        with patch("b2luigi.cli.utils.build_task_index") as mock_build:
            mock_build.return_value = _FakeIndex([])
            complete_task_names(ctx, param, "")

        mock_build.assert_called_once_with("custom_tasks.py")

    def test_uses_env_var_when_no_ctx_param(self) -> None:
        """B2LUIGI_TASK_FILE env var is used when task_filename is not in ctx.params."""
        import os
        from b2luigi.cli.utils import complete_task_names

        ctx = self._make_ctx()
        param = MagicMock()

        with patch.dict(os.environ, {"B2LUIGI_TASK_FILE": "env_tasks.py"}):
            with patch("b2luigi.cli.utils.build_task_index") as mock_build:
                mock_build.return_value = _FakeIndex([])
                complete_task_names(ctx, param, "")

        mock_build.assert_called_once_with("env_tasks.py")


if __name__ == "__main__":
    unittest.main()
