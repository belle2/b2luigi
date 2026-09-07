"""Tests that process_task_instance correctly arms the internal batch-runner settings."""

from unittest import TestCase
from unittest.mock import MagicMock, patch


class TestProcessTaskInstanceSettings(TestCase):
    """Verify that process_task_instance sets __batch_runner_use_cli and __batch_runner_task_file."""

    @patch("b2luigi.cli.utils.b2luigi.process")
    @patch("b2luigi.cli.utils.set_setting")
    def test_always_sets_use_cli_flag(self, mock_set_setting, _mock_process):
        """process_task_instance always sets __batch_runner_use_cli=True."""
        from b2luigi.cli.utils import process_task_instance

        process_task_instance(MagicMock())
        mock_set_setting.assert_any_call("__batch_runner_use_cli", True)

    @patch("b2luigi.cli.utils.b2luigi.process")
    @patch("b2luigi.cli.utils.set_setting")
    def test_sets_task_file_when_provided(self, mock_set_setting, _mock_process):
        """process_task_instance sets __batch_runner_task_file when task_file is given."""
        from b2luigi.cli.utils import process_task_instance

        process_task_instance(MagicMock(), task_file="/abs/path/myscript.py")
        mock_set_setting.assert_any_call("__batch_runner_task_file", "/abs/path/myscript.py")

    @patch("b2luigi.cli.utils.b2luigi.process")
    @patch("b2luigi.cli.utils.set_setting")
    def test_does_not_set_task_file_when_none(self, mock_set_setting, _mock_process):
        """process_task_instance does not call set_setting for __batch_runner_task_file when task_file=None."""
        from b2luigi.cli.utils import process_task_instance

        process_task_instance(MagicMock())
        called_keys = [c.args[0] for c in mock_set_setting.call_args_list]
        self.assertNotIn("__batch_runner_task_file", called_keys)

    @patch("b2luigi.cli.utils.b2luigi.process")
    @patch("b2luigi.cli.utils.set_setting")
    def test_sets_params_file_when_provided(self, mock_set_setting, _mock_process):
        """process_task_instance sets __batch_runner_params_file when params_file is given."""
        from b2luigi.cli.utils import process_task_instance

        process_task_instance(MagicMock(), params_file="sweep.py")
        mock_set_setting.assert_any_call("__batch_runner_params_file", "sweep.py")

    @patch("b2luigi.cli.utils.b2luigi.process")
    @patch("b2luigi.cli.utils.set_setting")
    def test_does_not_set_params_file_when_none(self, mock_set_setting, _mock_process):
        """process_task_instance does not touch __batch_runner_params_file when params_file=None."""
        from b2luigi.cli.utils import process_task_instance

        process_task_instance(MagicMock())
        called_keys = [c.args[0] for c in mock_set_setting.call_args_list]
        self.assertNotIn("__batch_runner_params_file", called_keys)
