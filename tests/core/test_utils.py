import os
import sys
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

import b2luigi
import shlex
from b2luigi.core import utils
from b2luigi.core.utils import create_cmd_from_task, SYNTHETIC_TASK_MODULE
from ..helpers import B2LuigiTestCase


class WireQualifiedTask(b2luigi.Task):
    """Real, module-defined task class used to test module-qualified batch encoding."""

    pass


class ProductDictTestCase(TestCase):
    def test_basic_usage(self):
        kwargs = list(utils.product_dict(first_arg=[1, 2, 3], second_arg=["a", "b"]))

        self.assertEqual(len(kwargs), 6)

        self.assertIn({"first_arg": 1, "second_arg": "a"}, kwargs)
        self.assertIn({"first_arg": 1, "second_arg": "b"}, kwargs)
        self.assertIn({"first_arg": 2, "second_arg": "a"}, kwargs)
        self.assertIn({"first_arg": 2, "second_arg": "b"}, kwargs)
        self.assertIn({"first_arg": 3, "second_arg": "a"}, kwargs)
        self.assertIn({"first_arg": 3, "second_arg": "b"}, kwargs)

        kwargs = list(utils.product_dict(first_arg=[1, 2, 3]))

        self.assertEqual(len(kwargs), 3)

        self.assertIn({"first_arg": 1}, kwargs)
        self.assertIn({"first_arg": 2}, kwargs)
        self.assertIn({"first_arg": 3}, kwargs)

        kwargs = list(utils.product_dict(first_arg=[1, 2, 3], second_arg=[]))

        self.assertEqual(len(kwargs), 0)

        kwargs = list(utils.product_dict(first_arg=[1], second_arg=["a"]))

        self.assertEqual(len(kwargs), 1)

        self.assertIn({"first_arg": 1, "second_arg": "a"}, kwargs)


class FlattenTestCase(TestCase):
    def test_list_input(self):
        inputs = [{"key1": "value1"}, {"key2": "value2"}]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "value1")
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], "value2")

        inputs = [{"key1": "value1"}, {"key2": "value2"}, {"key1": "repeated"}]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "repeated")
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], "value2")

        inputs = []

        outputs = utils.flatten_to_dict(inputs)

        self.assertFalse(outputs)

        inputs = [{"key1": "value1"}, "value2"]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "value1")
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], "value2")

        inputs = ["value1", "value2"]

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("value1", outputs)
        self.assertEqual(outputs["value1"], "value1")
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], "value2")

    def test_raw_input(self):
        inputs = {"key1": "value1"}

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], "value1")

        inputs = "value1"

        outputs = utils.flatten_to_dict(inputs)

        self.assertIn("value1", outputs)
        self.assertEqual(outputs["value1"], "value1")

    def test_list_of_list_input(self):
        inputs = [{"key1": "value1"}, {"key2": "value2"}]

        outputs = utils.flatten_to_dict_of_lists(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1"])
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], ["value2"])

        inputs = [{"key1": "value1"}, {"key2": "value2"}, {"key1": "repeated"}]

        outputs = utils.flatten_to_dict_of_lists(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1", "repeated"])
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], ["value2"])

        inputs = []

        outputs = utils.flatten_to_dict_of_lists(inputs)

        self.assertFalse(outputs)

        inputs = [{"key1": "value1"}, "value2"]

        outputs = utils.flatten_to_dict_of_lists(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1"])
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], ["value2"])

        inputs = ["value1", "value2"]

        outputs = utils.flatten_to_dict_of_lists(inputs)

        self.assertIn("value1", outputs)
        self.assertEqual(outputs["value1"], ["value1"])
        self.assertIn("value2", outputs)
        self.assertEqual(outputs["value2"], ["value2"])

        inputs = [[{"key1": "value1"}, {"key2": "value2"}], [{"key1": "repeated"}]]

        outputs = utils.flatten_to_dict_of_lists(inputs)

        self.assertIn("key1", outputs)
        self.assertEqual(outputs["key1"], ["value1", "repeated"])
        self.assertIn("key2", outputs)
        self.assertEqual(outputs["key2"], ["value2"])


class OutputFileNameTestCase(B2LuigiTestCase):
    dummy_dir = "/foo/bar/"
    dummy_filename = "/foo/bar/run.py"
    dummy_parameter = "foo"

    def _get_dummy_task(self, parameter):
        class MyTask(b2luigi.Task):
            parameter = b2luigi.Parameter()

        task = MyTask(parameter)
        return task

    def test_output_file_name_basename(self):
        """
        Test that utils.create_output_file_name will return the expected output filename path
        """
        test_task = self._get_dummy_task(self.dummy_parameter)
        output = utils.create_output_file_name(
            test_task, base_filename="output.txt", result_dir=f"{self.test_dir}/results"
        )

        self.assertEqual(output, f"{self.test_dir}/results/parameter={self.dummy_parameter}/output.txt")

    def test_output_file_name_basename_alternate_separator(self):
        """
        Test that utils.create_output_file_name will return the expected output filename path
        """
        b2luigi.set_setting("parameter_separator", "_EQ_")

        test_task = self._get_dummy_task(self.dummy_parameter)
        output = utils.create_output_file_name(
            test_task, base_filename="output.txt", result_dir=f"{self.test_dir}/results"
        )
        b2luigi.clear_setting("parameter_separator")
        self.assertEqual(output, f"{self.test_dir}/results/parameter_EQ_{self.dummy_parameter}/output.txt")

    def test_output_file_name_basename_error(self):
        """
        Test that utils.create_output_file_name will raise an error if a parameter contains
        path separator "/" or is not interpretable as basename due to other reasons.
        """

        test_task_dir = self._get_dummy_task(self.dummy_dir)
        with self.assertRaises(ValueError):
            utils.create_output_file_name(test_task_dir, base_filename="output.txt")

        test_task_filename = self._get_dummy_task(self.dummy_filename)
        with self.assertRaises(ValueError):
            utils.create_output_file_name(test_task_filename, base_filename="output.txt")


class MapFolderTestCase(TestCase):
    dummy_rel_dir = "./some/rel_dir"
    dummy_abs_dir = "/path/to/some/abs_dir"
    main_no_file_file_err_msg = "module '__main__' has no attribute '__file__'"

    def test_map_folder_abspath_identity(self):
        """Test that for an absolute path, map_folder returns and identity"""
        self.assertEqual(utils.map_folder(self.dummy_abs_dir), self.dummy_abs_dir)

    def test_map_folder_relpath(self):
        """
        Test map_folder with a relative input_folder, which joins it with ``__main__.__file__``
        """
        with mock.patch("__main__.__file__", self.dummy_abs_dir):
            mapped_folder = utils.map_folder(self.dummy_rel_dir)
            self.assertEqual(mapped_folder, os.path.join(self.dummy_abs_dir, mapped_folder))

    def test_map_folder_abspath_identity_when_no_filename(self):
        """
        Test that for an absolute path, map_folder returns and identity even
        if ``get_filename`` would raise an ``AttributeError`` because ``__main__.__file__``
        is not available (e.g. in jupyter)
        """
        with mock.patch(
            "b2luigi.core.utils.get_filename",
            side_effect=AttributeError(self.main_no_file_file_err_msg),
        ):
            mapped_folder = utils.map_folder(self.dummy_abs_dir)
            self.assertEqual(mapped_folder, self.dummy_abs_dir)

    def test_map_folder_raises_attribute_error_for_relpath_when_no_filename(self):
        """
        Test that when ``get_filename`` returns an ``AttributeError`` b/c
        ``__main__.__file__`` is not available, ``map_folder`` also returns an
        ``AttributeError`` when the input folder is relative
        """
        with self.assertRaises(AttributeError):
            with mock.patch(
                "b2luigi.core.utils.get_filename",
                side_effect=AttributeError(self.main_no_file_file_err_msg),
            ):
                utils.map_folder(self.dummy_rel_dir)

    def _get_map_folder_error_mesage(self):
        """
        Get the error message that ``map_folder`` raises when ``get_filename``
        raises an attribute error because ``__main__.__file__`` is not
        accessible (e.g. in Jupyter)
        """
        try:
            with mock.patch(
                "b2luigi.core.utils.get_filename",
                side_effect=AttributeError(self.main_no_file_file_err_msg),
            ):
                utils.map_folder(self.dummy_rel_dir)
        except AttributeError as err:
            return str(err)
        raise RuntimeError("No AttributeError raised when calling ``utils.map_folder``")

    def test_original_message_in_error(self):
        """
        Check that the error message of ``map_folder`` still contains the
        original error message raised by ``get_filename`` during an
        ``AttributeError`` due to ``__main__.__file__`` not being accessible
        """
        message = self._get_map_folder_error_mesage()
        self.assertTrue(message.endswith(self.main_no_file_file_err_msg))

    def test_additional_info_added_to_error(self):
        """
        Check that the error message of ``map_folder`` adds additional
        information to the ``AttributeError`` raised by ``get_filename``
        """
        message = self._get_map_folder_error_mesage()
        self.assertTrue(message.startswith("Could not determine the current script location."))


class GetFilenameTestCase(TestCase):
    """
    Tests for ``get_filename``'s resolution of the task-definitions file,
    in particular its handling of ``python -m <package>`` invocations.
    """

    def test_direct_script_execution_returns_main_file(self):
        """
        When Python is invoked directly on a script (``python tasks.py``),
        ``__main__.__spec__`` is ``None`` and ``__main__.__file__`` is the
        script itself; ``get_filename`` should return that script path.
        """
        with mock.patch("__main__.__file__", "/some/project/tasks.py"), mock.patch("__main__.__spec__", None):
            self.assertEqual(utils.get_filename(), "/some/project/tasks.py")

    def test_module_invocation_with_py_main_file_falls_back_to_cwd(self):
        """
        When Python is invoked via ``-m`` on some package that happens to have
        a ``__main__.py`` (e.g. ``python -m b2luigi`` or ``python -m pytest``),
        ``__main__.__file__`` points at *that package's* ``__main__.py``
        (which ends in ``.py``) rather than the user's task file.
        ``get_filename`` must not mistake this for direct script execution;
        it should fall back to the ``cwd``-based ``tasks.py`` placeholder
        instead of returning the package's own ``__main__.py``.
        """
        with mock.patch("__main__.__file__", "/some/site-packages/some_package/__main__.py"), mock.patch(
            "__main__.__spec__", mock.MagicMock(name="some_package.__main__")
        ):
            self.assertEqual(utils.get_filename(), os.path.join(os.path.abspath(os.getcwd()), "tasks.py"))


class TaskIteratorTestCase(TestCase):
    def test_task_iterator_unique_tasks(self):
        """
        Test that even when multiple worker tasks require same common
        dependency task, it appears only once in task iterator output.
        """

        class CommonDependencyTask(b2luigi.ExternalTask):
            def output(self):
                return b2luigi.LocalTarget("some_dependency")

        @b2luigi.requires(CommonDependencyTask)
        class WorkerTask(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("output")

        class AggregatorTask(b2luigi.WrapperTask):
            def requires(self):
                for param in range(3):
                    yield self.clone(WorkerTask, some_parameter=param)

        expected_task_str_order = [
            "AggregatorTask()",
            "WorkerTask(some_parameter=0)",
            "CommonDependencyTask()",
            "WorkerTask(some_parameter=1)",
            "WorkerTask(some_parameter=2)",
        ]
        resulting_task_str_order = [str(t) for t in utils.task_iterator(AggregatorTask())]
        self.assertListEqual(expected_task_str_order, resulting_task_str_order)


class IsSubdirTestCase(TestCase):
    def test_is_subdir_true(self):
        self.assertTrue(utils.is_subdir("/path/to/child", "/path/to"))
        self.assertTrue(utils.is_subdir("/path/to/child/grandchild", "/path/to"))
        self.assertTrue(utils.is_subdir("/path/to/child", "/path/to/child"))
        self.assertTrue(utils.is_subdir("/path/to/child/", "/path/to"))
        self.assertTrue(utils.is_subdir("/path/to/child", "/path/to/"))

    def test_is_subdir_false(self):
        self.assertFalse(utils.is_subdir("/path/to/child", "/path/to/other"))
        self.assertFalse(utils.is_subdir("/path/to/child", "/path/to/child/grandchild"))
        self.assertFalse(utils.is_subdir("/path/to", "/path/to/child"))
        self.assertFalse(utils.is_subdir("/path/to/child", "/other/path/to"))

    def test_is_subdir_relative_paths(self):
        self.assertTrue(utils.is_subdir("child", "."))
        self.assertTrue(utils.is_subdir("child/grandchild", "."))
        self.assertFalse(utils.is_subdir(".", "child"))
        self.assertFalse(utils.is_subdir("child", "other"))


class CreateApptainerCommandTestCase(TestCase):
    def setUp(self):
        self.command = "echo Hello World"
        self.task = mock.Mock()
        self.env_setup_script = "/path/to/env_setup.sh"
        self.apptainer_image = "/path/to/apptainer_image.sif"
        self.result_dir = "/path/to/results"
        self.log_dir = "/path/to/logs"
        self.additional_params = "--nv"
        self.mounts = ["/mnt/data"]

        self.settings = {
            "env_script": self.env_setup_script,
            "apptainer_image": self.apptainer_image,
            "result_dir": self.result_dir,
            "log_dir": self.log_dir,
            "apptainer_additional_params": self.additional_params,
            "apptainer_mounts": self.mounts,
            "apptainer_mount_defaults": True,
            "batch_system": "lsf",
        }

    def test_create_apptainer_command(self):
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with mock.patch("b2luigi.core.utils.map_folder", side_effect=lambda x: x):
                with mock.patch("b2luigi.core.utils.get_log_file_dir", return_value=self.log_dir):
                    with mock.patch("os.makedirs"):
                        expected_command = [
                            "apptainer",
                            "exec",
                            f" {self.additional_params}",
                            "--bind",
                            self.mounts[0],
                            "--bind",
                            self.result_dir,
                            "--bind",
                            self.log_dir,
                            self.apptainer_image,
                            "/bin/bash",
                            "-c",
                            f"'source {self.env_setup_script} && {self.command}'",
                        ]
                        with mock.patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer"):
                            result = utils.create_apptainer_command(self.command, task=self.task)
                        self.assertEqual(result, shlex.split(" ".join(expected_command)))

    def test_create_apptainer_command_no_env_script(self):
        self.settings["env_script"] = ""
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with self.assertRaises(ValueError) as context:
                utils.create_apptainer_command(self.command, task=self.task)
            self.assertEqual(str(context.exception), "Apptainer execution requires an environment setup script.")

    def test_create_apptainer_command_invalid_batch_system(self):
        self.settings["batch_system"] = "gbasf2"
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with self.assertRaises(ValueError) as context:
                utils.create_apptainer_command(self.command, task=self.task)
            self.assertEqual(
                str(context.exception),
                "Invalid batch system for apptainer usage. Apptainer is not supported for gbasf2.",
            )

    def test_create_apptainer_command_additional_params_as_string(self):
        """A string ``apptainer_additional_params`` is word-split via shlex.split."""
        self.settings["apptainer_additional_params"] = "--cleanenv --nv"
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with mock.patch("b2luigi.core.utils.map_folder", side_effect=lambda x: x):
                with mock.patch("b2luigi.core.utils.get_log_file_dir", return_value=self.log_dir):
                    with mock.patch("os.makedirs"):
                        with mock.patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer"):
                            result = utils.create_apptainer_command(self.command, task=self.task)
                        self.assertIn("--cleanenv", result)
                        self.assertIn("--nv", result)
                        self.assertNotIn("--cleanenv --nv", result)

    def test_create_apptainer_command_additional_params_as_list(self):
        """A list ``apptainer_additional_params`` is used verbatim, no shlex.split involved."""
        self.settings["apptainer_additional_params"] = ["--cleanenv", "--nv"]
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with mock.patch("b2luigi.core.utils.map_folder", side_effect=lambda x: x):
                with mock.patch("b2luigi.core.utils.get_log_file_dir", return_value=self.log_dir):
                    with mock.patch("os.makedirs"):
                        with mock.patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer"):
                            result = utils.create_apptainer_command(self.command, task=self.task)
                        self.assertIn("--cleanenv", result)
                        self.assertIn("--nv", result)

    def test_create_apptainer_command_no_additional_params(self):
        self.settings["apptainer_additional_params"] = ""
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with mock.patch("b2luigi.core.utils.map_folder", side_effect=lambda x: x):
                with mock.patch("b2luigi.core.utils.get_log_file_dir", return_value=self.log_dir):
                    with mock.patch("os.makedirs"):
                        expected_command = [
                            "apptainer",
                            "exec",
                            "--bind",
                            self.mounts[0],
                            "--bind",
                            self.result_dir,
                            "--bind",
                            self.log_dir,
                            self.apptainer_image,
                            "/bin/bash",
                            "-c",
                            f"'source {self.env_setup_script} && {self.command}'",
                        ]
                        with mock.patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer"):
                            result = utils.create_apptainer_command(self.command, task=self.task)
                        self.assertEqual(result, shlex.split(" ".join(expected_command)))

    def test_create_apptainer_command_no_mounts(self):
        self.settings["apptainer_mounts"] = []
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with mock.patch("b2luigi.core.utils.map_folder", side_effect=lambda x: x):
                with mock.patch("b2luigi.core.utils.get_log_file_dir", return_value=self.log_dir):
                    with mock.patch("os.makedirs"):
                        expected_command = [
                            "apptainer",
                            "exec",
                            f" {self.additional_params}",
                            "--bind",
                            self.result_dir,
                            "--bind",
                            self.log_dir,
                            self.apptainer_image,
                            "/bin/bash",
                            "-c",
                            f"'source {self.env_setup_script} && {self.command}'",
                        ]
                        with mock.patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer"):
                            result = utils.create_apptainer_command(self.command, task=self.task)
                        self.assertEqual(result, shlex.split(" ".join(expected_command)))

    def test_create_apptainer_command_override_apptainer_name(self):
        with mock.patch(
            "b2luigi.core.utils.get_setting",
            side_effect=lambda key, **kwargs: self.settings.get(key, kwargs.get("default")),
        ):
            with mock.patch("b2luigi.core.utils.map_folder", side_effect=lambda x: x):
                with mock.patch("b2luigi.core.utils.get_log_file_dir", return_value=self.log_dir):
                    with mock.patch("os.makedirs"):
                        expected_command = [
                            "test_apptainer_cmd",
                            "exec",
                            f" {self.additional_params}",
                            "--bind",
                            self.mounts[0],
                            "--bind",
                            self.result_dir,
                            "--bind",
                            self.log_dir,
                            self.apptainer_image,
                            "/bin/bash",
                            "-c",
                            f"'source {self.env_setup_script} && {self.command}'",
                        ]
                        self.settings["apptainer_cmd"] = "test_apptainer_cmd"
                        result = utils.create_apptainer_command(self.command, task=self.task)
                        self.assertEqual(result, shlex.split(" ".join(expected_command)))
                        del self.settings["apptainer_cmd"]


def _mock_task(family="MyTask", task_id="MyTask_0_abc123", str_params=None):
    """Build a mock task whose class carries the synthetic task-file module.

    ``create_cmd_from_task`` reads ``type(task).__module__``/``__name__`` (not
    ``get_task_family()``) to build the wire classname, and a plain
    ``MagicMock``'s real ``type()`` cannot be spoofed via ``__class__``
    assignment (only ``isinstance()`` checks are fooled that way). A tiny
    real class with the synthetic module name reproduces the bare-name
    encoding these tests exercise; ``get_task_family``/``to_str_params``
    stay mocked since ``create_cmd_from_task`` still calls them.
    """
    task_cls = type(family, (), {"__module__": SYNTHETIC_TASK_MODULE})
    task = task_cls()
    task.get_task_family = MagicMock(return_value=family)
    task.task_id = task_id
    task.to_str_params = MagicMock(return_value=str_params or {})
    return task


def _make_get_setting(overrides=None):
    """Build a side_effect for get_setting that returns sensible defaults with optional overrides."""
    values = {
        "task_cmd_additional_args": [],
        "executable_prefix": [],
        "executable": [sys.executable],
        "batch_runner_cli": "b2luigi",
        "__batch_runner_use_cli": False,
        "__batch_runner_task_file": None,
        "__batch_runner_params_file": None,
        "add_filename_to_cmd": True,
    }
    if overrides:
        values.update(overrides)

    def _side_effect(key, task=None, default=None, deprecated_keys=None):
        return values.get(key, default)

    return _side_effect


class TestCreateCmdFromTask(TestCase):
    """Unit tests for create_cmd_from_task branching on __batch_runner_use_cli."""

    @patch("b2luigi.core.utils.get_filename", return_value="/abs/path/myscript.py")
    @patch("b2luigi.core.utils.get_setting")
    def test_old_mode_format(self, mock_gs, _mock_gf):
        """Old mode emits filename + --batch-runner + --task-id (no -m b2luigi)."""
        mock_gs.side_effect = _make_get_setting({"__batch_runner_use_cli": False})
        cmd = create_cmd_from_task(_mock_task(task_id="MyTask_0_abc123"))
        self.assertIn("myscript.py", cmd)
        self.assertIn("--batch-runner", cmd)
        self.assertIn("--task-id", cmd)
        self.assertIn("MyTask_0_abc123", cmd)
        self.assertNotIn("-m", cmd)

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_format(self, mock_gs):
        """New CLI mode emits -m b2luigi batch-runner --classname --param (no --batch-runner)."""
        mock_gs.side_effect = _make_get_setting({"__batch_runner_use_cli": True, "executable": [sys.executable]})
        cmd = create_cmd_from_task(_mock_task(str_params={"alpha": "1"}))
        self.assertIn("-m", cmd)
        self.assertIn("b2luigi", cmd)
        self.assertIn("batch-runner", cmd)
        self.assertIn("--classname", cmd)
        self.assertIn("MyTask", cmd)
        self.assertIn("--param", cmd)
        self.assertIn("alpha=1", cmd)
        m_idx = cmd.index("-m")
        self.assertEqual(cmd[m_idx : m_idx + 3], ["-m", "b2luigi", "batch-runner"])

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_appends_task_file(self, mock_gs):
        """--task-file is appended when __batch_runner_task_file is set."""
        mock_gs.side_effect = _make_get_setting(
            {
                "__batch_runner_use_cli": True,
                "__batch_runner_task_file": "/abs/path/myscript.py",
            }
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertIn("--task-file", cmd)
        idx = cmd.index("--task-file")
        self.assertEqual(cmd[idx + 1], "/abs/path/myscript.py")

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_no_task_file_when_unset(self, mock_gs):
        """--task-file is absent when __batch_runner_task_file is None."""
        mock_gs.side_effect = _make_get_setting({"__batch_runner_use_cli": True, "__batch_runner_task_file": None})
        cmd = create_cmd_from_task(_mock_task())
        self.assertNotIn("--task-file", cmd)

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_appends_params_file_as_given(self, mock_gs):
        """--params-file is appended verbatim when __batch_runner_params_file is set.

        Same convention as --task-file: a relative path stays relative so it
        resolves against working_dir on the worker; the encoder never absolutises.
        """
        mock_gs.side_effect = _make_get_setting(
            {"__batch_runner_use_cli": True, "__batch_runner_params_file": "conf/sweep.py"}
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertIn("--params-file", cmd)
        self.assertEqual(cmd[cmd.index("--params-file") + 1], "conf/sweep.py")

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_no_params_file_when_unset(self, mock_gs):
        """--params-file is absent when __batch_runner_params_file is None."""
        mock_gs.side_effect = _make_get_setting({"__batch_runner_use_cli": True, "__batch_runner_params_file": None})
        cmd = create_cmd_from_task(_mock_task())
        self.assertNotIn("--params-file", cmd)

    @patch("b2luigi.core.utils.get_filename", return_value="/abs/path/myscript.py")
    @patch("b2luigi.core.utils.get_setting")
    def test_add_filename_to_cmd_false_old_mode(self, mock_gs, _mock_gf):
        """add_filename_to_cmd=False omits the filename from the old-mode command."""
        mock_gs.side_effect = _make_get_setting({"__batch_runner_use_cli": False, "add_filename_to_cmd": False})
        cmd = create_cmd_from_task(_mock_task(task_id="MyTask_0_abc123"))
        self.assertNotIn("myscript.py", cmd)
        self.assertIn("--batch-runner", cmd)
        self.assertIn("--task-id", cmd)

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_task_cmd_additional_args_appended_last(self, mock_gs):
        """task_cmd_additional_args entries appear after all --param and --task-file entries."""
        mock_gs.side_effect = _make_get_setting(
            {
                "__batch_runner_use_cli": True,
                "task_cmd_additional_args": ["--extra", "val"],
            }
        )
        cmd = create_cmd_from_task(_mock_task(str_params={"k": "v"}))
        self.assertEqual(cmd[-2:], ["--extra", "val"])

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_defaults_to_entrypoint_when_executable_unset(self, mock_gs):
        """No executable override + no executable_is_entrypoint override -> entrypoint mode."""
        mock_gs.side_effect = _make_get_setting({"__batch_runner_use_cli": True, "executable": None})
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(cmd[0], "b2luigi")
        self.assertNotIn("-m", cmd)
        self.assertIn("batch-runner", cmd)
        self.assertEqual(cmd[1], "batch-runner")

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_explicit_entrypoint_false_keeps_m_mode(self, mock_gs):
        """executable unset but executable_is_entrypoint explicitly False -> old -m mode with sys.executable."""
        mock_gs.side_effect = _make_get_setting(
            {"__batch_runner_use_cli": True, "executable": None, "executable_is_entrypoint": False}
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(cmd[0], sys.executable)
        self.assertIn("-m", cmd)
        m_idx = cmd.index("-m")
        self.assertEqual(cmd[m_idx : m_idx + 3], ["-m", "b2luigi", "batch-runner"])

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_custom_executable_defaults_to_m_mode(self, mock_gs):
        """Custom executable set, executable_is_entrypoint unset -> old -m mode preserved (no breakage)."""
        mock_gs.side_effect = _make_get_setting(
            {"__batch_runner_use_cli": True, "executable": ["/custom/venv/bin/python3"]}
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(cmd[0], "/custom/venv/bin/python3")
        self.assertIn("-m", cmd)
        m_idx = cmd.index("-m")
        self.assertEqual(cmd[m_idx : m_idx + 3], ["-m", "b2luigi", "batch-runner"])

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_custom_executable_explicit_entrypoint_true(self, mock_gs):
        """Custom executable + executable_is_entrypoint=True -> entrypoint mode with the custom executable."""
        mock_gs.side_effect = _make_get_setting(
            {"__batch_runner_use_cli": True, "executable": ["flare"], "executable_is_entrypoint": True}
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(cmd[0], "flare")
        self.assertNotIn("-m", cmd)
        self.assertEqual(cmd[1], "batch-runner")

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_entrypoint_default_uses_batch_runner_cli(self, mock_gs):
        """executable unset, entrypoint mode default True -> default entrypoint name comes from batch_runner_cli."""
        mock_gs.side_effect = _make_get_setting(
            {"__batch_runner_use_cli": True, "executable": None, "batch_runner_cli": "flare"}
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(cmd[0], "flare")
        self.assertNotIn("-m", cmd)

    @patch("b2luigi.core.utils.get_setting")
    def test_new_cli_mode_entrypoint_ignores_batch_runner_cli_when_executable_set(self, mock_gs):
        """Custom executable + executable_is_entrypoint=True -> batch_runner_cli is ignored, not an error."""
        mock_gs.side_effect = _make_get_setting(
            {
                "__batch_runner_use_cli": True,
                "executable": ["flare"],
                "executable_is_entrypoint": True,
                "batch_runner_cli": "some_other_cli",
            }
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(cmd[0], "flare")
        self.assertNotIn("some_other_cli", cmd)

    @patch("b2luigi.core.utils.get_filename", return_value="/abs/path/myscript.py")
    @patch("b2luigi.core.utils.get_setting")
    def test_old_mode_ignores_executable_is_entrypoint(self, mock_gs, _mock_gf):
        """Legacy mode ignores executable_is_entrypoint; unset executable falls back to sys.executable."""
        mock_gs.side_effect = _make_get_setting(
            {"__batch_runner_use_cli": False, "executable": None, "executable_is_entrypoint": True}
        )
        cmd = create_cmd_from_task(_mock_task(task_id="MyTask_0_abc123"))
        self.assertEqual(cmd[0], sys.executable)
        self.assertIn("myscript.py", cmd)
        self.assertIn("--batch-runner", cmd)
        self.assertIn("--task-id", cmd)
        self.assertNotIn("batch-runner", [c for c in cmd if c != "--batch-runner"])

    def test_unset_sentinel_with_real_settings(self):
        """Test _UNSET sentinel fix: create_cmd_from_task doesn't raise when executable is unset."""
        # Set up required settings for CLI mode without setting executable
        b2luigi.set_setting("__batch_runner_use_cli", True)
        b2luigi.set_setting("result_dir", ".")
        # Global settings are not reset between tests, so an in-process CLI test
        # elsewhere in the suite can leave a stale task file behind. This test is
        # about the executable _UNSET sentinel, not the task-file encoding.
        b2luigi.clear_setting("__batch_runner_task_file")
        try:
            # Should not raise ValueError due to _UNSET sentinel fix
            task = _mock_task()
            cmd = create_cmd_from_task(task)
            # Verify the command was created successfully
            self.assertIsInstance(cmd, list)
            self.assertGreater(len(cmd), 0)
            # In CLI mode with no executable set and executable_is_entrypoint=True (default),
            # should default to the CLI module name (e.g., "b2luigi")
            self.assertEqual(cmd[0], "b2luigi")
        finally:
            # Clean up settings
            b2luigi.clear_setting("__batch_runner_use_cli")
            b2luigi.clear_setting("result_dir")


class TestCreateCmdFromTaskQuoting(TestCase):
    """create_cmd_from_task must emit tokens that survive shell word-splitting."""

    @staticmethod
    def _cli_settings(overrides=None):
        values = {
            "__batch_runner_use_cli": True,
            "executable": ["b2luigi"],
            "executable_is_entrypoint": True,
        }
        if overrides:
            values.update(overrides)
        return _make_get_setting(values)

    @patch("b2luigi.core.utils.get_setting")
    def test_list_parameter_survives_shell_split(self, mock_gs):
        """A ListParameter's serialized form contains spaces and must stay one token."""
        mock_gs.side_effect = self._cli_settings()
        cmd = create_cmd_from_task(_mock_task(str_params={"mylist": "[1, 2, 3]"}))
        self.assertEqual(
            shlex.split(" ".join(cmd)),
            ["b2luigi", "batch-runner", "--classname", "MyTask", "--param", "mylist=[1, 2, 3]"],
        )

    @patch("b2luigi.core.utils.get_setting")
    def test_plain_parameter_with_space_survives_shell_split(self, mock_gs):
        """Any parameter value containing a space is affected, not just list types."""
        mock_gs.side_effect = self._cli_settings()
        cmd = create_cmd_from_task(_mock_task(str_params={"name": "hello world"}))
        self.assertEqual(
            shlex.split(" ".join(cmd)),
            ["b2luigi", "batch-runner", "--classname", "MyTask", "--param", "name=hello world"],
        )

    @patch("b2luigi.core.utils.get_setting")
    def test_embedded_single_quote_survives_shell_split(self, mock_gs):
        """shlex.quote uses the '\\'' idiom; the value must come back byte-identical."""
        mock_gs.side_effect = self._cli_settings()
        cmd = create_cmd_from_task(_mock_task(str_params={"label": "it's here"}))
        self.assertEqual(
            shlex.split(" ".join(cmd)),
            ["b2luigi", "batch-runner", "--classname", "MyTask", "--param", "label=it's here"],
        )

    @patch("b2luigi.core.utils.get_setting")
    def test_task_file_path_with_space_survives_shell_split(self, mock_gs):
        """The --task-file path has the same exposure as --param values."""
        mock_gs.side_effect = self._cli_settings({"__batch_runner_task_file": "/my path/tasks.py"})
        cmd = create_cmd_from_task(_mock_task())
        argv = shlex.split(" ".join(cmd))
        self.assertEqual(argv[argv.index("--task-file") + 1], "/my path/tasks.py")

    @patch("b2luigi.core.utils.get_setting")
    def test_user_supplied_settings_are_not_quoted(self, mock_gs):
        """Negative control: executable_prefix and task_cmd_additional_args pass through verbatim."""
        mock_gs.side_effect = self._cli_settings(
            {"executable_prefix": ["time"], "task_cmd_additional_args": ["--flag=a b"]}
        )
        cmd = create_cmd_from_task(_mock_task())
        self.assertIn("time", cmd)
        self.assertIn("--flag=a b", cmd)

    @patch("b2luigi.core.utils.get_setting")
    def test_simple_values_are_left_unchanged(self, mock_gs):
        """shlex.quote is a no-op for tokens with no shell-significant characters."""
        mock_gs.side_effect = self._cli_settings()
        cmd = create_cmd_from_task(_mock_task(str_params={"alpha": "1"}))
        self.assertIn("alpha=1", cmd)


class TestCreateCmdFromTaskWireEncoding(B2LuigiTestCase):
    """create_cmd_from_task must encode the worker classname with module provenance.

    Classes defined in the task file itself (synthetic ``TaskClasses`` module)
    are sent bare; everything else (transitive imports, e.g. a task required
    by another module the task file imports) is sent module-qualified so the
    worker can resolve it through the task index instead of a plain
    ``getattr`` on the task file's namespace.
    """

    def setUp(self):
        super().setUp()
        b2luigi.set_setting("__batch_runner_use_cli", True)
        # Global settings survive across tests; this class asserts on the
        # classname token only, so drop any task file a previous test left set.
        b2luigi.clear_setting("__batch_runner_task_file")

    def tearDown(self):
        b2luigi.clear_setting("__batch_runner_use_cli")
        super().tearDown()

    def test_transitive_class_is_encoded_module_qualified(self):
        task = WireQualifiedTask()
        cmd = create_cmd_from_task(task)
        joined = " ".join(cmd)
        self.assertIn(f"--classname {WireQualifiedTask.__module__}.WireQualifiedTask", joined)

    def test_task_file_class_is_encoded_bare(self):
        cls = type("WireBareTask", (b2luigi.Task,), {"__module__": SYNTHETIC_TASK_MODULE})
        cmd = create_cmd_from_task(cls())
        joined = " ".join(cmd)
        self.assertIn("--classname WireBareTask", joined)
        self.assertNotIn(f"{SYNTHETIC_TASK_MODULE}.WireBareTask", joined)


def _make_apptainer_get_setting(overrides=None):
    """Settings side_effect for create_apptainer_command with mounts disabled."""
    values = {
        "env_script": "/env.sh",
        "apptainer_image": "/img.sif",
        "batch_system": "local",
        "apptainer_additional_params": "",
        "apptainer_mounts": None,
        "apptainer_mount_defaults": False,
    }
    if overrides:
        values.update(overrides)

    def _side_effect(key, task=None, default=None, deprecated_keys=None):
        return values.get(key, default)

    return _side_effect


class TestCreateApptainerCommandQuoting(TestCase):
    """create_apptainer_command must return a clean argv list and own no quoting."""

    @patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer")
    @patch("b2luigi.core.utils.get_setting")
    def test_payload_element_is_unquoted(self, mock_gs, _mock_ap):
        """The bash payload must not carry its own surrounding quotes."""
        mock_gs.side_effect = _make_apptainer_get_setting()
        cmd = utils.create_apptainer_command("b2luigi batch-runner --param 'x=1 2'")
        self.assertEqual(cmd[-1], "source /env.sh && b2luigi batch-runner --param 'x=1 2'")

    @patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer")
    @patch("b2luigi.core.utils.get_setting")
    def test_bash_c_is_followed_by_exactly_one_element(self, mock_gs, _mock_ap):
        """/bin/bash -c takes exactly one argument; the payload must be that one element."""
        mock_gs.side_effect = _make_apptainer_get_setting()
        cmd = utils.create_apptainer_command("echo hi")
        self.assertEqual(cmd[-3:], ["/bin/bash", "-c", "source /env.sh && echo hi"])

    @patch("b2luigi.core.utils.get_apptainer_or_singularity", return_value="apptainer")
    @patch("b2luigi.core.utils.get_setting")
    def test_additional_params_become_separate_argv_elements(self, mock_gs, _mock_ap):
        """apptainer_additional_params is a free-form string that must be word-split."""
        mock_gs.side_effect = _make_apptainer_get_setting({"apptainer_additional_params": "--cleanenv --nv"})
        cmd = utils.create_apptainer_command("echo hi")
        self.assertIn("--cleanenv", cmd)
        self.assertIn("--nv", cmd)
        self.assertNotIn(" --cleanenv --nv", cmd)


class TestTaskFileEncodingForWorker(TestCase):
    """The ``--task-file`` token keeps the shape the user asked for.

    b2luigi's convention: an absolute path the user supplied stays absolute, a
    relative one stays relative and is resolved against ``working_dir`` (which
    defaults to the directory you invoked b2luigi from). Applied here, that is
    also what makes a relocating ``working_dir`` work — the wrapper ``cd``s
    into a copy of the project and a relative token resolves inside it, where
    an absolute submission-host path would either not exist or, worse, point
    back at a different checkout on a shared filesystem.

    Absolutising the path here would take that choice away from the user, so
    the encoder passes it through untouched.
    """

    @staticmethod
    def _settings(task_file, working_dir=None):
        values = {
            "__batch_runner_use_cli": True,
            "executable": ["b2luigi"],
            "executable_is_entrypoint": True,
            "__batch_runner_task_file": task_file,
        }
        if working_dir is not None:
            values["working_dir"] = working_dir
        return _make_get_setting(values)

    @staticmethod
    def _task_file_token(cmd):
        return cmd[cmd.index("--task-file") + 1]

    @patch("b2luigi.core.utils.get_setting")
    def test_relative_task_file_stays_relative(self, mock_gs):
        """The default case: a relative path is what relocates to the worker."""
        mock_gs.side_effect = self._settings("tasks.py")
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(self._task_file_token(cmd), "tasks.py")

    @patch("b2luigi.core.utils.get_setting")
    def test_relative_subdirectory_is_preserved(self, mock_gs):
        """A relative path with a directory component keeps it."""
        mock_gs.side_effect = self._settings(os.path.join("sub", "tasks.py"))
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(self._task_file_token(cmd), os.path.join("sub", "tasks.py"))

    @patch("b2luigi.core.utils.get_setting")
    def test_absolute_task_file_stays_absolute(self, mock_gs):
        """An absolute path the user gave is honoured, not rewritten.

        ``b2luigi run --task-file /elsewhere/tasks.py --batch`` is legitimate
        and works locally; the batch path must not reject or relativise it.
        """
        mock_gs.side_effect = self._settings("/elsewhere/proj/tasks.py")
        cmd = create_cmd_from_task(_mock_task())
        self.assertEqual(self._task_file_token(cmd), "/elsewhere/proj/tasks.py")

    @patch("b2luigi.core.utils.get_setting")
    def test_working_dir_does_not_rewrite_the_token(self, mock_gs):
        """working_dir names a directory on the worker; it cannot rewrite a path here."""
        mock_gs.side_effect = self._settings("tasks.py", working_dir="/scratch/job42/proj")
        cmd = create_cmd_from_task(_mock_task())
        token = self._task_file_token(cmd)
        self.assertEqual(token, "tasks.py")
        self.assertNotIn("/scratch", token)
