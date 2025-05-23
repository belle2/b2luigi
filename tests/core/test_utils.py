import os
from unittest import TestCase, mock

import b2luigi
import shlex
from b2luigi.core import utils
from ..helpers import B2LuigiTestCase


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
