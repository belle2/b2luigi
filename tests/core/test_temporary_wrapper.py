import os
import shutil
import tempfile
import unittest
from unittest import mock


import b2luigi
from b2luigi.core.temporary_wrapper import EnsuredTemporaryScratchDirectory, TemporaryFileContextManager


from ..helpers import B2LuigiTestCase


class TemporaryFileContextManagerTestCase(B2LuigiTestCase):
    def test_get_output_file_name(self):
        class TaskA(b2luigi.Task):
            def output(self):
                yield self.add_to_output("final.txt")

            def run(self):
                with open(self.get_output_file_name("final.txt"), "w") as f:
                    f.write("Test")

        task = TaskA()
        non_temp_path = task.get_output_file_name("final.txt")
        with TemporaryFileContextManager(task):
            temp_path = task.get_output_file_name("final.txt")
            self.assertIsInstance(temp_path, str)
            # Using assertNotEqual as a dummy check here. The important part is, that the replaced function is called
            self.assertNotEqual(non_temp_path, temp_path)
            task.run()

    def test_get_input_file_name_simple(self):
        class TaskA(b2luigi.Task):
            def output(self):
                yield self.add_to_output("input.txt")

            @b2luigi.on_temporary_files
            def run(self):
                output_file = self.get_output_file_name("input.txt")
                with open(output_file, "w") as f:
                    f.write("Test")

        task_a = TaskA()
        task_a.run()

        @b2luigi.requires(TaskA)
        class TaskB(b2luigi.Task):
            pass

        task_b = TaskB()

        not_temp_path = task_b.get_input_file_names("input.txt")
        with TemporaryFileContextManager(task_b):
            temp_path = task_b.get_input_file_names("input.txt")
            self.assertIsInstance(temp_path, list)
            # Using assertNotEqual as a dummy check here. The important part is, that the replaced function is called
            self.assertNotEqual(not_temp_path, temp_path)

    def test_get_input_file_names_from_dict(self):
        class TaskA(b2luigi.Task):
            def output(self):
                yield self.add_to_output("input.txt")

            @b2luigi.on_temporary_files
            def run(self):
                output_file = self.get_output_file_name("input.txt")
                with open(output_file, "w") as f:
                    f.write("Test")

        task_a = TaskA()
        task_a.run()

        class TaskB(b2luigi.Task):
            def requires(self):
                return {"req1": TaskA(), "req2": TaskA()}

        task_b = TaskB()
        not_temp_path_req1 = task_b.get_input_file_names_from_dict("req1")
        not_temp_path_req2 = task_b.get_input_file_names_from_dict("req2")
        with TemporaryFileContextManager(task_b):
            temp_path_req1 = task_b.get_input_file_names_from_dict("req1")
            temp_path_req2 = task_b.get_input_file_names_from_dict("req2")
            self.assertIsInstance(temp_path_req1, list)
            self.assertIsInstance(temp_path_req2, list)
            # Using assertNotEqual as a dummy check here. The important part is, that the replaced function is called
            self.assertNotEqual(not_temp_path_req1, temp_path_req1)
            self.assertNotEqual(not_temp_path_req2, temp_path_req2)


class TestPatchedTemporaryDirectory(unittest.TestCase):
    def setUp(self):
        self.base_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def test_creates_parent_directory_if_missing(self):
        parent = os.path.join(self.base_dir, "nonexistent_parent")

        # Ensure parent doesn't exist beforehand
        self.assertFalse(os.path.exists(parent))

        with EnsuredTemporaryScratchDirectory(dir=parent) as tmpdir:
            self.assertTrue(os.path.exists(parent))
            self.assertTrue(os.path.isdir(tmpdir))
            self.assertTrue(tmpdir.startswith(parent))

    def test_works_when_parent_exists(self):
        parent = os.path.join(self.base_dir, "existing_parent")
        os.makedirs(parent)

        with EnsuredTemporaryScratchDirectory(dir=parent) as tmpdir:
            self.assertTrue(os.path.isdir(tmpdir))
            self.assertTrue(tmpdir.startswith(parent))

    @mock.patch("os.makedirs")
    def test_permission_error_message(self, mock_makedirs):
        mock_makedirs.side_effect = PermissionError()

        with self.assertRaises(PermissionError) as ctx:
            EnsuredTemporaryScratchDirectory(dir="/restricted/path")

        self.assertIn(
            "You do not have the permission to write to the temporary directory",
            str(ctx.exception),
        )


class OnTemporaryFilesFlagsTestCase(B2LuigiTestCase):
    """The ``inputs``/``outputs`` keyword arguments of :func:`b2luigi.on_temporary_files`."""

    def _make_producer(self):
        class Producer(b2luigi.Task):
            def output(self):
                yield self.add_to_output("input.txt")

            @b2luigi.on_temporary_files
            def run(self):
                with open(self.get_output_file_name("input.txt"), "w") as f:
                    f.write("Test")

        producer = Producer()
        producer.run()
        return Producer

    def _make_consumer(self, decorator):
        Producer = self._make_producer()

        @b2luigi.requires(Producer)
        class Consumer(b2luigi.Task):
            seen = {}

            def output(self):
                yield self.add_to_output("final.txt")

            @decorator
            def run(self):
                self.seen["inputs"] = self.get_input_file_names("input.txt")
                self.seen["all_inputs"] = list(self.get_all_input_file_names())
                self.seen["output"] = self.get_output_file_name("final.txt")
                with open(self.seen["output"], "w") as f:
                    f.write("Done")

        return Consumer()

    def test_bare_decorator_stages_inputs_and_outputs(self):
        task = self._make_consumer(b2luigi.on_temporary_files)
        real_inputs = task.get_input_file_names("input.txt")
        real_output = task.get_output_file_name("final.txt")

        task.run()

        self.assertNotEqual(task.seen["inputs"], real_inputs)
        self.assertNotEqual(task.seen["output"], real_output)
        self.assertTrue(os.path.exists(real_output))

    def test_inputs_false_reads_inputs_in_place(self):
        task = self._make_consumer(b2luigi.on_temporary_files(inputs=False))
        real_inputs = task.get_input_file_names("input.txt")
        real_all_inputs = list(task.get_all_input_file_names())
        real_output = task.get_output_file_name("final.txt")

        task.run()

        self.assertEqual(task.seen["inputs"], real_inputs)
        self.assertEqual(task.seen["all_inputs"], real_all_inputs)
        self.assertNotEqual(task.seen["output"], real_output)
        self.assertTrue(os.path.exists(real_output))

    def test_inputs_false_still_protects_output_on_failure(self):
        Producer = self._make_producer()

        @b2luigi.requires(Producer)
        class Consumer(b2luigi.Task):
            def output(self):
                yield self.add_to_output("final.txt")

            @b2luigi.on_temporary_files(inputs=False)
            def run(self):
                with open(self.get_output_file_name("final.txt"), "w") as f:
                    f.write("half")
                    raise ValueError()

        task = Consumer()
        with self.assertRaises(ValueError):
            task.run()

        self.assertFalse(os.path.exists(task.get_output_file_name("final.txt")))

    def test_outputs_false_writes_output_directly(self):
        task = self._make_consumer(b2luigi.on_temporary_files(outputs=False))
        real_inputs = task.get_input_file_names("input.txt")
        real_output = task.get_output_file_name("final.txt")

        task.run()

        self.assertNotEqual(task.seen["inputs"], real_inputs)
        self.assertEqual(task.seen["output"], real_output)
        self.assertTrue(os.path.exists(real_output))

    def test_both_false_is_a_passthrough(self):
        task = self._make_consumer(b2luigi.on_temporary_files(inputs=False, outputs=False))
        real_inputs = task.get_input_file_names("input.txt")
        real_output = task.get_output_file_name("final.txt")

        task.run()

        self.assertEqual(task.seen["inputs"], real_inputs)
        self.assertEqual(task.seen["output"], real_output)

    def test_methods_are_restored_after_run(self):
        task = self._make_consumer(b2luigi.on_temporary_files(inputs=False))

        task.run()

        self.assertIs(task.get_output_file_name.__func__, b2luigi.Task.get_output_file_name)
        self.assertIs(task.get_input_file_names.__func__, b2luigi.Task.get_input_file_names)

    def test_positional_flag_is_rejected(self):
        with self.assertRaises(TypeError):
            b2luigi.on_temporary_files(False)
