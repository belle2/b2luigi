from ..helpers import B2LuigiTestCase

import os

import b2luigi
from b2luigi.core.utils import get_filled_params, get_log_file_dir, get_task_file_dir


class TaskTestCase(B2LuigiTestCase):
    def test_file_path_usage(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("file_a")
                yield self.add_to_output("file_b")

        task = TaskA(some_parameter=3)

        b2luigi.set_setting("result_dir", "results/some_crazy_path")

        self.assertEqual(get_filled_params(task), {"some_parameter": 3})
        self.assertFalse(task.get_input_file_names())
        self.assertRaises(KeyError, lambda: task._get_input_targets("some_file"))
        self.assertEqual(task._get_output_target("file_a").path, task.get_output_file_name("file_a"))
        self.assertIn("file_a", task.get_output_file_name("file_a"))
        self.assertIn("file_b", task.get_output_file_name("file_b"))
        self.assertIn("some_parameter=3", task.get_output_file_name("file_a"))
        self.assertIn("some_crazy_path", task.get_output_file_name("file_a"))

    def test_dependencies(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("file_a")

        @b2luigi.requires(TaskA)
        class TaskB(b2luigi.Task):
            def output(self):
                yield self.add_to_output("file_b")

        task = TaskB(some_parameter=42)

        self.assertEqual(get_filled_params(task), {"some_parameter": 42})
        self.assertEqual(len(task._get_input_targets("file_a")), 1)
        self.assertEqual(len(task.get_input_file_names("file_a")), 1)
        self.assertEqual(len(task.get_input_file_names().keys()), 1)
        self.assertIn("some_parameter=42/file_a", task.get_input_file_name("file_a"))
        self.assertIn("some_parameter=42/file_a", task.get_input_file_name())
        self.assertEqual(task._get_input_targets("file_a")[0].path, task.get_input_file_names("file_a")[0])
        self.assertEqual(task._get_output_target("file_b").path, task.get_output_file_name("file_b"))
        self.assertIn("file_b", task.get_output_file_name("file_b"))
        self.assertIn("some_parameter=42", task.get_output_file_name("file_b"))
        self.assertEqual(len(list(task.get_all_input_file_names())), 1)
        self.assertEqual(len(list(task.get_all_output_file_names())), 1)
        self.assertIn("some_parameter=42/file_a", list(task.get_all_input_file_names())[0])
        self.assertIn("some_parameter=42/file_b", list(task.get_all_output_file_names())[0])

    def test_many_dependencies(self):
        class TaskA(b2luigi.Task):
            some_parameter = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("file_a")

        class TaskB(b2luigi.Task):
            def requires(self):
                for i in range(100):
                    yield self.clone(TaskA, some_parameter=i)

        task = TaskB()

        self.assertEqual(len(task._get_input_targets("file_a")), 100)
        self.assertEqual(len(task.get_input_file_names("file_a")), 100)
        self.assertEqual(len(task.get_input_file_names().keys()), 1)

        self.assertEqual(len(task.get_input_file_names()["file_a"]), 100)

        input_file_names = task.get_input_file_names("file_a")

        # We are only interested in the last 3 parts of the folder
        input_file_names = ["/".join(x.split("/")[-3:]) for x in input_file_names]

        for i in range(100):
            self.assertIn(f"results/some_parameter={i}/file_a", input_file_names)


class DefaultRemoveOutputTestCase(B2LuigiTestCase):
    """:meth:`b2luigi.Task.remove_output` exists by default and removes ``add_to_output`` targets."""

    def test_default_remove_output_deletes_add_to_output_targets(self):
        class MyTask(b2luigi.Task):
            def output(self):
                yield self.add_to_output("some_file.txt")

        b2luigi.set_setting("result_dir", "results")
        task = MyTask()
        path = task.get_output_file_name("some_file.txt")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("x")
        self.assertTrue(os.path.exists(path))

        task.remove_output()

        self.assertFalse(os.path.exists(path))


class RemoveLogAndTaskFilesTestCase(B2LuigiTestCase):
    """:meth:`b2luigi.Task.remove_log_files` and :meth:`b2luigi.Task.remove_task_files` defaults."""

    def setUp(self):
        super().setUp()
        self.log_base = os.path.join(self.test_dir, "logs")
        self.task_base = os.path.join(self.test_dir, "task_files")
        b2luigi.set_setting("log_dir", self.log_base)
        b2luigi.set_setting("task_file_dir", self.task_base)

    @staticmethod
    def _populate(directory):
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, "stdout"), "w") as f:
            f.write("x")

    def test_default_removes_populated_directories(self):
        class MyTask(b2luigi.Task):
            number = b2luigi.IntParameter()

        task = MyTask(number=3)
        log_dir = get_log_file_dir(task)
        task_dir = get_task_file_dir(task)
        self._populate(log_dir)
        self._populate(task_dir)

        task.remove_log_files()
        task.remove_task_files()

        self.assertFalse(os.path.exists(log_dir))
        self.assertFalse(os.path.exists(task_dir))
        self.assertTrue(os.path.isdir(self.log_base))
        self.assertTrue(os.path.isdir(self.task_base))

    def test_missing_directory_is_a_noop(self):
        class MyTask(b2luigi.Task):
            pass

        task = MyTask()
        self.assertFalse(os.path.exists(get_log_file_dir(task)))

        task.remove_log_files()
        task.remove_task_files()
