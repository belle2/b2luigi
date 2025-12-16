import os

from ..helpers import B2LuigiTestCase

import b2luigi


class TemporaryWrapperTestCase(B2LuigiTestCase):
    def test_temporary_file_name(self):
        base_name = "a-complex_^name&114%"

        targetA = b2luigi.LocalTarget(f"{base_name}.foo")
        tmp_nameA = targetA.tmp_name
        self.assertTrue(tmp_nameA.startswith(f"{base_name}-luigi-tmp-"))
        self.assertTrue(tmp_nameA.endswith(".foo"))
        self.assertFalse(tmp_nameA.endswith("..foo"))
        self.assertFalse(tmp_nameA.endswith("foo."))

        targetB = b2luigi.LocalTarget(f"{base_name}.foo.bar")
        tmp_nameB = targetB.tmp_name
        self.assertTrue(tmp_nameB.startswith(f"{base_name}-luigi-tmp-"))
        self.assertTrue(tmp_nameB.endswith(".foo.bar"))
        self.assertFalse(tmp_nameB.endswith("..foo.bar"))
        self.assertFalse(tmp_nameB.endswith(".foo..bar"))

        targetC = b2luigi.LocalTarget(f"{base_name}")
        tmp_nameC = targetC.tmp_name
        self.assertTrue(tmp_nameC.startswith(f"{base_name}-luigi-tmp-"))
        self.assertFalse(tmp_nameC.endswith("."))

    def test_failed_temporary_files(self):
        self.call_file("core/temporary_task_1.py")

        self.assertFalse(os.path.exists("test.txt"))

    def test_good_temporary_files(self):
        self.call_file("core/temporary_task_2.py")

        self.assertTrue(os.path.exists("test.txt"))

        with open("test.txt", "r") as f:
            self.assertEqual(f.read(), "Test")

    def test_reset_output(self):
        class MyTask(b2luigi.Task):
            def output(self):
                yield self.add_to_output("test.txt")

            @b2luigi.on_temporary_files
            def run(self):
                with open(self.get_output_file_name("test.txt"), "w") as f:
                    f.write("Test")

        task = MyTask()

        self.assertTrue(task.get_output_file_name("test.txt").startswith(self.test_dir))

    def test_reset_input(self):
        class TaskA(b2luigi.Task):
            parameter_a = b2luigi.IntParameter()

            def output(self):
                yield self.add_to_output("resA.txt")

            @b2luigi.on_temporary_files
            def run(self):
                with open(self.get_output_file_name("resA.txt"), "w") as f:
                    f.write(f"TaskA: {self.parameter_a}")

        class MyTask(b2luigi.Task):
            def requires(self):
                return TaskA(parameter_a=1)

        task = MyTask()
        self.assertTrue(task.get_input_file_name("resA.txt").startswith(self.test_dir))
