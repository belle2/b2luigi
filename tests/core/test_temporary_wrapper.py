import b2luigi

from b2luigi.core.temporary_wrapper import TemporaryFileContextManager

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
