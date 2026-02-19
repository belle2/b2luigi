from ..helpers import B2LuigiTestCase


class TestRemoveOutput(B2LuigiTestCase):
    def test_output_removed_mytask(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyTask", "-y"],
        )
        self.assertIn(b"Removing output for MyOtherTask()", output)
        self.assertIn(b"Removing output for MyTask()", output)

    def test_output_removed_mytask_only(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove-only", "MyTask", "-y"],
        )
        self.assertNotIn(b"Removing output for MyOtherTask()", output)
        self.assertIn(b"Removing output for MyTask()", output)

    def test_output_removed_mytask_keep(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyTask", "-y", "--keep", "MyOtherTask"],
        )
        self.assertNotIn(b"Removing output for MyOtherTask()", output)
        self.assertIn(b"Removing output for MyTask()", output)
        self.assertIn(b"Keeping MyOtherTask outputs", output)

    def test_output_removed_myothertask(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyOtherTask", "-y"],
        )
        self.assertIn(b"Removing output for MyOtherTask()", output)
        self.assertNotIn(b"Removing output for MyTask()", output)
