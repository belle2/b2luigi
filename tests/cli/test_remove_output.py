from ..helpers import B2LuigiTestCase


class TestRemoveOutput(B2LuigiTestCase):
    def test_output_removed_mytask(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyTask", "-y"],
        )
        # Only the named task is removed; MyOtherTask (which requires MyTask) is untouched
        self.assertIn(b"MyTask", output)
        self.assertNotIn(b"MyOtherTask", output)

    def test_output_removed_mytask_only(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove-only", "MyTask", "-y"],
        )
        self.assertNotIn(b"MyOtherTask", output)
        self.assertIn(b"MyTask", output)

    def test_output_removed_mytask_keep(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyTask", "-y", "--keep", "MyOtherTask"],
        )
        # --keep MyOtherTask is a no-op here (MyOtherTask was never targeted),
        # but the removal of MyTask still proceeds
        self.assertIn(b"MyTask", output)
        self.assertNotIn(b"MyOtherTask", output)

    def test_output_removed_myothertask(self):
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyOtherTask", "-y"],
        )
        self.assertIn(b"MyOtherTask", output)
        self.assertNotIn(b"MyTask", output)
