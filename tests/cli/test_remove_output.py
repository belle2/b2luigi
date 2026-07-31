from ..helpers import B2LuigiTestCase


class TestRemoveOutput(B2LuigiTestCase):
    def test_output_removed_mytask_cascades_to_dependents(self):
        """Legacy --remove cascades: removing MyTask also removes MyOtherTask, which requires it."""
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyTask", "-y"],
        )
        self.assertIn(b"MyTask", output)
        self.assertIn(b"MyOtherTask", output)

    def test_output_removed_mytask_only(self):
        """--remove-only does NOT cascade: only MyTask itself is removed."""
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove-only", "MyTask", "-y"],
        )
        self.assertNotIn(b"MyOtherTask", output)
        self.assertIn(b"MyTask", output)

    def test_output_removed_mytask_keep_dependent(self):
        """--keep MyOtherTask preserves it even though cascade would otherwise remove it too."""
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyTask", "-y", "--keep", "MyOtherTask"],
        )
        self.assertIn(b"Keeping MyOtherTask outputs.", output)
        self.assertIn(b"Removed outputs for 1 tasks.", output)

    def test_output_removed_myothertask(self):
        """Removing MyOtherTask (nothing requires it) only removes itself, not its own requirement MyTask."""
        output = self.call_file(
            "cli/process_remove_output.py",
            cli_args=["--remove", "MyOtherTask", "-y"],
        )
        self.assertIn(b"MyOtherTask", output)
        self.assertNotIn(b"MyTask", output)
