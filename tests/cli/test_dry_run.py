from ..helpers import B2LuigiTestCase


class TestDryRun(B2LuigiTestCase):
    def test_dry_run_unfinished(self):
        output = self.call_file(
            "cli/process_dry_run_unfinished.py",
            cli_args=["--dry-run"],
        )
        self.assertIn(b"This is a dummy output.", output)

    def test_dry_run_finished(self):
        output = self.call_file(
            "cli/process_dry_run_finished.py",
            cli_args=["--dry-run"],
        )
        self.assertIn(b"All tasks are finished!", output)
