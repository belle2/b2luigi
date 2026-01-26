from ..helpers import B2LuigiTestCase


class TestDryRun(B2LuigiTestCase):
    def test_dry_run(self):
        self.call_file(
            "cli/process_dry_run.py",
        )
