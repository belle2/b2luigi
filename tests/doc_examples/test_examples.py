import os
from glob import glob
from pathlib import Path

from ..helpers import B2LuigiTestCase


class ExampleTestCase(B2LuigiTestCase):
    def test_examples(self):
        test_file_dir = Path(__file__).parent.resolve()

        for file_name in glob(os.path.join(os.path.dirname(__file__), "*.py")):
            short_file_name = os.path.basename(file_name)

            if short_file_name.startswith("test_") or short_file_name.startswith("__"):
                continue

            self.call_file(os.path.join(test_file_dir, os.path.basename(file_name)))

    def test_tutorials(self):
        tutorials = [
            "Ex01_basics_b2luigi_task.py",
            "Ex02_basics_b2luigi_require.py",
            "Ex03_basics_b2luigi_wrappertask.py",
            "Ex04_basics_b2luigi_averagetask.py",
        ]

        test_file_dir = Path(__file__).parent.resolve()

        tutorial_dir = test_file_dir.parents[0] / "examples" / "tutorial"

        for tutorial in tutorials:
            tutorial_file = tutorial_dir / tutorial

            self.call_file(os.path.relpath(tutorial_file, start=test_file_dir))
