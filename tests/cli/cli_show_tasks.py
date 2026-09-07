"""Minimal tasks module for testing CLI show command.

:Description: Defines simple tasks with dependencies for integration tests.
"""

import b2luigi
import os


class LeafTask(b2luigi.Task):
    """A simple leaf task with no dependencies."""

    split = b2luigi.IntParameter()

    def output(self):
        return b2luigi.LocalTarget(f"leaf_{self.split}.txt")

    def run(self):
        os.makedirs(os.path.dirname(self.output().path) or ".", exist_ok=True)
        with self.output().open("w") as f:
            f.write(str(self.split))


class RootTask(b2luigi.Task):
    """A root task that depends on LeafTask."""

    split = b2luigi.IntParameter()

    def requires(self):
        return LeafTask(split=self.split)

    def output(self):
        return b2luigi.LocalTarget(f"root_{self.split}.txt")

    def run(self):
        os.makedirs(os.path.dirname(self.output().path) or ".", exist_ok=True)
        with self.output().open("w") as f:
            f.write("done")
