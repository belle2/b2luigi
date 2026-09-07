"""Task fixture for ParameterGenerator integration tests."""

import os

import b2luigi


class SimpleTask(b2luigi.Task):
    """A minimal task that writes its ``value`` parameter to a file."""

    value = b2luigi.IntParameter()

    def output(self):
        return b2luigi.LocalTarget(f"output_{self.value}.txt")

    def run(self):
        os.makedirs(os.path.dirname(self.output().path) or ".", exist_ok=True)
        with self.output().open("w") as f:
            f.write(str(self.value))
