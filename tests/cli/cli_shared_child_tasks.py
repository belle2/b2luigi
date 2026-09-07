"""Fixture: two parent tasks (ParentA, ParentB) both requiring the same SharedChild.

:Description: Used to test that a child task shows multiple parents in its
    'required by' annotation when ``show --with-requirements`` is used.
"""

import b2luigi


class SharedChild(b2luigi.Task):
    """Child task required by both ParentA and ParentB."""

    param = b2luigi.IntParameter()

    def output(self):
        return b2luigi.LocalTarget(f"child_{self.param}.txt")

    def run(self):
        with open(self.output().path, "w") as f:
            f.write(str(self.param))


class ParentA(b2luigi.Task):
    """First parent task requiring SharedChild."""

    param = b2luigi.IntParameter()

    def requires(self):
        return SharedChild(param=self.param)

    def output(self):
        return b2luigi.LocalTarget(f"parent_a_{self.param}.txt")

    def run(self):
        with open(self.output().path, "w") as f:
            f.write("a")


class ParentB(b2luigi.Task):
    """Second parent task requiring SharedChild."""

    param = b2luigi.IntParameter()

    def requires(self):
        return SharedChild(param=self.param)

    def output(self):
        return b2luigi.LocalTarget(f"parent_b_{self.param}.txt")

    def run(self):
        with open(self.output().path, "w") as f:
            f.write("b")
