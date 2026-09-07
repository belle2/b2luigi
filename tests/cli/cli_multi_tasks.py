"""Multi-param fixture: ParentTask (root) depends on ChildTask (non-root).

:Description: ParentTask has ``parent_param``; ChildTask has ``child_param``
    computed from the parent. parameters.py only contains ``parent_param``,
    so ChildTask cannot be directly instantiated from it.
"""

import b2luigi


class ChildTask(b2luigi.Task):
    """Leaf task with its own parameter, not present in parameters.py."""

    child_param = b2luigi.IntParameter()

    def output(self):
        return b2luigi.LocalTarget(f"child_{self.child_param}.txt")

    def run(self):
        with open(self.output().path, "w") as f:
            f.write(str(self.child_param))


class ParentTask(b2luigi.Task):
    """Root task whose requires() computes ChildTask's parameter."""

    parent_param = b2luigi.IntParameter()

    def requires(self):
        return ChildTask(child_param=self.parent_param * 2)

    def output(self):
        return b2luigi.LocalTarget(f"parent_{self.parent_param}.txt")

    def run(self):
        with open(self.output().path, "w") as f:
            f.write(str(self.parent_param))
