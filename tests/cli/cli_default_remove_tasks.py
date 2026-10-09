"""Tasks module for testing the default ``remove_output`` implementation.

:Description: Two-task chain declaring outputs via ``add_to_output`` and
    overriding nothing else, so ``b2luigi remove`` exercises the default
    ``Task.remove_output`` inherited from :class:`b2luigi.Task`.
"""

import b2luigi


class LeafTask(b2luigi.Task):
    """A leaf task declaring one output via ``add_to_output``."""

    split = b2luigi.IntParameter()

    def output(self):
        yield self.add_to_output("leaf.txt")

    def run(self):
        with open(self.get_output_file_name("leaf.txt"), "w") as f:
            f.write(str(self.split))


@b2luigi.requires(LeafTask)
class RootTask(b2luigi.Task):
    """A root task declaring one output via ``add_to_output``."""

    def output(self):
        yield self.add_to_output("root.txt")

    def run(self):
        with open(self.get_output_file_name("root.txt"), "w") as f:
            f.write("done")
