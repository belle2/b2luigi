"""Fixture: a grouped task and a local wrapper, used to exercise luigi batching through the CLI."""

import b2luigi


class GroupedTask(b2luigi.Task):
    grouped = b2luigi.BatchIntParameter(default=0, grouping=True)

    max_grouping_size = 5

    def output(self):
        yield self.add_to_output("out.txt")

    def run(self):
        with open(self.get_output_file_name("out.txt"), "w") as f:
            f.write(f"grouped={self.grouped}\n")


class RunAll(b2luigi.WrapperTask):
    batch_system = "local"

    def requires(self):
        return [GroupedTask(grouped=i) for i in range(3)]
