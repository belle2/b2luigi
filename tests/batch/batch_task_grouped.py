import b2luigi


class MyGroupedTask(b2luigi.Task):
    """Task with one grouped and one plain parameter, used by the grouping tests."""

    plain = b2luigi.IntParameter(default=0)
    grouped = b2luigi.BatchIntParameter(default=0, grouping=True)

    max_grouping_size = 10

    def output(self):
        yield self.add_to_output("grouped.txt")

    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("grouped.txt"), "w") as f:
            f.write(f"{self.plain}-{self.grouped}")
