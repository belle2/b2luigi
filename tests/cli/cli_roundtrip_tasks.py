"""Task fixture for batch-runner parameter round-trip tests."""

import b2luigi
import luigi


class RoundTripTask(b2luigi.Task):
    """Task whose parameter value is valid JSON but must stay an exact string."""

    text = luigi.Parameter()

    def output(self):
        yield self.add_to_output("out.txt")

    def run(self):
        with open(self.get_output_file_name("out.txt"), "w") as f:
            f.write(self.text)
