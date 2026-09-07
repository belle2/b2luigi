TASKS_TEMPLATE = """\
import b2luigi


class MyTask(b2luigi.Task):
    \"\"\"
    This is a sample task that demonstrates how to define a b2luigi task. It takes a single parameter and writes it to an output file.
    \"\"\"

    parameter = b2luigi.Parameter()

    def run(self):
        with open(self.get_output_file_name("output.txt"), "w") as f:
            f.write(f"{self.parameter}")

    def output(self):
        yield self.add_to_output("output.txt")

"""

PARAMS_TEMPLATE = """\
# Must contain a dict named 'config' with parameter values for the task(s) to run.
# The keys should match the parameter names defined in the task class.
config = {"parameter": "1"}
"""
