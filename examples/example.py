# ----------------------------------------------------------------------------
# Starter Kit: b2luigi (B2GM 2024)
# Authors: Alexander Heidelbach, Jonas Eppelt
#
# Scope: This example demonstrates how a single task can require multiple
# tasks to complete. This is useful if the output of multiple tasks is needed
# to compute the output of the following task. Additionally, we introduce the
# core concepts of schedulers.
#
# ----------------------------------------------------------------------------

import b2luigi


class MyTask(b2luigi.Task):
    parameter = b2luigi.IntParameter()
    apptainer_image = "/cvmfs/belle.cern.ch/images/belle2-base-el9"
    apptainer_mounts = ["/cvmfs", "/work"]

    @property
    def env_script(self):
        return "setup.sh"

    def run(self):
        with open(self.get_output_file_name("output.txt"), "w") as f:
            f.write(f"{self.parameter}")

    def output(self):
        yield self.add_to_output("output.txt")


class MyOtherTask(b2luigi.Task):
    parameter = b2luigi.IntParameter()

    def requires(self):
        return MyTask(parameter=self.parameter)

    def output(self):
        yield self.add_to_output("output2.txt")

    def run(self):
        with open(self.get_input_file_names("output.txt")[0], "r") as f:
            number = int(f.read())

        with open(self.get_output_file_name("output2.txt"), "w") as f:
            f.write(f"{number**2}")


class MyAverageTask(b2luigi.Task):
    max_value = b2luigi.IntParameter()

    # The `require` method and the task parameter look identical to the previous
    # example in `03_basics_b2luigi_wrappertask.py`.
    def requires(self):
        return [MyOtherTask(parameter=value) for value in range(self.max_value)]

    def output(self):
        # Define multiple outputs at the same time.
        yield self.add_to_output("average.txt")
        yield self.add_to_output("sum.txt")

    def run(self):
        numbers = []
        for filename in self.get_input_file_names("output2.txt"):
            with open(filename, "r") as f:
                numbers.append(int(f.read()))

        with open(self.get_output_file_name("average.txt"), "w") as f:
            f.write(f"{sum(numbers)/len(numbers)}")

        with open(self.get_output_file_name("sum.txt"), "w") as f:
            f.write(f"{sum(numbers)}")


if __name__ == "__main__":
    b2luigi.set_setting("result_dir", "results")
    b2luigi.process(MyTask(parameter=10))
