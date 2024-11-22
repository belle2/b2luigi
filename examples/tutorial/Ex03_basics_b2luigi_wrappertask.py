# ----------------------------------------------------------------------------
# Starter Kit: b2luigi (B2GM 2024)
# Authors: Alexander Heidelbach, Jonas Eppelt
#
# Scope: This example demonstrates the usage of the `b2luigi.WrapperTask`
# class. This class is used to define a task that requires multiple other
# tasks to be executed.
#
# ----------------------------------------------------------------------------

import b2luigi


class MyTask(b2luigi.Task):
    parameter = b2luigi.IntParameter()

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


# The `b2luigi.WrapperTask` class is used to define a task that requires all
# parameter combinations of another task to be executed. The combinations are
# determined by the `requires` method of the `WrapperTask`. In contrast to
# the usual `b2luigi.Task` class it does not need a `run` and `output` method.
# The task counts as completed if its requirement is fulfilled.
class MyWrapperTask(b2luigi.WrapperTask):
    max_value = b2luigi.IntParameter()

    def requires(self):
        return [MyOtherTask(parameter=value) for value in range(self.max_value)]


if __name__ == "__main__":
    # Here, we make use of the settings mechanism of b2luigi to define the
    # directory where the results will be stored. This is done by assigning a
    # value to the `result_dir` key. Setting the `results_dir` here, globaly,
    # will now save all the outputs of the processed tasks in the specified
    # location.
    # It is also possible to define for each task specifically the settings.
    # The order in which the settings are respected is as follows:
    # - Task instance attribute
    # - Task class property
    # - Global setting (as demostrated here)
    # - Configuration file 'settings.json'
    b2luigi.set_setting("result_dir", "results")

    b2luigi.process(MyWrapperTask(max_value=10))
    # Check the output with:
    # `for file in results/*/output2.txt; do cat "$file"; echo ""; done`

    # In very easy cases, it is also possible to directly process a list of
    # tasks at this position. However, this is not recommended for more complex
    # task trees. The equivalent process call would be:
    # b2luigi.process([MyOtherTask(parameter=value) for value in range(10)])
