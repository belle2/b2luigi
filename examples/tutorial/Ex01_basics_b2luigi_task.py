# ----------------------------------------------------------------------------
# Starter Kit: b2luigi (B2GM 2024)
# Authors: Alexander Heidelbach, Jonas Eppelt
#
# Scope: This example demonstrates how to create a simple b2luigi
# task that writes a parameter to a file. The key points to be
# learned from this example are the `run` and `output` methods.
#
# ----------------------------------------------------------------------------

import b2luigi


# (b2)luigi tasks are defined as classes that inherit from a luigi task class
class MyTask(b2luigi.Task):
    # Task parameters are defined as class attributes.
    # Although `luigi.Parameter` acts as general parameter type, it assumes
    # a string value. More parameter types are available in the luigi API:
    # https://luigi.readthedocs.io/en/stable/api/luigi.parameter.html
    # Commonly used types are:
    # - (b2)luigi.IntParameter
    # - (b2)luigi.FloatParameter
    # - (b2)luigi.BoolParameter
    # - (b2)luigi.DictParameter
    # Usefull parameter attributes are:
    # - default: default value of the parameter
    # - significant: specify False if the parameter should not be treated
    #                as part of the unique identifier for a Task
    # - description: description of the parameter
    # - hashed: hashes the parameter value to generate the unique identifier
    parameter = b2luigi.Parameter()

    # The `run` method defines the task's computation. It is called when the
    # task is executed by being called as part of `b2luigi.process`.
    # The method should contain the actual computation and write the output
    # to the specified location.
    def run(self):
        # The `get_output_file_name` method returns an output file defined
        # in the output function with the given key.
        with open(self.get_output_file_name("output.txt"), "w") as f:
            f.write(f"{self.parameter}")

    # The `output` method defines the output of the task. It should return an
    # iterable of output targets. The output targets are specified as either
    # strings or `luigi.LocalTarget` objects. The task is considered complete
    # when all output targets are present.
    def output(self):
        # The `add_to_output` method is a helper function that adds a target
        # to the list of files, this task will output. Always use in
        # combination with yield. This function will automatically add all
        # current parameter values to the output directory.
        yield self.add_to_output("output.txt")


if __name__ == "__main__":
    # Call `b2luigi.process` in your main method to tell b2luigi where your
    # entry point of the task graph is. Optional arguments are:
    # -show_output: Instead of running the task(s), write out all output
    #               files which will be generated marked in color, if they
    #               are present already. Good for testing of your tasks will
    #               do, what you think they should.
    # -dry_run: Instead od running the task(s), write out which tasks will
    #           be executed. This is a simplified form of dependency
    #           resolution, so this information may be wrong in some corner
    #           cases. Also good for testing.
    # -test: Does neither run on the batch system, with multiprocessing or
    #        dispatched but directly on the machine for debugging reasons.
    #        Does output all logs to the console.
    # -batch: More in 08_basf2_analysis_LSF.py
    b2luigi.process(MyTask(parameter=1))

    # The script can be executed with
    # `$ python 01_basics_b2luigi_task.py`
    # A directory with the name "parameter=1" will be created containing a
    # file "output.txt" with the content "1".
