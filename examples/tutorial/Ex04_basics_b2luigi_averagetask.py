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


# `b2luigi.WrapperTask` is not the only way to require multiple tasks. Also
# normal tasks can require multiple tasks. This is useful if the wrapper
# task uses the inputs of the required tasks to compute its output.
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

        # Now, the reuturned iterable of `b2luigi.Task.get_input_file_names`
        # returns all the input targets that match the provided key. In this
        # case, the key is the file name defined with `add_to_output` in
        # `MyOtherTask`.
        for filename in self.get_input_file_names("output2.txt"):
            with open(filename, "r") as f:
                numbers.append(int(f.read()))

        with open(self.get_output_file_name("average.txt"), "w") as f:
            f.write(f"{sum(numbers)/len(numbers)}")

        with open(self.get_output_file_name("sum.txt"), "w") as f:
            f.write(f"{sum(numbers)}")


if __name__ == "__main__":
    b2luigi.set_setting("result_dir", "results")

    # The output of `MyAverageTask` will now be in the `result_dir`, however,
    # since we used the `add_to_output` it will create another subdirectory
    # corresponding to its parameter value, i.e. `max_value=10`.
    b2luigi.process(MyAverageTask(max_value=10))

    # In real applications, tasks depending on multiple other tasks can
    # form very large dependecy graphs. Additionally, the execution time of the
    # entire workflow can be very long. To handle the schedule and execution of
    # such workflows, luigi provides the user with the concept of local and
    # global schedulers. The local_scheduler is the default scheduler and is
    # run in the background without the user noticing it.
    # We can also define which scheduler should be used to schedule the task
    # here. For this, we spawn the scheduler in a separate process with the
    # help of the luigi deamon:
    # ```
    # host@user: $ luigid --port 8080
    # ```
    # The port number should be an accesible port of the "host" machine. We
    # can then use the `--scheduler-host` and `--scheduler-port` arguments to
    # tell the task where to find the scheduler.
    # ```
    # otherhost@user: $ python 04_basics_b2luigi_averagetask.py --scheduler-host host.url --scheduler-port 8080
    # ```
    # The luigi scheduler now can also be spectated when spectating the port
    # on the "host" machine in the browser with `localhost:8080`.
    # The scheduler settings can also be set in the `settings.json` or
    # in the `luigi.cfg` file.
