"""
.. _exercise04_label:

Multiple Tasks and Introduction to Schedulers
=============================================

.. hint::
    This example demonstrates how a single task can require multiple tasks to complete.
    This is useful if the output of multiple tasks is needed to compute the output of the following task.
    Additionally, we introduce the core concepts of schedulers.

:class:`b2luigi.WrapperTask` is not the only way to require multiple tasks.
Also normal tasks can require multiple tasks.
This is useful if the wrapper task uses the inputs of the required tasks to compute its output.

For this, we define the first and second task in the same way as in the previous example.
"""
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


# %%
# In this example we will build a task whose ``require`` method and the task parameter look identical to the previous example in :ref:`exercise03_label`.
#
# Now, we make use of the fact that the iterable of :meth:`b2luigi.Task.get_input_file_names`
# returns all the input targets that match the provided key.
# In this case, the key is the file name defined with :meth:`b2luigi.Task.add_to_output` in ``MyOtherTask``.


# %%
class MyAverageTask(b2luigi.Task):
    max_value = b2luigi.IntParameter()

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


# %%
# The output of ``MyAverageTask`` will now be in the `result_dir`, however,
# since we used the :meth:`b2luigi.Task.add_to_output` it will create another subdirectory
# corresponding to its parameter value, i.e. `max_value=10`.

# %%
if __name__ == "__main__":
    b2luigi.set_setting("result_dir", "results")
    b2luigi.process(MyAverageTask(max_value=10))

# %%
# In real applications, tasks depending on multiple other tasks can
# form very large dependency graphs. Additionally, the execution time of the
# entire workflow can be very long. To handle the schedule and execution of
# such workflows, ``luigi`` provides the user with the concept of local and
# global schedulers. The ``local_scheduler`` is the default scheduler and is
# run in the background without the user noticing it.
#
# We can also define which scheduler should be used to schedule the task
# here. For this, we spawn the scheduler in a separate process with the
# help of the ``luigi`` daemon:
#
# .. code-block:: bash
#
#   host@user:$ luigid --port 8080
#
# The ``port`` number should be an accessible port of the ``host`` machine. We
# can then use the ``--scheduler-host`` and ``--scheduler-port`` arguments to
# tell the task where to find the scheduler.
#
# .. code-block:: bash
#
#   otherhost@user:$ python3 Ex04_basics_b2luigi_averagetask.py --scheduler-host host.url --scheduler-port 8080
#
# The luigi scheduler now can also be see when spectating the port
# on the ``host`` machine in the browser with ``localhost:8080``.
# The scheduler settings can also be set in the `settings.json` or
# in the `luigi.cfg` file.
