"""
.. _exercise02_label:

Building a Dependency Graph with ``b2luigi``
============================================

.. hint::
    This example demonstrates the third key method of a ``b2luigi`` task: the ``requires`` method.
    This method is used to define dependencies between different tasks.

The ``requires`` method is used to define dependencies between different tasks.
The method should return iterable instances of the required tasks.
This task will be scheduled to run after the output of the required task is completed.
The output is considered completed if the output files are present.

.. warning::
    There is no check of the content of the output files!
    Consequently, the previous task can be forced to rerun by deleting the output.

To show the functionality of the ``requires`` method, we will create a second task that depends on the first task.
For this, we define the first task in the same way as in the previous example.
"""
import b2luigi


class MyTask(b2luigi.Task):
    parameter = b2luigi.IntParameter()

    def run(self):
        with open(self.get_output_file_name("output.txt"), "w") as f:
            f.write(f"{self.parameter}")

    def output(self):
        yield self.add_to_output("output.txt")


# %%
# We again make use of the :meth:`b2luigi.Task.add_to_output` method to add
# another output file in the now existing directory structure. In this
# example, the output is structured as follows:
#
# .. code-block:: none
#
#   parameter=1/
#     output.txt
#     output2.txt
#
# We use :meth:`b2luigi.Task.get_input_file_names` to get the file name of
# the output file of the required task. The method returns an iterable
# of file names. In this case, we know that there is only one file
# present and we can access it by accessing the first element.


# %%
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
# For the process method, we now have to call the new task. The scheduler
# will automatically resolve the dependencies between the tasks. If the
# output of ``MyTask`` is not present, it will be executed first. As soon as,
# the output is present, ``MyOtherTask`` will be executed.
if __name__ == "__main__":
    b2luigi.process(MyOtherTask(parameter=1))

# %%
# You can try to adjust the value in `parameter=1/output.txt`, delete the
# output of ``MyOtherTask`` and rerun the script to see how this affects the
# result of the tasks.
