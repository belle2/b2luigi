"""
.. _exercise01_label:

A Simple ``b2luigi`` Task
=========================

.. hint::
    This example demonstrates how to create a simple b2luigi
    task that writes a parameter to a file. The key points to be
    learned from this example are the ``run`` and ``output`` methods.

``(b2)luigi`` tasks are defined as classes that inherit from a luigi task class, for this example we will start
with a basic class that inherits from :class:`b2luigi.Task`. The task class should define the parameters, the run method,
and the output method. The parameters are defined as class attributes, and the run method contains the actual
computation. The output method defines the output of the task, which is used to determine if the task has been
completed.

Task parameters are defined as class attributes.
Although :class:`luigi.Parameter` acts as general parameter type, it assumes
a string value. More parameter types are available in the luigi API: `Parameter <https://luigi.readthedocs.io/en/stable/api/luigi.parameter.html>`_.

Commonly used types are:

- ``b2luigi.IntParameter``
- ``b2luigi.FloatParameter``
- ``b2luigi.BoolParameter``
- ``b2luigi.DictParameter``

Useful parameter attributes are:

- ``default``: default value of the parameter
- ``significant``: specify ``False`` if the parameter should not be treated as part of the unique identifier for a Task
- ``description``: description of the parameter
- ``hashed``: hashes the parameter value to generate the unique identifier

The ``run`` method defines the task's computation. It is called when the
task is executed by being called as part of :meth:`b2luigi.process`.
The method should contain the actual computation and write the output
to the specified location.

The ``output`` method defines the output of the task. It should return an
iterable of output targets. The output targets are specified as either
strings (will be interpreted as :class:`b2luigi.LocalTarget) or target objects (e.g. :class:`b2luigi.LocalTarget`, :class:`b2luigi.XRootDTarget`). The task is considered complete
when all output targets are present.

A few additional methods are available to help with the output structure:

- The :meth:`b2luigi.Task.add_to_output` method is a helper function that adds a target to the list of files, this task will output.
  Always use in combination with yield. This function will automatically add all current parameter values to the output
  directory.
- The :meth:`b2luigi.Task.get_output_file_name` method returns an output file defined in the output function with the given key.

"""

import b2luigi


class MyTask(b2luigi.Task):
    parameter = b2luigi.Parameter()

    def run(self):
        with open(self.get_output_file_name("output.txt"), "w") as f:
            f.write(f"{self.parameter}")

    def output(self):
        yield self.add_to_output("output.txt")


# %%
# Call :meth:`b2luigi.process` in your main method to tell ``b2luigi`` where your
# entry point of the task graph is. Optional arguments are:
#
# - ``show_output``:
#   Instead of running the task(s), write out all output files which will be
#   generated marked in color, if they
#   are present already. Good for testing of your tasks will
#   do, what you think they should.
# - ``dry_run``:
#   Instead of running the task(s), write out which tasks will
#   be executed. This is a simplified form of dependency
#   resolution, so this information may be wrong in some corner
#   cases. Also good for testing.
# - ``test``:
#   Does neither run on the batch system, with multiprocessing or
#   dispatched but directly on the machine for debugging reasons.
#   Does output all logs to the console.
# - ``batch``: More in :ref:`exercise08_label`.

# %%
if __name__ == "__main__":
    b2luigi.process(MyTask(parameter=1))

# %%
# The script can be executed with
#
# .. code-block:: bash
#
#   python3 Ex01_basics_b2luigi_task.py
#
# The output of the script will be the directory structure
#
# .. code-block:: none
#
#   parameter=1/
#     output.txt
#
# The output file will contain the value of the parameter, in this case
# ``1``.
