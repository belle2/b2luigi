.. _faq-label:

FAQ
===

Can I specify my own paths for the log files for tasks running on a batch system?
---------------------------------------------------------------------------------

``b2luigi`` will automatically create log files for the ``stdout`` and ``stderr``
output of a task processed on a batch system. The paths of these log files are defined
relative to the location of the executed python file and contain the parameter of
the task.
In some cases, one might one to specify other paths for the log files. To achieve this,
the ``get_log_file_dir()`` method of the task class must be defined. This method
must return a directory path for the stdout and the stderr files, for example:

.. code-block:: python

  class MyBatchTask(b2luigi.Task):
      ...
      def get_log_file_dir(self):
          filename = os.path.realpath(sys.argv[0])
          path = os.path.join(os.path.dirname(filename), "logs")
          return path

``b2luigi`` will use this method if it is defined and write the log output in the respective
files. Be careful though, as these log files will of course be overwritten if more than one
task receive the same paths to write to!


Can I exclude one job from batch processing?
--------------------------------------------

The setting ``batch_system`` defines which submission method is used for scheduling
your tasks when using ``batch=True`` or ``--batch``.
In most cases, you set your ``batch_system`` globally (e.g. in a ``settings.json`` file)
and start all your tasks with ``--batch`` or ``batch=True``.
If you want a single task to run only locally (e.g. because of constraints in
the batch farm), you can set the ``batch_system`` only for this job by adding a member to this task:

.. code-block:: python

    class MyLocalTask(b2luigi.Task):
        batch_system = "local"

        def run(self):
            ...

How do I handle parameter values which include "/" (or other unusual characters)?
---------------------------------------------------------------------------------

``b2luigi`` automatically generates the filenames for your output or log files out of
the current tasks values in the form::

    <result-path>/param1=value1/param2=value2/.../<output-file-name.ext>

The values are given by the serialisation of your parameter, which is basically its string representation.
Sometimes, this representation may include characters not suitable for their usage as a path name,
e.g. "/".
Especially when you use a :obj:`DictParameter` or a :obj:`ListParameter`, you might not
want to have its value in your output.
Also, if you have credentials in the parameter (what you should never do of course!), you do not
want to show them to everyone.

When using a parameter in ``b2luigi`` (or any of its derivatives), they have a new flag called ``hashed``
in their constructor, which makes the path creation only using a hashed version of your parameter value.

For example, this task will::

    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.ListParameter(hashed=True)

        def run(self):
            with open(self.get_output_file_name("test.txt"), "w") as f:
                f.write("test")

        def output(self):
            yield self.add_to_output("test.txt")


    if __name__ == "__main__":
        b2luigi.process(MyTask(my_parameter=["Some", "strange", "items", "with", "bad / signs"]))

create a file called ``my_parameter=hashed_08928069d368e4a0f8ac02a0193e443b/test.txt`` in your output folder
instead of using the list value.


What does the ValueError `"The task id {task.task_id} to be executed..."`` mean?
--------------------------------------------------------------------------------

The :obj:`ValueError` exception `"The task id <task_id> to be executed by this batch worker does
not exist in the locally reproduced task graph.""` is thrown by ``b2luigi`` batch workers if
the task that should have been executed by this batch worker does not exist in the task
graph reproduced by the batch worker. This means that the task graph produced by the initial
:meth:`b2luigi.process` call and the one reproduced in the batch job differ from each other.
This can be caused by a non-deterministic behavior of your dependency graph generation, such
as a random task parameter or parameters which are paths and differ for local vs. batch execution.


I do not like to have "=" in my output file names. Is there a way to not have them in the generated output paths?
-----------------------------------------------------------------------------------------------------------------

Yes, there are two options. The first is to set the setting ``use_parameter_name_in_output`` to ``False``.
The paths for your outputs and logs will then be generated using only the parameter values.
It is then up to you to remember which parameter value belongs to which parameter name.
Alernatively, you can use the setting ``parameter_separator`` to change "=" to a string of your choice.


Can I alter the ``exec`` string in the executable wrapper made by b2luigi for batch submissions?
------------------------------------------------------------------------------------------------

Yes, you can adjust the ``exec`` string used in the executable wrapper for batch submissions. The exec string is made up
of three key components::

    <executable_prefix> <executable> <filename> --batch-runner --task-id ExampleTask_id_123 <task_cmd_additional_args>

Where by default:

- ``executable`` = ``[python3]``
- ``filename`` = ``[path/to/main/python/script.py]``
- ``task_cmd_additional_args`` = ``[]`` i.e nothing

The ``executable`` variable can be set to a custom value using the ``b2luigi`` settings manager, like so:

.. code-block:: python

    b2luigi.set_setting("executable", ["my_custom", "executable"])

The `filename` can not be customised. However, if necessary, it can be excluded from the exec string through the boolean setting ``add_filename_to_cmd``.
By default ``add_filename_to_cmd`` is ``True``; by setting it to ``False``, the filename is excluded from the exec string:

.. code-block:: python

    b2luigi.set_setting("add_filename_to_cmd", False)

Lastly, ``task_cmd_additional_args`` is a way to parse your own custom arguments to your python script or CLI that is being called on the batch system.
To do this correctly, you must set ``ignore_additional_command_line_args=False`` in your :meth:`b2luigi.process` call, for example:

.. code-block:: python

    b2luigi.process(
        MyTask(),
        # other required arguments
        ignore_additional_command_line_args = False
    )

Why we do this is to let ``b2luigi`` know that we are using our own argparser and to not throw an error when it encounters unknown arguments meant for our argparser.
With this in place, we can freely add our own additional arguments to the exec command, like so:

.. code-block:: python

    b2luigi.set_setting("task_cmd_additional_args", ["--name", "foo", "--import-variable", "bar"])

And with that, the exec function created by b2luigi for batch submission can be customised to suit your needs.

Can I check how the `gbasf2` command will be build before the submission?
-------------------------------------------------------------------------

Yes! You can use the :meth:`build_gbasf2_submit_command <b2luigi.batch.processes.gbasf2.build_gbasf2_submit_command>` method to check for the submission command.
This method rebuilds as closley as possible the submission command. An example usage is:

.. code-block:: python

    import shlex
    import b2luigi

    class MyTask(b2luigi.Task):
        parameter = b2luigi.IntParameter()

        @property
        def gbasf2_project_name_prefix(self):
            return "my_project"

        @property
        def gbasf2_input_dataset(self):
            return "my_input_dataset"

        def dry_run(self):
            gbasf2_command = b2luigi.batch.processes.gbasf2.build_gbasf2_submit_command(self)
            print(shlex.join(gbasf2_command)) # shlex.join is used to create a shell-friendly command string


Who made the beautiful logo?
----------------------------
The logo was created by Lea Reuter! We are forever grateful to her!


When submitting to the grid via gbasf2 can I adjust the length of the unique hash ID?
-------------------------------------------------------------------------------------
Yes you can, if you find yourself in a position where your ``gbasf2_project_prefix`` needs to be detailed
but the 22 character limit is too restrictive you can shorten the unique hash! For context, the project name
b2luigi submits to the grid is built similar to the example below.

.. code-block:: python

    project_name = f"{gbasf2_project_name_prefix}{unique_10_digit_hash}"
    assert len(project_name)<=32

Where the total project name cannot exceed 32 characters, as per gbasf2 guidelines. To adjust the hash length, you can give an integer
value between 5-10 to ``gbasf2_project_name_hash_length`` via the settings manager like so:

.. code-block:: python

    b2luigi.set_setting("gbasf2_project_name_hash_length", 7)

This example would allow three additional characters in ``gbasf2_project_prefix``, up to a maximum of five if ``gbasf2_project_name_hash_length=5``.
