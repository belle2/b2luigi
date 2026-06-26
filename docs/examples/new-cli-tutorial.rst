.. _cli-tutorial-label:

Getting Started with the b2luigi CLI
=====================================

This tutorial walks you through the new ``b2luigi`` command-line interface
from first task to multi-parameter runs.  By the end you will have used
``b2luigi run``, ``b2luigi show``, ``b2luigi remove``, and
``ParameterGenerator``.

**Prerequisites:** ``b2luigi`` installed (``pip install b2luigi``).
Verify with:

.. code-block:: bash

    b2luigi --version

Step 1 — Create your first task
---------------------------------

Create a file called ``tasks.py`` in a new directory:

.. code-block:: python

    import b2luigi


    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.Parameter()

        def output(self):
            return self.add_to_output("output.txt")

        def run(self):
            with open(self.get_output_file_name("output.txt"), "w") as f:
                f.write(f"my_parameter = {self.my_parameter}\n")

Step 2 — Run it
----------------

.. code-block:: bash

    b2luigi run MyTask --my-parameter 3

b2luigi creates the output file under a ``results/`` directory, in a
subfolder named after the parameter value (``my_parameter=3/``).

Step 3 — Inspect outputs
-------------------------

.. code-block:: bash

    b2luigi show                      # full dependency tree
    b2luigi show -t MyTask            # just MyTask's outputs

The output panel uses colour coding: **green** means the file exists,
**red** means it is missing.

Step 4 — Remove and re-run
---------------------------

.. code-block:: bash

    b2luigi remove -t MyTask -y       # -y skips the confirmation prompt
    b2luigi run MyTask --my-parameter 3

Step 5 — Multiple parameter values with ``parameters.py``
-----------------------------------------------------------

Create a ``parameters.py`` alongside ``tasks.py``:

.. code-block:: python

    from b2luigi import ParameterGenerator

    config = {
        "my_parameter": ParameterGenerator([1, 2, 3]),
    }

Now run without any parameter flags:

.. code-block:: bash

    b2luigi run

b2luigi reads ``parameters.py`` automatically and schedules three task
instances — one per value in the generator.

.. code-block:: bash

    b2luigi show    # shows all three instances and their output status

Step 6 — Working with task requirements
-----------------------------------------

Add a parent task that depends on ``MyTask``:

.. code-block:: python

    class ParentTask(b2luigi.Task):
        my_parameter = b2luigi.Parameter()

        def requires(self):
            return MyTask(my_parameter=self.my_parameter)

        def output(self):
            return self.add_to_output("summary.txt")

        def run(self):
            with open(self.get_output_file_name("summary.txt"), "w") as f:
                f.write("done\n")

Use ``--with-requirements`` to traverse the full requirement chain:

.. code-block:: bash

    b2luigi show -t ParentTask --with-requirements
    b2luigi remove -t MyTask --with-requirements -y

``--with-requirements`` walks downward through ``requires()`` — it shows
or removes ``MyTask`` and everything it transitively depends on, but does
not touch ``ParentTask`` itself.

Step 7 — Shell auto-completion
--------------------------------

Install tab-completion for your shell:

.. code-block:: bash

    b2luigi --install-completion

After sourcing your shell rc file, ``b2luigi <Tab>`` completes subcommand
names and ``b2luigi show --<Tab>`` completes flag names.

What's next?
-------------

*   :ref:`cli-label` — full feature reference for all subcommands and options
*   :ref:`cli-api-label` — auto-generated command reference with all flags
*   :ref:`quick-start-label` — end-to-end quickstart including batch submission
