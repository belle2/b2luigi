.. _run-modes-label:

Run Modes
=========

The run mode can be chosen by calling your python file with

.. code-block:: bash

    python file.py --mode

or by calling ``b2luigi.process`` with a given mode set to ``True``

.. code-block:: python

    b2luigi.process(.., mode=True)

where mode can be one of:

*   **batch**: Run the tasks on a batch system, as described in :ref:`quick-start-label`. The maximal number of
    batch jobs to run in parallel (jobs in flight) is equal to the number of workers.
    This is 1 by default, so you probably want to change this.
    By default, LSF is used as a batch system. If you want to change this, set the corresponding ``batch_system``
    (see :ref:`batch-label`) to one of the supported systems.

*   **dry-run**: Similar to the dry-run funtionality of ``luigi``, this will not start any tasks but just tell
    you, which tasks it would run, and call the ``dry_run`` method of the task if implemented:

    .. code-block:: python

        class SomeTask(b2luigi.Task):
            [...]
            def dry_run(self):
                # if a method with this name is provided, it will be executed
                # automatically when starting the processing in dry-run mode
                do_some_stuff_in_dry_run_mode()

    This feature can be easily used for e.g. file name debugging, i.e. to print out the file names ``b2luigi``
    will create when running the actual task. The exit code of the ``dry-run`` mode is 1 in case a task needs
    to run and 0 otherwise.

*   **show-output**: List all output files that this has produced/will produce. Files which already exist
    (where the targets define, what exists mean in this case) are marked as green whereas missing targets are
    marked red.

*   **test**: Run the tasks normally (no batch submission), but turn on debug logging of ``luigi``. Also,
    do not dispatch any task (if requested) and print the output to the console instead of in log files.
