.. _parameter-grouping-label:

Parameter Grouping
==================

.. warning::
   This is an experimental feature and may change in the future. Please report any issues you encounter when using it.

.. note::
   Parameter grouping is available for the HTCondor, Slurm and LSF batch systems.
   Other batch systems (e.g. gbasf2) refuse a grouped task with ``max_grouping_size > 1``.
   Help extending it is very welcome, so if you want to contribute, please check out the :ref:`development-label`.

Overview
--------

When running large workflows on a batch system, submitting and monitoring a very high number of individual jobs can put unnecessary load on both the local scheduler and the batch system itself.
Parameter grouping addresses this by allowing multiple logical b2luigi tasks to be submitted together as a single task.

With grouping enabled:

- A single worker can submit and manage multiple tasks at once.
- The number of workers required is reduced.
- Job submission and status queries are faster.
- The overall load on the batch system is significantly reduced.
- However, this feature does not change the total number of tasks that (b2)luigi has to iterate, e.g. when scheduling tasks in a workflow.

Enabling Parameter Grouping
---------------------------

Grouping is enabled on a per-parameter basis by setting the ``grouping`` flag on a ``b2luigi.Parameter``:

.. code-block:: python

    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.Parameter(grouping=True)

Multiple parameters may be marked as grouped.
To control the size of these groups, one needs to set the ``max_grouping_size`` attribute within the task (defaults to 1)


.. code-block:: python

    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.Parameter(grouping=True)

        max_grouping_size = 10

In the example above, when running 100 tasks of the type ``MyTask``, b2luigi would group them in 10 groups of 10 tasks.
Consequently, only 10 workers are consumed instead of 100.

A complete example can be found in the code examples in the ``examples/htcondor/grouping_example.py`` file.
The same task definition works unchanged on Slurm and LSF; only the ``batch_system`` setting differs.

How a group is submitted
------------------------

Every supported batch system expands a group at submission time into one job per parameter value:

- **HTCondor** writes one ``queue 1`` block per value into a single submit file, so one ``condor_submit`` call creates all jobs.
- **Slurm** creates one submit script per value and calls ``sbatch`` once per value.
- **LSF** calls ``bsub`` once per value.

In all three cases each job runs exactly one scalar task with its own log directory, and the group is reported to luigi as one task
that finishes when the last of its jobs has finished.

Failure Semantics and Resubmission
----------------------------------

.. warning::
   A grouped chunk is considered successful only if all tasks within that chunk succeed.

If one or more tasks within a group fail:

- Only the failed tasks are resubmitted to the batch system.
- Completed tasks in the same group are not rerun.

Choosing an appropriate ``max_grouping_size`` therefore involves a trade-off:

**Larger values**

- Faster submission and status querying
- Fewer workers consumed
- Slower turnaround time for individual tasks

**Smaller values**

- Faster feedback for individual tasks
- Higher scheduler and batch-system load

For large workflows, larger grouping sizes are usually preferable.

Advanced Usage: Custom Grouping Logic
-------------------------------------

Although not recommended, it is possible to provide a custom ``grouping_function`` to control how parameter values are divided into chunks.

.. warning::
   This is intended for expert use only.

Internally, grouped parameter values are packed and unpacked in a specific way.
Changing the grouping logic can therefore lead to subtle or unexpected behaviour if not done with care.


Interaction with luigi Batching
-------------------------------

.. warning::
   Internally, parameter grouping is implemented using luigi’s batching mechanism by setting ``max_batch_size`` on the task and providing a ``batch_method`` for the parameters.

As a result:

- Enabling grouping in b2luigi overwrites any user-defined ``max_batch_size`` or ``batch_method``.
- If grouping is **not** enabled, the full luigi batching functionality remains available and untouched.

Keep this in mind if you rely on custom batching behaviour in your workflows.
