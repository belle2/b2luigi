.. _parameter-grouping-label:

Parameter Grouping
==================

.. warning::
   This is an experimental feature and may change in the future. Please report any issues you encounter when using it.

.. note::
   Parameter grouping is available for the HTCondor, Slurm and LSF batch systems.
   On the ``local`` batch system grouping is skipped automatically (see :ref:`grouping-locally-label`),
   other batch systems (e.g. gbasf2) refuse a group.
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

.. _grouping-locally-label:

Running Grouped Tasks Locally
-----------------------------

Whether a group can be formed depends on whether anyone is able to take it apart again.
Only the HTCondor, Slurm and LSF processes do so, by submitting one job per task in the group.

On the ``local`` batch system there is no such step, so grouping is **skipped automatically**:
the group is never formed and the tasks simply run one after another, exactly as if no parameter
had ``grouping=True``. A warning is emitted once per task class, and nothing else changes — in particular
the output paths are the normal per-value ones, not the hashed group path.

This is not only a convenience for debugging, it is what makes grouping usable together with a
batch system chosen at runtime. Grouping is declared on the task *class*: ``grouping=True`` is part of
the parameter definition and ``max_grouping_size`` is a class attribute, neither of which can be changed
for a single task instance. The ``batch_system``, on the other hand, can be a property and therefore
differ from instance to instance, e.g. depending on a configuration or the stage of a workflow.
Without this behaviour, a task class with grouped parameters could never run on the ``local`` batch
system at all. With it, the class is defined once and each instance is grouped wherever its batch
system supports it:

.. code-block:: python

    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.Parameter(grouping=True)

        max_grouping_size = 10

        # Grouped on Slurm, silently ungrouped when this is "local".
        @property
        def batch_system(self):
            return "local" if debugging else "slurm"

The decision is made per task, not per task class, so a workflow may mix grouped Slurm tasks
and ungrouped local tasks of the same class in a single run.

The same applies in test mode (``b2luigi.process(..., test=True)``), which runs every task
in process.

On the remaining batch systems (``gbasf2``, ``test`` and ``custom``) grouping is still
an error rather than being skipped, so that a custom process class implementing grouping itself
is not silently bypassed.

Failure Semantics and Resubmission
----------------------------------

.. warning::
   A grouped chunk is considered successful only if all tasks within that chunk succeed.

If one or more tasks within a group fail:

- Only the failed tasks are resubmitted to the batch system.
- Completed tasks in the same group are not rerun.

The failure message printed by b2luigi names the log directory of the *group*, whose path is built from the tuple of
grouped values. The jobs themselves log into the directory of their own scalar value, so the group directory only holds a
``failed_jobs.log`` listing every failed batch job id together with the log directory of that job. Start there to find
the ``stdout``/``stderr`` of the member that failed.

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
