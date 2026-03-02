.. _parameter-grouping-label:

Parameter Grouping
==================

.. warning::
   This is an experimental feature and may change in the future. Please report any issues you encounter when using it.

.. warning::
   This feature currently only works with the HTCondor batch system, but we plan to extend it to other batch systems in the future.
   Help is very welcome here, so if you want to contribute, please check out the :ref:`development-label`.

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

A complete example can be found in the code examples in the `examples/htcondor/grouping_example.py` file.

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
