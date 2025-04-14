.. _api-batch-core-label:

Worker Scheduler Factory
========================

The key principle of ``b2luigi`` is the steering of different batch processes.
For this, ``b2luigi`` defines a custom so-called "Worker Scheduler Factory".
With the factory, we define a custom worker which determines the appropriate batch system for a task and creates a task process accordingly.

.. autoclass:: b2luigi.batch.workers.BatchSystems
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: b2luigi.batch.workers.SendJobWorker
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: b2luigi.batch.workers.SendJobWorkerSchedulerFactory
   :members:
   :undoc-members:
   :show-inheritance:

Batch Process Base Class
========================

.. autoclass:: b2luigi.batch.processes.BatchProcess
   :members:
   :show-inheritance:
