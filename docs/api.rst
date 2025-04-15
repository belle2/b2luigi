.. _`api-documentation-label`:

API Documentation
=================

``b2luigi`` summarizes different topics to help you in your everyday task
creation and processing.
Most important is the :meth:`b2luigi.process` function, which lets you run
arbitrary task graphs on the batch.
It is very similar to :meth:`luigi.build`, but lets you hand in additional parameters
for steering the batch execution.

If you are not yet familiar with ``luigi`` itself, we recommend you to read its current documentation: `luigi Documentation <https://luigi.readthedocs.io/en/stable/index.html>`_
A few highlights are already included in the :ref:`luigi-features-label` section.


.. toctree::
    :maxdepth: 2

    api/top_level_functions
    api/utilities
    api/tasks
    api/target
    api/xrootdtargets
    api/parameters
    api/settings
    api/on_temporary
    api/runner
    api/apptainer
    api/batch_core
    api/batch_processes
    api/basf2_helper
