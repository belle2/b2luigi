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
    api/tasks
    api/parameters
    api/xrootdtargets
    api/settings
    api/other_functions
    api/b2luigi.basf2_helper
