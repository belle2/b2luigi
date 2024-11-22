.. _`api-documentation-label`:

API Documentation
=================

``b2luigi`` summarizes different topics to help you in your everyday task
creation and processing.
Most important is the :meth:`b2luigi.process` function, which lets you run
arbitrary task graphs on the batch.
It is very similar to ``luigi.build``, but lets you hand in additional parameters
for steering the batch execution.

If you are not yet familiar with ``luigi`` itself, we recommend you to read its current documentation: `Luigi Documentation <https://luigi.readthedocs.io/en/stable/index.html>`_


Important features from luigi:
-----------------------------------

``luigi`` itself already boasts a number of nice features. Check out some highlights:
- `The central scheduler  <https://luigi.readthedocs.io/en/stable/central_scheduler.html>`_
- `The notification system <https://luigi.readthedocs.io/en/stable/api/luigi.notifications.html#module-luigi.notifications>`_
- `communication with tasks <https://luigi.readthedocs.io/en/stable/luigi_patterns.html#sending-messages-to-tasks>`_


.. toctree::
    :maxdepth: 2

    top_level_functions
    tasks
    parameters
    xrootdtargets
    settings
    other_functions
    b2luigi.basf2_helper
