.. _luigi-features-label:

``luigi`` Features
==================

``luigi`` already has many nice and helpful features.
All of them can be used with ``b2luigi`` as well.
In the following is a collection of the most important features.
Please note, that the following list is not exhaustive; check the `luigi documentation <https://luigi.readthedocs.io/en/stable/index.html>`_ for more information.

The Central Scheduler
---------------------

When the number of tasks grows, it is sometimes hard to keep track of all of them (despite the summary in the end).
For this, ``luigi`` (the parent project of ``b2luigi``) brings a nice visualization and scheduling tool called `the central scheduler <https://luigi.readthedocs.io/en/stable/central_scheduler.html>`_.
If you are submitting your jobs from a remote machine, you can forward the schedulers port to your local machine via ssh.

Parameters
----------
``luigi`` already has various Parameter types to pass to your tasks, like :obj:`ListParameter`, :obj:`IntParameter`, :obj:`DateParameter` and many more.
Check the `luigi parameter documentation <https://luigi.readthedocs.io/en/stable/parameters.html>`_ for more information.

Logging
-------
``luigi`` has some `logging features <https://luigi.readthedocs.io/en/stable/logging.html>`_ built in.

Even more settings
------------------
Many aspects of ``luigi``'s scheduling mechanism can be controlled via the `luigi configuration <https://luigi.readthedocs.io/en/stable/configuration.html>`_.
Please be aware that these settings are a separate configuration from the settings in ``b2luigi``.
Yes, this is very confusing, but historically grown. At some point in the future, this might change.

Resource Management
-------------------
Using the central scheduler, ``luigi`` can do some simple `resource management <https://luigi.readthedocs.io/en/stable/configuration.html#resources>`_.
Just set the available resources in the `luigi.cfg` and set the amount used as a task property.
