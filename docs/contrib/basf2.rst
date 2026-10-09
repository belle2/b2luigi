.. _contrib-basf2-label:

``basf2``
=========

The ``b2luigi.contrib.basf2`` package integrates ``b2luigi`` with ``basf2``, the Belle II
analysis software framework.
Its central piece is :class:`Basf2PathTask <b2luigi.contrib.basf2.tasks.Basf2PathTask>`:
instead of implementing ``run``, you implement
:meth:`create_path <b2luigi.contrib.basf2.tasks.Basf2PathTask.create_path>` and return a
``basf2.Path``, which ``b2luigi`` then processes for you.
The package also provides tasks to merge the outputs of many ``basf2`` jobs, a ROOT file target
and a helper to record the ``basf2`` release a task was run with.

``basf2`` itself is not installed with ``b2luigi``. Set up a Belle II software release
(see :ref:`installation-label`) and import the tasks from ``b2luigi.contrib.basf2``:

.. code-block:: python

    from b2luigi.contrib.basf2 import Basf2PathTask, Basf2nTupleMergeTask

For complete workflows, have a look at the :ref:`basf2-example-label` and the ``basf2`` part of the
:ref:`starterkit_label`, starting with :ref:`sphx_glr_starterkit_Ex05_basf2_simulation.py`.

API Reference
-------------

.. automodule:: b2luigi.contrib.basf2.targets
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: b2luigi.contrib.basf2.tasks
    :members:
    :undoc-members:
    :private-members: _process
    :show-inheritance:

.. automodule:: b2luigi.contrib.basf2.utils
    :members:
    :undoc-members:
    :show-inheritance:
