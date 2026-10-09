.. _contrib-gbasf2-label:

``gbasf2``
==========

The ``b2luigi.contrib.gbasf2`` package provides a batch process for ``gbasf2``, the Belle II
submission tool for the LHC Computing Grid.
It takes the ``basf2`` path of a :class:`Basf2PathTask <b2luigi.contrib.basf2.tasks.Basf2PathTask>`,
submits it as a ``gbasf2`` project, monitors the project, reschedules failed jobs and downloads
the output dataset once all jobs have succeeded.

To use it, install the ``gbasf2`` extra and set ``batch_system = "gbasf2"`` on the tasks that should
run on the grid:

.. code-block:: bash

    pip3 install "b2luigi[gbasf2]"

``gbasf2`` is never selected by ``batch_system = "auto"``; it always has to be set explicitly.
Parameter grouping (see :ref:`parameter-grouping-label`) is not supported for ``gbasf2`` tasks.
All ``gbasf2``-specific settings are described in :class:`Gbasf2Process <b2luigi.contrib.gbasf2.process.Gbasf2Process>`
below, and a complete example is shown in :ref:`sphx_glr_starterkit_Ex11_basf2_analysis_gbasf2.py`.

API Reference
-------------

.. automodule:: b2luigi.contrib.gbasf2.process
   :members:
   :private-members:
   :show-inheritance:
