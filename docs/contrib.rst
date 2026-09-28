.. _contrib-label:

Contrib Packages
================

We try to keep the core of ``b2luigi`` as lightweight and general-purpose as possible.
Specialized functionalities that are dependent on other software libraries or have a more specific use case are implemented in the contrib packages.

Currently, ``b2luigi.contrib`` holds the integrations with the Belle II software:

* :ref:`contrib-basf2-label` (``b2luigi.contrib.basf2``): tasks for running ``basf2`` paths
  and merging their outputs.
* :ref:`contrib-gbasf2-label` (``b2luigi.contrib.gbasf2``): a batch process that submits
  ``basf2`` paths to the LHC Computing Grid with ``gbasf2``.


.. note::
    The ``basf2`` and ``gbasf2`` integrations used to live in ``b2luigi.basf2_helper`` and
    ``b2luigi.batch.processes.gbasf2``.
    Importing from ``b2luigi.basf2_helper`` still works but emits a warning, so please update
    your imports to ``b2luigi.contrib.basf2``.
    We will remove the old imports in a future release of ``b2luigi``.

However, contributions to the contrib packages are welcome and highly encouraged.

While the contrib packages are always installed with ``b2luigi``, you can ensure the correct additional dependencies are installed by using optional extras when installing ``b2luigi``:

.. code-block:: bash

    pip3 install "b2luigi[basf2]"    # basf2 integration
    pip3 install "b2luigi[gbasf2]"   # gbasf2 grid submission
    pip3 install "b2luigi[contrib]"  # all contrib packages

.. toctree::
    :maxdepth: 2

    contrib/basf2
    contrib/basf2-examples
    contrib/gbasf2
