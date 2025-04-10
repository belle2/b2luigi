.. _starterkit_label:

``b2luigi`` Starter Kit
========================

This tutorial was developed as a workshop for the October 2024 Belle II General Meeting.
The idea of this tutorial is that each of the following examples is present in your current working directory and is executed by hand.
To get the example files...

1. ... eiher copy the Python code directly from this page into a blank file or...
2. ... download the examples directly from their respective pages or...
3. ... clone the ``b2luigi`` repository and move to the ``examples`` directory, e.g.

.. code-block:: bash

    git clone https://gitlab.desy.de/belle2/software/b2luigi.git
    cd b2luigi/examples

Alternatively, you can also clone directly from `GitHub <https://github.com/belle2/b2luigi>`_.

In any of these cases, it is recommended to run the tutorial in a virtual environment.
With access to the Belle II software stack, you can use the following commands to set up a virtual environment and install the necessary packages:

.. code-block:: bash

    source /cvmfs/belle.cern.ch/tools/b2setup
    b2venv release-09-00-00

.. hint::

    Use a full ``basf2`` release if you want to run the ``basf2`` examples with the reconstruction.

The ``b2venv`` command creates a ``venv`` directory in the repository with the name "venv".
The next step is to activate the environment:

.. code-block:: bash

    source venv/bin/activate

This environment will be based on the ``basf2`` environment.
However, Python packages that are not provided by the externals will be installed in the virtual environment.
To install the requirements with in a ``b2venv`` for this tutorial run:

.. code-block:: bash

    pip install b2luigi

If you use a clear ``venv``, you can install the requirements with:

.. code-block:: bash

    pip install b2luigi pandas pyarrow uproot matplotlib plothist

If you copied the examples from the Git repository, you can install the requirements with:

.. code-block:: bash

    pip install -r requirements.txt
