.. _installation-label:

Installation
============

We recommend to always use a virtual environment for your Python projects.

1. Non-Belle II user can create a virtual environment with the following command:

    .. code-block:: bash

        python3 -m venv venv

    Belle II users see below for creation of a ``venv`` with the Belle II software.

2.  Setup your local environment.
    For example, run:

    .. code-block:: bash

        source venv/bin/activate

3.  Install ``b2luigi`` from `PyPI <https://pypi.org/project/b2luigi/>`_ into your environment.

    .. code-block:: bash

        pip3 install b2luigi


Now you can go on with the :ref:`quick-start-label`.


Install ``b2luigi`` with the Belle II software
----------------------------------------------

In the current versions of the Belle II software externals, ``b2luigi`` is not included.
The main reason is to have a faster release cycle for ``b2luigi``.
It is very highly recommended to create a `b2venv <https://software.belle2.org/development/sphinx/build/tools_doc/b2venv.html>`_ and install ``b2luigi`` there.
Please also have a look into the :ref:`basf2-example-label`.

Local Development Installation
------------------------------

See the :ref:`development-label` section for more information on how to install ``b2luigi`` for development.
