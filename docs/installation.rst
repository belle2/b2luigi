.. _installation-label:

Installation
============

Install b2luigi via pip
---------------

This installation description is for the general user. If you are using the Belle II software, see below:

1.  Setup your local environment.
    For example, run:

    .. code-block:: bash

        source venv/bin/activate

2.  Install b2luigi from pipy into your environment.

    a.  If you have a local installation, you can use the normal setup command

    .. code-block:: bash

        python -m pip install b2luigi


    b.  If this fails because you do not have write access to where your virtual environment lives, you can also install
        b2luigi locally:

    .. code-block:: bash

        python -m pip install --user b2luigi

    This will automatically also install `luigi` into your current environment.
    Please make sure to always setup your environment correctly before using `b2luigi`.

Now you can go on with the :ref:`quick-start-label`.


Install b2luigi with the Belle II software
------------------------------------------

In the current versions of the Belle II software externals, b2luigi is not included. 
The main reason is to have a faster release cycle for b2luigi.
It is very highly recommended to create a `b2venv <https://software.belle2.org/development/sphinx/build/tools_doc/b2venv.html>`_ and install b2luigi there.

Some older releases of the Belle II software externals might include old b2luigi versions. 
In these cases, using b2venv to install the latest version of b2luigi might have unintentional side effects.

.. attention::
    The examples in this documentation are all shown with calling ``python``,
    assuming this refers to the *Python 3* executable of their (virtual) environment.
    In some systems and e.g. basf2 environments, ``python`` refers to Python 2
    (not supported by b2luigi). Then, ``python3`` should be used instead.

Please also have a look into the :ref:`basf2-example-label`.

Local Development Installation
------------------------------

See the `Development`_ section for more information on how to install b2luigi for development.