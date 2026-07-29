.. _cli-label:

The b2luigi CLI
===============

``b2luigi`` ships a dedicated command-line tool, ``b2luigi``, that is
installed automatically with the package.  It provides a structured
interface for running, inspecting, and managing your task graphs and
replaces the legacy ``python file.py --flag`` invocation style.

.. note::

    If you are currently using ``python file.py --batch`` or
    ``python file.py --dry-run``, the equivalent new commands are
    ``b2luigi run`` and ``b2luigi run --dry``.
    The legacy flags are still supported; see :ref:`run-modes-label`.

Subcommands overview
--------------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Command
     - Description
   * - ``b2luigi run``
     - Run one or more tasks (local, batch, or test mode)
   * - ``b2luigi tasks``
     - List all task classes available in tasks.py and show their parameters
   * - ``b2luigi show``
     - Display output file status for the dependency tree
   * - ``b2luigi graph``
     - Render the task dependency graph as a Rich terminal tree or Graphviz DOT
   * - ``b2luigi remove``
     - Delete output files for one or more tasks
   * - ``b2luigi test``
     - Wrap an arbitrary script as a b2luigi task for debugging
   * - ``b2luigi about``
     - Print environment and installation information
   * - ``b2luigi init``
     - Scaffold a starter ``tasks.py`` and ``parameters.py`` in the current directory
   * - ``b2luigi version``
     - Print the installed version string
   * - ``b2luigi self-update``
     - Upgrade b2luigi via pip in the current environment

Project layout
--------------

The ``b2luigi`` CLI resolves your task definitions and parameter
configurations from two files in the working directory:

.. code-block:: text

    my_project/
    ├── tasks.py          # task definitions (required)
    └── parameters.py     # parameter config (optional)

``parameters.py`` is optional.  For a single run pass parameters directly
via ``--param key=value``; ``parameters.py`` is only needed when using
:class:`~b2luigi.cli.parameter_generator.ParameterGenerator` for multi-value sweeps.

You can override these paths with flags or environment variables:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - CLI flag
     - Environment variable
     - Description
   * - ``--task-file``
     - ``B2LUIGI_TASK_FILE``
     - Path to the task definitions file
   * - ``--params-file``
     - ``B2LUIGI_PARAMS_FILE``
     - Path to the parameters file

b2luigi run
-----------

Run one or more tasks.  With explicit parameter values:

.. code-block:: bash

    b2luigi run MyTask --my-parameter 3

When a ``parameters.py`` file is present, you can omit all parameter
flags — ``b2luigi run MyTask`` will read the configuration automatically:

.. code-block:: bash

    b2luigi run MyTask

Using ``ParameterGenerator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ParameterGenerator`` lets you fan out over many parameter combinations
directly inside ``parameters.py``:

.. code-block:: python

    # parameters.py
    from b2luigi import ParameterGenerator

    config = {
        "my_parameter": ParameterGenerator([1, 2, 3]),
    }

A single ``b2luigi run`` then schedules one task instance per value.
``ZippedParameterGenerator`` pairs values across multiple parameters
positionally instead of taking their Cartesian product:

.. code-block:: python

    from b2luigi import ZippedParameterGenerator

    config = ZippedParameterGenerator(
        my_parameter=[1, 2, 3],
        other_parameter=["a", "b", "c"],
    )

See :ref:`cli-api-label` for the full class reference.

b2luigi show
------------

With no arguments, ``b2luigi show`` renders the full dependency tree with
colour-coded output status for every task:

.. code-block:: bash

    b2luigi show

To inspect the outputs of a specific task, pass its class name as a positional argument:

.. code-block:: bash

    b2luigi show MyTask

Use ``--with-requirements`` to traverse the full requirement tree downward
from the named task (showing all tasks it transitively depends on):

.. code-block:: bash

    b2luigi show MyTask --with-requirements

Output is colour-coded: **green** means the file exists, **red** means it
is missing.

b2luigi graph
-------------

Render the full task dependency graph in the terminal:

.. code-block:: bash

    b2luigi graph

Scope the graph to a specific task and its requirements:

.. code-block:: bash

    b2luigi graph MyTask

Add ``--params`` to show parameter values on each node, and ``--status``
to show a completion indicator (✓ / ✗) for every task:

.. code-block:: bash

    b2luigi graph --params --status

Export to `Graphviz <https://graphviz.org/>`_ DOT format and pipe it to
``dot`` to produce an image:

.. code-block:: bash

    b2luigi graph --format dot | dot -Tpng -o graph.png
    b2luigi graph --format dot > graph.dot   # save first, render later

.. note::

    DOT export requires Graphviz to be installed (``brew install graphviz``
    on macOS, ``apt install graphviz`` on Debian/Ubuntu).

b2luigi remove
--------------

Remove the output files of one or more named tasks:

.. code-block:: bash

    b2luigi remove MyTask

Add ``-y`` to skip the confirmation prompt:

.. code-block:: bash

    b2luigi remove MyTask -y

Use ``--with-requirements`` to also remove all tasks that ``MyTask``
transitively depends on:

.. code-block:: bash

    b2luigi remove MyTask --with-requirements -y

b2luigi test
------------

Wrap an arbitrary script as a b2luigi task for local debugging:

.. code-block:: bash

    b2luigi test -s my_script.py -o output_file.txt

The ``-s`` flag specifies the script to run; ``-o`` names the output file
the task is expected to produce.

When submitting to a real batch system via ``--batch``, use ``--env-script``
to source an environment setup script before the job runs (only takes effect
combined with ``--batch``; a no-op otherwise):

.. code-block:: bash

    b2luigi test -s my_script.py -o output_file.txt --batch --env-script setup.sh

Use ``--setting key=value`` (repeatable, JSON-aware) to override any other
b2luigi setting for this run without creating a ``settings.json``, e.g.
``--setting apptainer_image=my_image.sif``.

Utility commands
----------------

``b2luigi about``
    Print version, Python, platform, working directory, and current
    environment variable overrides.

``b2luigi init``
    Scaffold a minimal ``tasks.py`` and ``parameters.py`` in the current
    working directory.  Use ``--force`` to overwrite existing files.

``b2luigi version``
    Print the installed version string and exit.

``b2luigi self-update``
    Upgrade b2luigi to the latest available version via pip.

Shell auto-completion
---------------------

``b2luigi`` ships with shell auto-completion for subcommand names and flag
names.  To install it for your current shell:

.. code-block:: bash

    b2luigi --install-completion

After sourcing your shell's rc file (``~/.bashrc``, ``~/.zshrc``, etc.),
pressing :kbd:`Tab` after ``b2luigi`` will complete subcommand names and
flag names automatically.

To print the completion script without installing it:

.. code-block:: bash

    b2luigi --show-completion

Supported shells: **bash**, **zsh**, **fish**, and **PowerShell**.

Migration from the legacy interface
------------------------------------

If you are currently calling ``python file.py --batch`` or similar legacy
flags, the equivalent ``b2luigi`` commands are:

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Legacy command
     - New CLI equivalent
   * - ``python tasks.py``
     - ``b2luigi run <CLASSNAME>``
   * - ``python tasks.py --batch``
     - ``b2luigi run <CLASSNAME> --batch``
   * - ``python tasks.py --dry-run``
     - ``b2luigi run <CLASSNAME> --dry``
   * - ``python tasks.py --show-output``
     - ``b2luigi show``
   * - ``python tasks.py --remove``
     - ``b2luigi remove``
   * - ``python tasks.py --test``
     - ``b2luigi test``

The legacy flags are still supported when calling ``b2luigi.process()``
directly in a script.  See :ref:`run-modes-label` for the full reference.
