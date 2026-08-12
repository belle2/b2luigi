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

``tasks.py`` is the entry point of task discovery, not a boundary. Two tiers
of task classes are addressable by name:

* **Manifest tasks** — every ``b2luigi.Task`` subclass in ``tasks.py``'s
  namespace, defined there or imported into it. Their bare class name always
  resolves, and takes precedence over any other class with the same name.
* **Project tasks** — every ``b2luigi.Task`` subclass that importing
  ``tasks.py`` loads from a module *inside your project directory* (the
  directory containing the task file). These are addressable by bare name
  when unique, and by their qualified ``module.ClassName`` (e.g.
  ``analysis.skim.SkimTask``) always. If two project modules define the same
  class name, the bare name is rejected as ambiguous and the error lists the
  qualified candidates.

.. code-block:: python

    # tasks.py
    from analysis.skim import SkimTask
    from analysis.reco import RecoTask

Your task code stays where it lives; importing a class into ``tasks.py`` is
what makes it addressable as a manifest task by ``b2luigi run``, ``tasks``,
``show``, ``graph`` and ``remove``. Task classes reachable only transitively
(for example a task that ``SkimTask.requires()`` depends on, in a module never
imported into ``tasks.py``) are still project tasks, addressable by their
qualified name. Classes belonging to ``b2luigi`` or ``luigi`` themselves (e.g.
``from b2luigi import Task``) are never treated as runnable tasks.

Task classes from installed libraries (outside the project directory) are
never addressable by name and never listed — they still appear in
``b2luigi graph`` and full-tree ``b2luigi show`` output when they are part of
the dependency graph. ``b2luigi tasks`` shows each task's module of origin.

Batch submission encodes each task's module, so a project task that is a
dependency of a submitted task executes correctly on the worker even if it
was never imported into ``tasks.py``.

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

.. _cli-param-precedence-label:

Precedence: ``--param`` overrides ``parameters.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a parameter is defined in both places, **the command line wins**.  The
``config`` dict from ``parameters.py`` is loaded first and each ``--param``
value is merged on top of it, key by key.  This applies identically to
``b2luigi run``, ``show``, ``remove`` and ``graph``.

Only the keys you actually pass are affected — everything else in
``parameters.py`` is left untouched:

.. code-block:: python

    # parameters.py
    config = {
        "my_parameter": 1,
        "other_parameter": "a",
    }

.. code-block:: bash

    b2luigi run MyTask --param my_parameter=2

runs with ``my_parameter=2`` and ``other_parameter="a"``.

Values are parsed as JSON when possible, so ``--param n=5`` yields the integer
``5`` and ``--param items=[1,2,3]`` a list (useful for a
:class:`luigi.ListParameter`).  Anything that is not valid JSON is kept as a
plain string.

.. warning::

    Overriding a :class:`~b2luigi.cli.parameter_generator.ParameterGenerator`
    from the command line **collapses the sweep to a single value**.  Given

    .. code-block:: python

        config = {"my_parameter": ParameterGenerator([1, 2, 3])}

    then ``b2luigi run MyTask`` schedules three task instances, but

    .. code-block:: bash

        b2luigi run MyTask --param my_parameter=2

    schedules exactly one.  This is the intended way to re-run a single point
    of a sweep — for example to reproduce one failed job — but it is easy to
    do by accident.

    Note that this holds for *any* ``--param`` value, including a JSON list.
    Only ``ParameterGenerator`` and ``ZippedParameterGenerator`` objects expand
    into multiple task instances, and those can only be constructed in
    ``parameters.py``.  Passing ``--param my_parameter=[1,2,3]`` therefore
    schedules one task whose ``my_parameter`` *is* the list ``[1, 2, 3]``, not
    three tasks.  To change the sweep itself, edit ``parameters.py``.

Parameters that do not apply
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``parameters.py`` is shared across every task in a project, so it may hold keys
that a given task does not declare. Those keys are filtered out rather than
treated as errors, and the affected command prints a single warning naming them:

.. code-block:: text

   Warning: ignoring parameters not declared by TaskB: number

A ``--param`` override is different. It is aimed at one invocation, so an
override that applies to no task is a mistake rather than a normal consequence
of sharing a config:

.. code-block:: bash

   b2luigi run TaskA --param numbr=99

.. code-block:: text

   Error: TaskA has no parameter 'numbr'. Did you mean 'number'?

``run`` always names a task, so an inapplicable override is always an error
there. ``show`` and ``graph`` error the same way when you name a task, and warn
instead when you do not, because a whole-tree listing legitimately spans tasks
that declare different parameters.

``remove`` is the deliberate exception to that rule: it errors on an
inapplicable override **whether or not** you name a task. It deletes files, and
quietly ignoring an override that was meant to narrow what gets deleted is the
dangerous direction.

The ``parameters.py`` warning above fires whenever a command errors on
overrides — that is, for ``run``, for ``remove``, and for ``show``/``graph``
with a task named. With no task named, ``show`` and ``graph`` drop inapplicable
``parameters.py`` keys silently: a whole-tree listing has nothing specific to
warn about, since it legitimately spans tasks with different parameters.

Warnings are written to standard error, so ``show --paths`` and
``graph --format dot`` stay pipeable.

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

Long output paths are never truncated: the ``Location`` column folds across lines
so every character stays on screen and can be selected or piped.

For a bare listing suitable for shell substitution, use ``--paths``:

.. code-block:: bash

    b2luigi show MyTask --paths

This prints one absolute output path per line with no table, styling, or status
column, so it composes with other tools:

.. code-block:: bash

    ls -lh $(b2luigi show MyTask --paths)

``--links`` additionally makes local paths clickable in terminals that support
hyperlinks:

.. code-block:: bash

    b2luigi show MyTask --links

Links are emitted only for local files, never for remote (XRootD or WebDAV)
targets. Be aware that a link resolves against the machine your *terminal* runs
on: when you run ``b2luigi show`` over SSH on a cluster, the link points at a path
on your local machine and will not find the file. This is why ``--links`` is
opt-in rather than the default.

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
``--setting apptainer_image=my_image.sif``. An apptainer/container run always
needs an environment setup script, so ``apptainer_image`` must be combined
with ``--env-script``; setting it alone raises
``ValueError: Apptainer execution requires an environment setup script.``.

Unlike ``settings.json``, which is re-read fresh by the batch worker,
``--setting`` overrides live only in the submitting process's in-memory
settings and are **never** propagated to the batch worker (the worker
reconstructs the task via ``batch-runner --script`` with no knowledge of
``--setting`` values). Use ``--setting`` for submission-side-only settings
such as ``apptainer_image``, ``env``, ``env_script``, or ``working_dir``.
For settings that both the submission host and the worker must agree on
(``result_dir``, ``log_dir``), use ``settings.json`` instead — otherwise the
submission side and the worker will resolve output paths differently under
``--batch``.

``--setting`` cannot override ``batch_system`` or ``env_script``: both are
already set as class attributes on the generated task (via ``--batch`` and
``--env-script`` respectively), and :func:`~b2luigi.core.settings.get_setting`
checks task attributes before global settings. Use ``--batch``/``--env-script``
for those two.

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

For a full worked walkthrough of moving an existing project over — what
carries over unchanged, how to adopt ``parameters.py`` and
``ParameterGenerator`` incrementally, and how the two interfaces coexist
during a gradual migration — see :ref:`migration-label`.
