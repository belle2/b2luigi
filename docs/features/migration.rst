.. _migration-label:

Migrating to the New CLI
=========================

This guide walks through moving an existing project from the legacy
``python file.py --flag`` workflow to the ``b2luigi`` command-line tool. It
is a hands-on companion to :ref:`cli-label`; if you just need a quick
flag-equivalence lookup, see that page's mapping table instead.

.. note::

    Migration is entirely **opt-in**. The legacy workflow keeps working
    unchanged — nothing here is a breaking change, and you can adopt the new
    CLI at your own pace, task by task or not at all.

Starting point: a legacy project
---------------------------------

A typical pre-CLI project looks like this:

.. code-block:: python

    # tasks.py
    import b2luigi

    class MyTask(b2luigi.Task):
        split = b2luigi.IntParameter()

        def output(self):
            return self.add_to_output("result.root")

        def run(self):
            ...

    if __name__ == "__main__":
        b2luigi.process(MyTask(split=3))

...run from the command line as:

.. code-block:: bash

    python tasks.py --batch
    python tasks.py --dry-run
    python tasks.py --show-output
    python tasks.py --remove

Step 1 — install the CLI binary
---------------------------------

.. code-block:: bash

    pip install --upgrade b2luigi
    b2luigi --version

If ``b2luigi: command not found`` after installing, you likely have an older
``b2luigi`` (pre-1.3.0) still active in that environment — reinstall/upgrade
in the same environment your project uses. Editable installs
(``pip install -e ".[dev]"``) also work.

Step 2 — nothing changes yet
------------------------------

Your ``tasks.py`` file needs **no modifications** to start using the CLI.
The exact same task class defined above already works with:

.. code-block:: bash

    b2luigi run MyTask --param split=3

The CLI reads ``tasks.py`` from the current directory by default (override
with ``--task-file``/``-f`` or ``$B2LUIGI_TASK_FILE`` — see
:ref:`cli-label`'s Project layout section). The
``if __name__ == "__main__": b2luigi.process(...)`` block is simply never
executed when you invoke ``b2luigi run`` — it's only reached by
``python tasks.py`` directly. You can leave it in place (so
``python tasks.py --batch`` keeps working for teammates who haven't switched
yet) or remove it once your whole team has migrated.

Step 3 — swap the flags
-------------------------

Replace each legacy invocation with its CLI equivalent:

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Legacy
     - New CLI
   * - ``python tasks.py --batch``
     - ``b2luigi run MyTask --batch``
   * - ``python tasks.py --dry-run``
     - ``b2luigi run MyTask --dry``
   * - ``python tasks.py --show-output``
     - ``b2luigi show``
   * - ``python tasks.py --remove``
     - ``b2luigi remove MyTask``
   * - ``python tasks.py --test``
     - ``b2luigi test -s script.py -o output``

See :ref:`cli-label` for the full flag reference of each subcommand
(``--with-requirements``, ``--keep``, ``--details``, and so on all have no
legacy equivalent — they're new capabilities, not replacements).

What carries over unchanged
------------------------------

Nothing else about your project needs to change:

* ``settings.json`` is read exactly as before — same search path (current
  directory upward), same precedence rules.
* Batch system configuration (``batch_system``, HTCondor/Slurm/LSF-specific
  settings, ``env_script``, ``apptainer_image``, etc.) is unaffected.
* ``Task``, ``Parameter``, and target classes are unchanged — the CLI is a
  new front door onto the same execution engine, not a rewrite of the task
  model.
* Parameters passed positionally on the legacy command line become
  ``--param key=value`` flags (repeatable, JSON-aware — ``--param n=3``
  passes an ``int``); nothing needs to move into a config file unless you
  want it to (Step 4).

Step 4 — optional: adopt ``parameters.py``
---------------------------------------------

If your legacy script had to be re-run repeatedly with different parameter
values from the shell (a loop, a Makefile, a wrapper script), you can move
those values into a ``parameters.py`` file instead:

.. code-block:: python

    # parameters.py
    config = {
        "split": 3,
    }

.. code-block:: bash

    b2luigi run MyTask   # reads split=3 from parameters.py automatically

This step is entirely optional — ``--param`` flags work standalone with no
``parameters.py`` present at all.

Step 5 — optional: replace a hand-written ``WrapperTask`` with ``ParameterGenerator``
------------------------------------------------------------------------------------------

If your legacy workflow needed to run the same task once per parameter
value, you wrote a ``WrapperTask`` with a ``requires()`` loop over the
values, hardcoded directly in ``tasks.py`` — this predates the new CLI
entirely and is plain luigi/b2luigi:

.. code-block:: python

    # before: tasks.py
    import b2luigi

    class SplitWrapper(b2luigi.WrapperTask):
        def requires(self):
            return [MyTask(split=value) for value in [1, 2, 3]]

    if __name__ == "__main__":
        b2luigi.process(SplitWrapper())

.. code-block:: bash

    python tasks.py

``ParameterGenerator`` writes that same ``requires()`` loop for you from
``parameters.py`` config, so you no longer hand-author the wrapper task:

.. code-block:: python

    # after: parameters.py
    from b2luigi import ParameterGenerator

    config = {
        "split": ParameterGenerator([1, 2, 3]),
    }

.. code-block:: bash

    b2luigi run MyTask

Add ``--batch`` to either the legacy or the new-CLI command above to submit
to a real batch system instead of running locally — that flag's behavior is
unrelated to and unaffected by ``ParameterGenerator``; see
:ref:`cli-label` for its full reference.

See :ref:`cli-label`'s ``b2luigi run`` section for ``ZippedParameterGenerator``
(pairing multiple parameters positionally instead of taking their full
Cartesian product) and how ``--param`` overrides interact with generators.

Coexistence during a gradual team migration
-----------------------------------------------

Both invocation styles read and write to the exact same ``result_dir``
layout and the exact same ``settings.json``, so they can be used
side-by-side without conflict — one teammate running ``b2luigi run`` while
another still runs ``python tasks.py --batch`` on the same project produces
consistent, interchangeable output. There is no shared state to reconcile
and no cutover moment required. See :ref:`run-modes-label` for the full
legacy mode reference if you keep any scripts on that path long-term.

Troubleshooting
------------------

**"command not found: b2luigi" after installing**
    Check you installed into the environment you're actually using
    (``which python`` / ``which b2luigi`` should point at the same venv).
    A stale pre-1.3.0 install in a different environment won't have the
    ``b2luigi`` entry point.

**A ``ParameterGenerator``-driven ``run --batch`` reports a missing task class on the worker**
    Make sure you're on b2luigi >= 1.3.0 — earlier builds on this branch had
    a bug where the auto-generated wrapper task wasn't runnable on batch
    workers; it's fixed as of this release.

**Output files land in an unexpected location under** ``python -m b2luigi``
    Invoke the installed ``b2luigi`` console script directly rather than
    ``python -m b2luigi`` if you're on a b2luigi version predating this fix;
    current releases resolve this correctly either way.
