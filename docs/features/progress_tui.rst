.. _progress-tui-label:

Progress TUI
============

.. warning::
   This is an experimental feature and may change in the future. Please report any issues you encounter when using it.


``b2luigi`` ships with an optional terminal-based progress interface (TUI) built on
`Textual <https://textual.textualize.io/>`_.
It shows a live task tree grouped by task class, with colour-coded progress bars and
real-time status updates.

Installation
------------

The TUI requires an optional dependency group:

.. code-block:: bash

    pip install b2luigi[tui]

Usage
-----

Pass ``--tui`` on the command line:

.. code-block:: bash

    python my_workflow.py --tui

or set ``progress_tui=True`` in :meth:`b2luigi.process`:

.. code-block:: python

    b2luigi.process(MyTask(), progress_tui=True)

Key bindings
------------

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Key
     - Action
   * - ``j`` / ``↓``
     - Move cursor down
   * - ``k`` / ``↑``
     - Move cursor up
   * - ``f``
     - Fold / unfold the selected task group
   * - ``d``
     - Toggle debug log view (shows live ``b2luigi`` log output)
   * - ``o``
     - View ``stdout`` log of the selected task instance
   * - ``e``
     - View ``stderr`` log of the selected task instance
   * - ``b`` / ``Escape``
     - Close log view and return to the progress view
   * - ``q``
     - Quit and stop all running tasks

Log files
---------

The ``o`` and ``e`` keys open the ``stdout`` and ``stderr`` files written by b2luigi
for the currently selected task instance (navigate into an expanded group first).
The files are read from the path returned by :func:`b2luigi.core.utils.get_log_file_dir`.
If a log file does not exist yet (task has not run, or not running in batch mode), a warning is shown at the bottom
of the progress view.
