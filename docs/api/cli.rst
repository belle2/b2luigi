.. _cli-api-label:

CLI Reference
=============

Command reference
-----------------

The ``b2luigi`` binary provides the following subcommands.  Run
``b2luigi <command> --help`` for full flag details.

.. code-block:: text

    b2luigi [OPTIONS] COMMAND [ARGS]...

        Run user-defined b2luigi tasks.

        Options:
          -V, -v, --version             Show the version and exit.
          --install-completion          Install shell completion.
          --show-completion             Print shell completion script.
          --help                        Show this message and exit.

    b2luigi run [OPTIONS] CLASSNAME

        Run a task class from tasks.py.

        Arguments:
          CLASSNAME                     The name of the task class to run.  [required]

        Options:
          -f, --task-file TEXT          Task definitions file (or $B2LUIGI_TASK_FILE)
          -p, --params-file TEXT        Parameters file (or $B2LUIGI_PARAMS_FILE)
          -P, --param TEXT              Override task parameters (repeatable):
                                        key=value.  Values are parsed as JSON when
                                        possible.
          -d, --dry                     Show which tasks would run, without executing.
          -b, --batch                   Submit tasks to the configured batch system.
          --scheduler-host TEXT         Host of a central Luigi scheduler.
          --scheduler-port INTEGER      Port of a central Luigi scheduler.

    b2luigi show [OPTIONS] [CLASSNAMES]...

        Show output files of task(s).  Without a task name shows the full
        dependency tree.

        Arguments:
          CLASSNAMES                    Task class name(s) to show.  Omit to show
                                        the full dependency tree for all tasks.

        Options:
          -f, --task-file TEXT          Task definitions file (or $B2LUIGI_TASK_FILE)
          -p, --params-file TEXT        Parameters file (or $B2LUIGI_PARAMS_FILE)
          -P, --param TEXT              Override task parameters (repeatable):
                                        key=value.
          --direct                      Skip dependency graph traversal.  Requires
                                        all target task parameters to be resolvable
                                        from parameters.py or --param.
          --with-requirements           Also show outputs of all tasks that the
                                        specified task(s) require.  Requires
                                        positional task name(s).

    b2luigi remove [OPTIONS] [CLASSNAMES]...

        Remove output files of task(s).

        Arguments:
          CLASSNAMES                    Task class name(s) to remove.  Omit to
                                        remove outputs for all tasks in tasks.py.

        Options:
          -f, --task-file TEXT          Task definitions file (or $B2LUIGI_TASK_FILE)
          -p, --params-file TEXT        Parameters file (or $B2LUIGI_PARAMS_FILE)
          -y, --yes                     Skip confirmation prompt.
          --keep TEXT                   Comma-separated task class names whose
                                        outputs should NOT be removed.
          -P, --param TEXT              Override task parameters (repeatable):
                                        key=value.
          --direct                      Skip dependency graph traversal.  Requires
                                        all target task parameters to be resolvable
                                        from parameters.py or --param.
          --with-requirements           Also remove outputs of all tasks that the
                                        named task(s) require.

    b2luigi tasks [OPTIONS] COMMAND [ARGS]...

        List and inspect available task classes.

        Options:
          -f, --task-file TEXT          Task definitions file (or $B2LUIGI_TASK_FILE)

        Commands:
          info [CLASSNAME]              Show docstring and parameters for a task
                                        class, or all task classes if omitted.

            Options:
              -f, --task-file TEXT      Task definitions file (or $B2LUIGI_TASK_FILE)

    b2luigi test [OPTIONS]

        Build a task from the given Python script and execute it as a b2luigi task.

        Options:
          -s TEXT                       Path to the Python script to execute.  [required]
          -o TEXT                       Output filename for the task target.  [required]
          -i TEXT                       Optional input filename; creates a prerequisite task.

    b2luigi about

        Show environment and b2luigi installation info.

    b2luigi init [OPTIONS]

        Create starter tasks.py, parameters.py, and optional config in the
        current directory.

        Options:
          --force                       Overwrite existing files.

    b2luigi version

        Print the installed version and exit.

    b2luigi self-update

        Upgrade b2luigi to the latest version in the current environment.

    b2luigi status [OPTIONS]

        Show the output status of the full dependency tree.

        Equivalent to ``b2luigi show`` with no task names — displays every task
        in the dependency tree together with whether its outputs exist.

        Options:
          -f, --task-file TEXT          Task definitions file (or $B2LUIGI_TASK_FILE)
          -p, --params-file TEXT        Parameters file (or $B2LUIGI_PARAMS_FILE)

Parameter generator classes
----------------------------

.. autoclass:: b2luigi.ParameterGenerator
   :members:

.. autoclass:: b2luigi.ZippedParameterGenerator
   :members:
