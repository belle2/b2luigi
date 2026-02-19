.. _settings-collection-label:

Settings
========

``b2luigi`` offers various ways to control important paths and behaviors of your tasks.

How to set settings
--------------------
These settings can be set in the following ways, where each way overwrites the previous one:

1. Via a settings file (e.g. `settings.json`). This file can be given via the environment variable ``B2LUIGI_SETTINGS_JSON``.
   If this variable is not set, ``b2luigi`` will look for a file named `settings.json` in the current working directory.
2. You can set a setting in your script via the function :func:`b2luigi.set_setting <b2luigi.core.settings.set_setting>`.
3. You can use give a task the setting as a property.

For more details, check the API documentation of :func:`b2luigi.get_setting <b2luigi.core.settings.get_setting>`.

Available settings
------------------
Here is a list of available settings already implemented in ``b2luigi``:

General settings
++++++++++++++++

- ``result_dir``: String
    The directory where the results of the tasks are stored.
    This will be used as the base path when the unique target path is created.
    If not set, the current working directory is used.
    It is recommended to set this path per project via the settings file.

- ``log_dir``: String
    The directory where the logs of the tasks are stored.
    If not set, a folder named ``logs`` in the current working directory is used.
    It is recommended to set this path per project via the settings file.

- ``task_file_dir``: String
    The directory where additional files for the task, e.g. the executable wrapper, is stored.
    If not set, a folder named ``task_files`` in the current working directory is used.
    It is recommended to set this path per project via the settings file.

- ``scratch_dir``: String
    The directory where temporary files are written to when using the :meth:`b2luigi.on_temporary_files` context.
    This is used when targets are created with the :meth:`b2luigi.Task.add_to_output` method. If not set, the path
    returned by ``tempfile.gettempdir()`` is used to write temporary files (very likely ``/tmp`` is used unless
    the environment variables ``TMPDIR``, ``TEMP`` or ``TMP`` are set).

- ``batch_system``: String
    The batch system to use when executed in batch mode. Currently, ``htcondor``, ``lsf``, ``slurm``, ``gbasf2``, ``auto`` and ``local`` are supported.
    In case of ``auto``, b2luigi will try to detect the batch system automatically by checking for the executables of ``htcondor`` and ``lsf``.
    If none of them is found, the local mode is used.
    Please note, that this setting does not activate the batch mode. For that, use the ``--batch`` flag when calling your script.
    Default setting is ``auto``.

- ``_dispatch_local_execution``: Boolean
    Whether to use batch submission for ``local`` batch mode.
    Never touch this, since it is set automatically.
    Use the ``--batch`` flag when calling your script to activate batch mode.
    For more information, see :meth:`b2luigi.dispatch`.

- ``use_parameter_name_in_output``: Boolean
    If set to ``True``, the parameter name is used in the output file name. E.g.: `/result_dir/parameter1_name=value1/parameter2_name=value2/output.txt`.
    If set to ``False``, only the parameter valueeth is used in the output file name: `/result_dir/value1/value2/output.txt`.
    Default is ``True``.

- ``parameter_separator``: String
    The string used to separate parameter names and parameter values in the output path.

- ``n_download_threads``: int
    The number of parallel threads used to download input targets with the :meth:`b2luigi.Task.get_input_file_names` function.
    Defaults to ``2``. Set it to ``None`` to disable the usage of a ThreadPool.

- ``default_task_target_class``: Object
    The default target class to use when creating targets.
    This is used when the task uses :meth:`b2luigi.Task.add_to_output` to define a target and no ``target_class`` is set.
    Defaults to :class:`b2luigi.LocalTarget`.

- ``target_class_kwargs``: Dict
    The default target kwargs to use when creating targets.
    This is used when the task uses :meth:`b2luigi.Task.add_to_output` to define a target and no ``target_kwargs`` is set.

Apptainer settings
++++++++++++++++++

- ``apptainer_image``: String
    If set, the task will be executed in an apptainer image, if the batch systems ``local`` or ``lsf`` are used.

- ``apptainer_mounts``: List[String]
    A list of bind mounts into the apptainer container.
    If not set, no paths are mounted.
    Default is an empty list.

- ``apptainer_mount_defaults``: Boolean
    If set to ``True``, the ``result_dir`` and ``log_dir`` are mounted into the apptainer container by default.
    Default is ``True``.

- ``apptainer_additional_params``: List[String]
    A list of additional parameters to pass to the apptainer container.
    If not set, no additional parameters are passed.
    Default is an empty list.


Batch mode specific settings
++++++++++++++++++++++++++++

- ``job_name``: String
    If set, a job name will be set for ``slurm``, ``lsf`` and ``htcondor`` batch systems.
    For HTCondor, the ClassAdd ``JobBatchName`` is set to this value.
    For LSF, the ``-J`` flag is set to this value.
    By default it is not set.

- ``shell``: String
    Which shell to to start the executable wrapper with.
    Defaults to ``bash`` and only this shell is tested.

- ``working_dir``: String
    The working directory to use when executing the task on a ``htcondor`` or ``lsf`` batch system.
    Defaults to the directory of the main script.

- ``env_script``: String
    Path to a script to setup the environment.
    Used when creating an executable wrapper for ``htcondor`` or ``lsf`` batch systems.
    In most cases, it is not necessary to set this setting for ``lsf``.
    Defaults to an empty String.

- ``env``: Dict
    A dictionary to overwrite the environment variables.
    This is used when building the executable wrapper for ``htcondor`` or ``lsf`` batch systems.

- ``executable``: List[String]
    The executable to use when executing the task on a ``htcondor`` or ``lsf`` batch system.
    It defaults to the executable used for starting the script.
    Only change this setting if you know what you are doing.

- ``executable_prefix``: List[String]
    The prefix to use when executing the task on a ``htcondor`` or ``lsf`` batch system.
    It defaults to an empty list.
    Only change this setting if you know what you are doing.
    This setting can be used to debug remote execution by pre pending e.g. ``strace`` to the executable.

- ``add_filename_to_cmd``: Boolean
    Whether to add the filename the the `exec` command in the `executable_wrapper.sh`. Defaults to `True`.

- ``task_cmd_additional_args``: List[String]
    A list of additional Parameters to add the the `exec` command in the `executable_wrapper.sh`. Defaults to `[]`.

HTCondor specific settings
++++++++++++++++++++++++++

- ``htcondor_settings``: Dict
    A dictionary of settings used for the submit file.

    .. warning::
        This setting is first loaded from the settings file and then the task specific settings are added.
        It is recommended to set this setting via Task properties.

- ``transfer_files``: List[String]
    Files to be transferred from the HTCondor Job. The ``env_script`` is automatically included.
    It is set as default for the ``transfer_input_files`` in the ``htcondor_settings``.

LSF specific settings
+++++++++++++++++++++
- ``queue``: String
    The queue to submit to.
    Defaults to not setting any queue.


Slurm specific settings
+++++++++++++++++++++++

- ``slurm_settings``: Dict
    A dictionary of settings used for the submit file.


``gbasf2`` specific settings
++++++++++++++++++++++++++++
To see a list of b2luigi settings mapped to ``gbasf2`` command line options, see :class:`Gbasf2Process <b2luigi.batch.processes.gbasf2.Gbasf2Process>`.

Custom settings
---------------
You can use the settings mechanism to handle your own settings.
For that, set your settings, like you would normally do and access them via :meth:`b2luigi.get_setting`.
