.. _`api-documentation-label`:

API Documentation
=================

``b2luigi`` summarizes different topics to help you in your everyday task
creation and processing.
Most important is the :meth:`b2luigi.process` function, which lets you run
arbitrary task graphs on the batch.
It is very similar to ``luigi.build``, but lets you hand in additional parameters
for steering the batch execution.

Top-Level Function
------------------

.. autofunction:: b2luigi.process

Super-hero Task Classes
-----------------------

If you want to use the default ``luigi.Task`` class or any derivative of it,
you are totally fine.
No need to change any of your scripts!
But if you want to take advantage of some of the recipies we have developed
to work with large ``luigi`` task sets, you can use the drop in replacements
from the ``b2luigi`` package.
All task classes (except the :class:`b2luigi.DispatchableTask`, see below) are superclasses of
a ``luigi`` class.
As we import ``luigi`` into ``b2luigi``, you just need to replace

.. code-block:: python

    import luigi

with

.. code-block:: python

    import b2luigi as luigi

and you will have all the functionality of ``luigi`` and ``b2luigi``
without the need to change anything!

.. autoclass:: b2luigi.Task
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: b2luigi.ExternalTask
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: b2luigi.WrapperTask
    :members:
    :undoc-members:
    :show-inheritance:

.. autofunction:: b2luigi.dispatch

.. autoclass:: b2luigi.DispatchableTask
    :members: process
    :show-inheritance:

Parameters
----------

As ``b2luigi`` automatically also imports ``luigi``, you can use all the parameters from ``luigi``
you know and love.
We have just added a single new flag called ``hashed`` to the parameters constructor.
Turning it to true (it is turned off by default) will make ``b2luigi`` use a hashed version
of the parameters value, when constructing output or log file paths.
This is especially useful if you have parameters, which may include "dangerous" characters, like "/" or "{" (e.g.
when using list or dictionary parameters).
See also one of our :ref:`faq-label`.

XrootDTargets
-------------

To ease the work with files stored on the grid, b2luigi provides an Target implementation using XRootD.
In the background, this relies on the Python bindings of the XRootD client and of course requires a working XRootD client installation.

.. hint::
    The XRootD client is already installed in basf2 enviroments on cvmfs.

.. hint::
    To access XRootD storage you will need a valid VOMS proxy.

To use the targets, you will need to overwrite the ``_get_output_file_target`` function of your Task.
This requires some additional setup compared to the standard `LocalTarget`.
First you need to create a `FileSystem` object, which is used to connect to the XRootD server.
For that you need to provide the server address like so: ``root://<server_address>``.
Optionally, one can also set a `scratch_dir` which will be used to store temporary files when using the `temporary_path` context. (https://luigi.readthedocs.io/en/stable/api/luigi.target.html)
When entering this context, a temporary path will be created in the scratch_directory.
At leaving the context, the file will then be copied to the final location on the XRootD storage.
A full task using XrootDTargets could look like this:

.. code-block:: python

        from b2luigi.targets import XRootDTarget
        from b2luigi.core.utils import create_output_filename
        import b2luigi

        class MyTask(b2luigi.Task):
            def _get_output_file_target(self, base_filename: str, **kwargs) -> b2luigi.Target:
                filename = create_output_filename(self, base_filename,  result_dir="<path on your xrootd storage>")
                fs = XrootDSystem("root://eospublic.cern.ch")
                return XrootDTarget(filename, fs, scratch_dir)

            def run(self):
                file_name = "Hello_world.txt"
                target = self._get_output_file_target(file_name)
                with target.temporary_path() as temp_path:
                    with open(temp_path, "w") as f:
                        f.write("Hello World")


            def output(self):
                yield self.add_to_output("Hello_world.txt")

.. autoclass:: b2luigi.XrootDSystem
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass::b2luigi.XRootDTarget
    :members:
    :undoc-members:
    :show-inheritance:

Settings
--------

.. autofunction:: b2luigi.get_setting
.. autofunction:: b2luigi.set_setting
.. autofunction:: b2luigi.clear_setting


Other functions
---------------

.. autofunction:: b2luigi.on_temporary_files
.. autofunction:: b2luigi.core.utils.product_dict

.. toctree::
    :maxdepth: 1

    b2luigi.basf2_helper
