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

.. autoclass::b2luigi.XrootDTarget
    :members:
    :undoc-members:
    :show-inheritance:
