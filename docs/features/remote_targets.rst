.. _remote-targets-label:

Remote Targets
==============

To ease the work with files stored on the grid or local dCache instances, ``b2luigi`` provides an Target implementation for so-called remote targets.
At this moment, two different remote file systems are supported: `XRootD <https://xrootd.org/docs.html>`_ and `WebDAV <https://en.wikipedia.org/wiki/WebDAV>`_.
Community contributions for different systems and protocols are very much welcome!

XRootD
------

In the background, the implementation for the :obj:`XRootDSystem` relies on the `Python bindings <https://pypi.org/project/xrootd/>`_ of the XRootD client and requires a working XRootD client installation.

.. hint::
    For Belle II users, the XRootD client is already installed in basf2 environments.

.. hint::
    To access the remote storage you will need a valid VOMS proxy.

WebDAV
------

In the background, the implementation for the :obj:`WebDAVSystem` relies on the `webdavclient3 Python package <https://pypi.org/project/webdavclient3/>`_ and is included in the dependencies of ``b2luigi``.

.. hint::
    To access the remote storage you will need a valid VOMS proxy and need to know the location of the certificates enabling the communication to the remote server.
    For Belle II users, the certificates can be found under ``/cvmfs/belle.kek.jp/grid/setup/Belle-KEK/6.1.1/Linux-x86_64`` in the folder ``/etc/grid-security/certificates`` (or other ``gbasf2`` versions).

.. hint::
    For the :obj:`WebDAVSystem` to work, you need to set ``X509_USER_PROXY`` and ``X509_CERT_DIR`` either as ``b2luigi`` setting or as environment variable.

    ``X509_USER_PROXY`` points to the location of the user proxy, usually of the form `/tmp/x509up_uXXXXX`.

    ``X509_CERT_DIR`` points to the location of the certificates.

General Usage
=============

To use the targets, you will have to pass the :obj:`RemoteTarget` class and the keyword arguments to the :meth:`add_to_output <b2luigi.Task.add_to_output>` function.
This requires some additional setup compared to the standard :class:`LocalTarget <b2luigi.core.target.LocalTarget>`.

First you need to create a ``FileSystem`` object, which is used to connect to the remote server.
For that you need to provide the server address like so: ``root://<server_address>:<port>`` in case of XRootD or ``https://<server_address>:<port>`` for WebDAV (``davs`` is currently not supported).

Optionally, one can also set a ``scratch_dir`` which will be used to store temporary files when using the ``temporary_path`` context. (see the `luigi documentation <https://luigi.readthedocs.io/en/stable/api/luigi.target.html#luigi.target.FileSystemTarget.temporary_path>`_)

When entering this context, a temporary path will be created in the ``scratch_dir``.
At leaving the context, the file will then be copied to the final location on the remote storage.

A full task using :obj:`RemoteTarget`\ s could look like this:

.. code-block:: python

        from b2luigi import XRootDSystem, RemoteTarget
        from b2luigi.core.utils import create_output_filename
        import b2luigi

        class MyTask(b2luigi.Task):
            def run(self):
                file_name = "Hello_world.txt"
                target = self._get_output_target(file_name)
                with target.temporary_path() as temp_path:
                    with open(temp_path, "w") as f:
                        f.write("Hello World")

            def output(self):
                fs = XRootDSystem("root://eospublic.cern.ch")
                yield self.add_to_output("Hello_world.txt", RemoteTarget, file_system=fs)

Another example could be:

.. code-block:: python

    import b2luigi

    class MyTask(b2luigi.Task):
        @property
        def result_dir(self):
            return "/path/on/server"

        @property
        def xrootdsystem(self):
            return b2luigi.XRootDSystem("root://eospublic.cern.ch")

        @property
        def default_task_target_class(self):
            return b2luigi.RemoteTarget

        @property
        def target_class_kwargs(self):
            return {
                "file_system": self.xrootdsystem,
            }

        def output(self):
            yield self.add_to_output("Hello_world.txt")

        @b2luigi.on_temporary_files
        def run(self):
            with open(self.get_output_file_name("Hello_world.txt"), "w") as f:
                f.write("Hello World")
