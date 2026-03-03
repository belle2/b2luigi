from pathlib import Path
import shutil
import tempfile
from luigi.target import FileSystem
import os
from contextlib import contextmanager
import logging
from typing import List, Optional, Generator
from b2luigi.core.target import FileSystemTarget
from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task


def ensure_requests_ca_bundle(extra: str) -> None:
    """
    Ensure REQUESTS_CA_BUNDLE includes `extra` (file or directory).

    Always converts everything into a temporary directory-based
    CA store and rehashes it.
    """

    extra_path = Path(extra).expanduser().resolve()
    if not extra_path.exists():
        raise FileNotFoundError(extra_path)

    existing = os.environ.get("REQUESTS_CA_BUNDLE")

    # If nothing is set, just use extra directly
    if not existing:
        os.environ["REQUESTS_CA_BUNDLE"] = str(extra_path)
        return

    existing_path = Path(existing).expanduser().resolve()
    if not existing_path.exists():
        raise FileNotFoundError(existing_path)

    if existing_path == extra_path:
        return

    # Always merge into a new temporary directory
    # (slow if directories are well populated,
    # but ensures a clean state and works for both files and directories)
    temp_dir_obj = tempfile.TemporaryDirectory(prefix="requests-ca-dir-")
    temp_dir = Path(temp_dir_obj.name)

    def copy_into_dir(src: Path):
        if src.is_dir():
            shutil.copytree(src, temp_dir, dirs_exist_ok=True)
        else:
            shutil.copy2(src, temp_dir / src.name)

    copy_into_dir(existing_path)
    copy_into_dir(extra_path)

    os.environ["REQUESTS_CA_BUNDLE"] = str(temp_dir)

    # Keep reference alive so TemporaryDirectory isn't GC’d
    ensure_requests_ca_bundle._temp_dir = temp_dir_obj


class WebDAVSystem(FileSystem):
    """
    WebDAV file system for ``b2luigi`` Targets. Inspiration taken from `RHofsaess <https://github.com/RHofsaess/xrd-interactive/blob/main/XRootD_utils.py>`_.
    It implements some standard file system operations, which can be used by the :obj:`WebDAVTarget`.
    The error handling is done by assertions, since WebDAV does not raise exceptions.
    """

    def __init__(self, server_path: str) -> None:
        """
        Args:
            server_path: Path to the server, e.g. root://eosuser.cern.ch/
        """
        try:
            from webdav3.client import Client

        except ModuleNotFoundError as err:
            logging.error("The WebDAV python package is not imported.")
            raise err

        self.server_path: str = server_path
        options = {
            "webdav_hostname": server_path,
        }
        self.client = Client(options)

        # Get the X509 proxy path from settings or environment variables
        x509_proxy = get_setting("X509_USER_PROXY", os.environ.get("X509_USER_PROXY", None))
        if x509_proxy is None:
            raise ValueError("X509_USER_PROXY is not set in environment variables or settings.")

        self.client.session.cert = x509_proxy

        # Get the X509 certificate directory from settings or environment variables
        x509_cert_dir = get_setting("X509_CERT_DIR", os.environ.get("X509_CERT_DIR", None))
        if x509_cert_dir is None:
            raise ValueError("X509_CERT_DIR is not set in environment variables or settings.")

        # Export as environment variable for the requests library used by webdav3
        ensure_requests_ca_bundle(x509_cert_dir)

    def exists(self, path: str) -> bool:
        """
        Implementation of the exists function for the WebDAVSystem.
        Will return True if the file or directory exists and False if it can not be found. This might also include cases, where the server is not reachable.

        Args:
            path (str): The path to check for existence.

        Returns:
            bool: True if the path exists, False otherwise..
        """

        return self.client.check(path)

    def copy_file_to_remote(self, local_path: str, remote_path: str, force: bool = False) -> None:
        """
        Function to copy a file from the local file system to the remote file system.
        In case the copy fails, a warning will be printed and a assertion will fail.

        Args:
            local_path (str): The path to the file on the local file system.
            remote_path (str): The destination path for the file on the remote file system.
            force (bool, optional): If True, overwrites the file on the remote system if it
                already exists. Defaults to False.

        Raises:
            AssertionError: If the file copy operation fails.
        """
        if force and self.client.check(remote_path):
            self.client.clean(remote_path=remote_path)
        self.client.upload_sync(remote_path=remote_path, local_path=local_path)

    def copy_file_from_remote(self, remote_path: str, local_path: str, force: bool = False) -> None:
        """
        Function to copy a file from the remote file system to the local file system.
        In case the copy fails, a warning will be printed and a assertion will fail.

        Args:
            remote_path: Path to the file on the remote file system.
            local_path: Path to the file on the local file system.
            force: If True, the file will be overwritten if it already exists. Default is False.

        This method uses the client to perform the file transfer. If the copy operation
        fails, a warning message is logged, and an assertion error is raised.

        Args:
            remote_path (str): The path to the file on the remote file system.
            local_path (str): The destination path for the file on the local file system.
            force (bool, optional): If True, overwrites the file on the local system if it
                already exists. Defaults to False.

        Raises:
            AssertionError: If the file copy operation fails.
        """
        path = Path(local_path)

        if path.exists() and force:
            path.unlink()

        self.client.download_sync(remote_path=remote_path, local_path=local_path)

    def copy_dir_from_remote(self, remote_path: str, local_path: str, force: bool = False) -> None:
        """
        Copies a directory and its files from the remote file system to the local file system.

        This method does not support nested directories. It iterates through the contents
        of the remote directory and copies each file to the specified local directory.

        Args:
            remote_path (str): Path to the directory on the remote file system.
            local_path (str): Path to the directory on the local file system.
            force (bool, optional): If True, overwrites files on the local system if they
                already exist. Defaults to False.
        """
        self.copy_file_from_remote(remote_path, local_path, force=force)

    def move(self, source_path: str, dest_path: str) -> None:
        """
        A function to move a file from one location to another on the XRootD server.
        In case the move fails, a warning will be printed and a assertion will fail.

        Args:
            source_path (str): The current path of the file on the remote file system.
            dest_path (str): The target path for the file on the remote file system.

        Raises:
            AssertionError: If the move operation fails.
        """
        self.client.move(remote_path_from=source_path, remote_path_to=dest_path)

    def mkdir(self, path: str, parents: bool = True, raise_if_exists: bool = False) -> None:
        """
        A function to create a directory on the remote file system.
        In case the creation fails, a warning will be printed and a assertion will fail.

        Args:
            path (str): The path of the directory to create.
        """
        self.client.mkdir(remote_path=path)

    def locate(self, path: str) -> bool:
        """
        Checks the location of a file on the remote file system.

        This method queries the remote file system to locate the specified file.
        If the operation fails, a warning is logged, and an assertion error is raised.

        Args:
            path (str): The path to the file on the remote file system.

        Returns:
            bool: True if the file location is successfully determined.
        """
        return self.client.check(path)

    def remove(self, path: str, recursive: bool = True, skip_trash: bool = True) -> None:
        """
        A function to remove a file from the remote file system.
        This function can not remove directories. Use ``remove_dir`` for that.
        In case the removal fails, a warning will be printed and a assertion will fail.

        Args:
            path: Path to the file on the remote file system.

        This method deletes a single file and does not support directory removal.

        Args:
            path (str): The path to the file on the remote file system.
        """
        self.client.clean(remote_path=path)

    def listdir(self, path: str) -> List[str]:
        """
        Lists the contents of a directory on the remote file system.

        This method retrieves the directory listing and categorizes entries as files or directories.
        If the operation fails, a warning is logged, and an assertion error is raised.

        Args:
            path (str): Path to the directory on the remote file system.

        Returns:
            List[str]: A list of paths to the entries in the directory.
        """
        return self.client.list(path)

    def remove_dir(self, path: str) -> None:
        """
        A function to iteratively remove a directory and all its content from the remote file system.
        In case the removal fails, a warning will be printed and a assertion will fail.

        Args:
            path (str): Path to the directory on the remote file system.

        Raises:
            AssertionError: If the directory removal operation fails.
        """
        self.remove(path=path)

    def rename_dont_move(self, path: str, dest: str) -> None:
        """
        Overwriting a the luigi function used to handle the atomic write problem.
        (See https://github.com/spotify/luigi/blob/master/luigi/target.py#L303)
        In this case it is just an alias for ``copy_file_to_remote`` with ``force=True``.

        Args:
            local_path: Path to the file on the local file system.
            remote_path: Path to the file on the remote file system.
        """
        self.copy_file_to_remote(path, dest, force=True)


class WebDAVTarget(FileSystemTarget):
    """
    Luigi target implementation for the WebDAV file system.
    """

    def __init__(self, path: str, file_system: WebDAVSystem):
        """
        Initialize the WebDAVTarget.

        Args:
            path (str): Path to the file on the remote file system.
            file_system (WebDAVSystem): Instance of the WebDAVSystem.
        """
        self._file_system = file_system
        super().__init__(path)

    @property
    def base_name(self) -> str:
        """Get the base name of the target path."""
        return os.path.basename(self.path)

    @property
    def fs(self) -> WebDAVSystem:
        """Get the associated file system."""
        return self._file_system

    def makedirs(self) -> None:
        """Create the target's directory on the remote file system."""
        self.fs.mkdir(self.path)

    def get(self, path: str = "~") -> str:
        """
        A function to copy the file from the remote file system to the local file system.

        Args:
            path: Path to copy the file to.

        Returns:
            Path to the copied file.
        """
        self.fs.copy_file_from_remote(self.path, f"{path}/{self.base_name}")
        return f"{path}/{self.base_name}"

    def open(self, mode: str) -> None:
        """Raise NotImplementedError as open is not supported."""
        raise NotImplementedError("WebDAVTarget does not support open yet.")

    @contextmanager
    def get_temporary_input(self, task: Optional[Task] = None, **tmp_file_kwargs) -> Generator[str, None, None]:
        """
        Create a temporary local copy of a remote input file.

        Downloads the remote file to a temporary local directory for processing,
        allowing safe concurrent access to the same remote input file by multiple tasks.

        Args:
            task (Optional[Task]): Task instance used to determine scratch directory settings.
                If None, uses default settings.

        Yields:
            str: Absolute path to the temporary local copy of the remote input file.

        Note:
            - The temporary file and its parent directory are automatically cleaned up
              when exiting the context manager.
            - Files are downloaded to the scratch directory specified in settings
              (defaults to the path returned by ``tempfile.gettempdir()`` if not set).

        Example:
            .. code-block:: python

                target = WebDAVTarget("https://server/path/data.root", fs)
                with target.get_temporary_input() as tmp_input:
                    process_local_file(tmp_input)
                # Temporary file is automatically cleaned up.
        """
        with tempfile.TemporaryDirectory(
            dir=get_setting("scratch_dir", task=task, default=tempfile.gettempdir())
        ) as tmp_dir:
            tmp_path = os.path.join(tmp_dir, self.base_name)
            self.fs.copy_file_from_remote(self.path, tmp_path)
            yield tmp_path
