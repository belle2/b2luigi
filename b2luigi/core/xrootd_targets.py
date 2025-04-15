import tempfile
from luigi.target import FileSystem
import os
from contextlib import contextmanager
import logging
from typing import Any, Optional, Tuple, Dict, Generator
from b2luigi.core.target import FileSystemTarget
from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task


class XRootDSystem(FileSystem):
    """
    XRootD file system for ``b2luigi`` Targets. Inspiration taken from `RHofsaess <https://github.com/RHofsaess/xrd-interactive/blob/main/XRootD_utils.py>`_.
    It implements some standard file system operations, which can be used by the :obj:`XRootDTarget`.
    The error handling is done by assertions, since XRootD does not raise exceptions.
    """

    def __init__(self, server_path: str) -> None:
        """
        Args:
            server_path: Path to the server, e.g. root://eosuser.cern.ch/
        """
        try:
            from XRootD import client
            from XRootD.client.flags import DirListFlags, OpenFlags, MkDirFlags

        except ModuleNotFoundError as err:
            logging.error("The XRootD python package is not imported.")
            raise err

        self.dir_list_flags = DirListFlags
        self.open_flags = OpenFlags
        self.mk_dir_flags = MkDirFlags

        self.server_path = server_path
        self.client = client.FileSystem(self.server_path)

    def exists(self, path: str) -> bool:
        """
        Implementation of the exists function for the XRootDSystem.
        Will return True if the file or directory exists and False if it can not be found. This might also include cases, where the server is not reachable.

        Args:
            path (str): The path to check for existence.

        Returns:
            bool: True if the path exists, False otherwise..
        """

        status, _ = self.client.stat(path, self.dir_list_flags.STAT)
        if not status.ok:
            return False
        else:
            return True

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

        status, _ = self.client.copy("file://" + local_path, self.server_path + remote_path, force=force)
        if not status.ok:
            logging.warning(status.message)
        assert status.ok

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
        status, _ = self.client.copy(f"{self.server_path}/{remote_path}", local_path, force=force)
        if not status.ok:
            logging.warning(f"Failed to copy file from {remote_path} to {local_path}: {status.message}")
            if "file exists" in status.message:
                logging.info(f"File already exists: {local_path}")
                status.ok = True
        assert status.ok, f"File copy operation failed: {status.message}"

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
        _, directory_listing = self.listdir(remote_path)
        for entry in directory_listing:
            if entry.statinfo.size != 512:  # Check if the entry is not a directory
                logging.info(f"Copying file: {entry.name}")
                self.copy_file_from_remote(
                    os.path.join(remote_path, entry.name), os.path.join(local_path, entry.name), force=force
                )

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
        status, _ = self.client.mv(source_path, dest_path)
        if not status.ok:
            logging.warning(f"Failed to move file from {source_path} to {dest_path}: {status.message}")
        assert status.ok, f"Move operation failed: {status.message}"

    def mkdir(self, path: str) -> None:
        """
        A function to create a directory on the remote file system.
        In case the creation fails, a warning will be printed and a assertion will fail.

        Args:
            path (str): The path of the directory to create.

        Raises:
            AssertionError: If the directory creation operation fails.
        """
        dir_path, _ = os.path.split(path)
        if self.exists(dir_path):
            logging.warning(f"Directory already exists: {dir_path}")
            return

        status, _ = self.client.mkdir(dir_path, self.mk_dir_flags.MAKEPATH)
        if not status.ok:
            logging.warning(f"Failed to create directory {path}: {status.message}")
            if "File exists" in status.message:
                status.ok = True

        assert status.ok, f"Directory creation failed: {status.message}"

    def locate(self, path: str) -> bool:
        """
        Checks the location of a file on the remote file system.

        This method queries the remote file system to locate the specified file.
        If the operation fails, a warning is logged, and an assertion error is raised.

        Args:
            path (str): The path to the file on the remote file system.

        Returns:
            bool: True if the file location is successfully determined.

        Raises:
            AssertionError: If the locate operation fails.
        """
        status, locations = self.client.locate(path, self.open_flags.REFRESH)
        if not status.ok:
            logging.warning(f"Failed to locate file {path}: {status.message}")
        assert status.ok, f"Locate operation failed: {status.message}"
        return True

    def remove(self, path: str) -> None:
        """
        A function to remove a file from the remote file system.
        This function can not remove directories. Use ``remove_dir`` for that.
        In case the removal fails, a warning will be printed and a assertion will fail.

        Args:
            path: Path to the file on the remote file system.

        This method deletes a single file and does not support directory removal.

        Args:
            path (str): The path to the file on the remote file system.

        Raises:
            AssertionError: If the file removal operation fails.
        """
        status, _ = self.client.rm(path)
        if not status.ok:
            logging.warning(f"Failed to remove file {path}: {status.message}")
        assert status.ok, f"File removal failed: {status.message}"

    def listdir(self, path: str, print_entries: bool = False) -> Tuple[Dict[str, int], Any]:
        """
        Lists the contents of a directory on the remote file system.

        This method retrieves the directory listing and categorizes entries as files or directories.
        If the operation fails, a warning is logged, and an assertion error is raised.

        Args:
            path (str): Path to the directory on the remote file system.

        Returns:
            Tuple[Dict[str, int], Any]: A dictionary mapping paths to their type (1 for directories, 0 for files),
            and the raw listing object.

        Raises:
            AssertionError: If the directory listing operation fails.
        """
        dir_dict = {}
        status, listing = self.client.dirlist(path, self.dir_list_flags.STAT)
        if not status.ok:
            logging.warning(f"Failed to list directory {path}: {status.message}")
        assert status.ok, f"Directory listing failed: {status.message}"

        for entry in listing:
            if entry.statinfo.flags in {51, 19}:  # Directory flags
                assert entry.statinfo.size == 512, "Unexpected size for a directory entry"
                dir_dict[f"{listing.parent}{entry.name}/"] = 1
            elif entry.statinfo.flags in {48, 16}:  # File flags
                dir_dict[f"{listing.parent}{entry.name}"] = 0
            else:
                logging.warning(f"[get_directory_listing] Info: {entry}")
                exit("Unknown flags. RO files, strange permissions?")
            if print_entries:
                print(entry.name, f"{entry.statinfo.size/1024/1024} MB")
        return dir_dict, listing

    def remove_dir(self, path: str) -> None:
        """
        A function to iteratively remove a directory and all its content from the remote file system.
        In case the removal fails, a warning will be printed and a assertion will fail.

        Args:
            path (str): Path to the directory on the remote file system.

        Raises:
            AssertionError: If the directory removal operation fails.
        """
        status, listing = self.client.dirlist(path, self.dir_list_flags.STAT)
        if not status.ok:
            logging.warning(f"Failed to list directory {path}: {status.message}")
        assert status.ok, f"Directory listing failed: {status.message}"

        for entry in listing:
            entry_path = os.path.join(listing.parent, entry.name)
            if entry.statinfo.size == 512:  # Check if the entry is a directory
                logging.info(f"Recursively removing directory: {entry_path}")
                assert entry.statinfo.flags in {51, 19}, "Unexpected flags for a directory entry"
                self.remove_dir(entry_path)
            else:
                logging.info(f"Removing file: {entry_path}")
                self.remove(entry_path)

        status, _ = self.client.rmdir(path)  # Remove the now-empty directory
        logging.info(f"Status: {status.message}")
        if not status.ok:
            logging.warning(f"Failed to remove directory {path}: {status.message}")
        assert status.ok, f"Directory removal failed: {status.message}"

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


class XRootDTarget(FileSystemTarget):
    """
    Luigi target implementation for the XRootD file system.
    """

    def __init__(self, path: str, file_system: XRootDSystem):
        """
        Initialize the XRootDTarget.

        Args:
            path (str): Path to the file on the remote file system.
            file_system (XRootDSystem): Instance of the XRootDSystem.
        """
        self._file_system = file_system
        super().__init__(path)

    @property
    def base_name(self) -> str:
        """Get the base name of the target path."""
        return os.path.basename(self.path)

    @property
    def fs(self) -> XRootDSystem:
        """Get the associated file system."""
        return self._file_system

    def makedirs(self) -> None:
        """Create the target's directory on the remote file system."""
        self.fs.mkdir(self.path)

    def move(self) -> None:
        """Move the target to a new location."""
        self.fs.move(self.path)

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
        raise NotImplementedError("XRootDTarget does not support open yet.")

    @contextmanager
    def get_temporary_input(self, task: Optional[Task] = None) -> Generator[str, None, None]:
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
              (defaults to /tmp if not set).

        Example:
            .. code-block:: python

                target = XRootDTarget("root://server/path/data.root", fs)
                with target.get_temporary_input() as tmp_input:
                    process_local_file(tmp_input)
                # Temporary file is automatically cleaned up.
        """
        with tempfile.TemporaryDirectory(dir=get_setting("scratch_dir", task=task, default="/tmp")) as tmp_dir:
            tmp_path = os.path.join(tmp_dir, self.base_name)
            self.fs.copy_file_from_remote(self.path, tmp_path)
            yield tmp_path
