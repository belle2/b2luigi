import tempfile
import luigi
from luigi.target import FileSystem
import os
from contextlib import contextmanager
from typing import Any, List, Optional, Tuple, Dict, Generator, Union
from b2luigi.core.target import FileSystemTarget
from b2luigi.core.settings import get_setting


class RemoteFileSystem(FileSystem):
    """
    Base remote file system for ``b2luigi`` Targets. Inspiration taken from `RHofsaess <https://github.com/RHofsaess/xrd-interactive/blob/main/XRootD_utils.py>`_.
    It implements some standard file system operations, which can be used by the :obj:`RemoteTarget`.
    """

    def __init__(self, server_path: str) -> None:
        self.server_path = server_path

    def exists(self, path: str) -> bool:
        """
        Implementation of the exists function for the RemoteFileSystem.
        Will return True if the file or directory exists and False if it can not be found. This might also include cases, where the server is not reachable.

        Args:
            path (str): The path to check for existence.

        Returns:
            bool: True if the path exists, False otherwise..
        """
        raise NotImplementedError("The exists method must be implemented by subclasses of RemoteFileSystem.")

    def copy_file_to_remote(self, local_path: str, remote_path: str, force: bool = False) -> None:
        """
        Function to copy a file from the local file system to the remote file system.

        Args:
            local_path (str): The path to the file on the local file system.
            remote_path (str): The destination path for the file on the remote file system.
            force (bool, optional): If True, overwrites the file on the remote system if it
                already exists. Defaults to False.
        """
        raise NotImplementedError(
            "The copy_file_to_remote method must be implemented by subclasses of RemoteFileSystem."
        )

    def copy_file_from_remote(self, remote_path: str, local_path: str, force: bool = False) -> None:
        """
        Function to copy a file from the remote file system to the local file system.

        Args:
            remote_path: Path to the file on the remote file system.
            local_path: Path to the file on the local file system.
            force: If True, the file will be overwritten if it already exists. Default is False.
        """
        raise NotImplementedError(
            "The copy_file_from_remote method must be implemented by subclasses of RemoteFileSystem."
        )

    def copy_dir_from_remote(self, remote_path: str, local_path: str, force: bool = False) -> None:
        """
        Copies a directory and its files from the remote file system to the local file system.

        Args:
            remote_path (str): Path to the directory on the remote file system.
            local_path (str): Path to the directory on the local file system.
            force (bool, optional): If True, overwrites files on the local system if they
                already exist. Defaults to False.
        """
        raise NotImplementedError(
            "The copy_dir_from_remote method must be implemented by subclasses of RemoteFileSystem."
        )

    def move(self, source_path: str, dest_path: str) -> None:
        """
        A function to move a file from one location to another on the XRootD server.

        Args:
            source_path (str): The current path of the file on the remote file system.
            dest_path (str): The target path for the file on the remote file system.
        """
        raise NotImplementedError("The move method must be implemented by subclasses of RemoteFileSystem.")

    def mkdir(self, path: str, parents: bool = True, raise_if_exists: bool = False) -> None:
        """
        A function to create a directory on the remote file system.

        Args:
            path (str): The path of the directory to create.
        """
        raise NotImplementedError("The mkdir method must be implemented by subclasses of RemoteFileSystem.")

    def locate(self, path: str) -> bool:
        """
        Checks the location of a file on the remote file system.

        This method queries the remote file system to locate the specified file.

        Args:
            path (str): The path to the file on the remote file system.

        Returns:
            bool: True if the file location is successfully determined.
        """
        raise NotImplementedError("The locate method must be implemented by subclasses of RemoteFileSystem.")

    def remove(self, path: str, recursive: bool = True, skip_trash: bool = True) -> None:
        """
        This method deletes a single file and does not support directory removal.

        Args:
            path (str): The path to the file on the remote file system.
        """
        raise NotImplementedError("The remove method must be implemented by subclasses of RemoteFileSystem.")

    def listdir(self, path: str, print_entries: bool = False) -> Union[Tuple[Dict[str, int], Any], List[str]]:
        """
        Lists the contents of a directory on the remote file system.

        This method retrieves the directory listing and categorizes entries as files or directories.

        Args:
            path (str): Path to the directory on the remote file system.
        """
        raise NotImplementedError("The listdir method must be implemented by subclasses of RemoteFileSystem.")

    def remove_dir(self, path: str) -> None:
        """
        A function to iteratively remove a directory and all its content from the remote file system.

        Args:
            path (str): Path to the directory on the remote file system.
        """
        raise NotImplementedError("The remove_dir method must be implemented by subclasses of RemoteFileSystem.")

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


class RemoteTarget(FileSystemTarget):
    """
    Luigi target implementation for the remote file system.
    """

    def __init__(self, path: str, file_system: RemoteFileSystem):
        """
        Initialize the RemoteTarget.

        Args:
            path (str): Path to the file on the remote file system.
            file_system (RemoteFileSystem): Instance of the RemoteFileSystem.
        """
        self._file_system = file_system
        super().__init__(path)

    @property
    def base_name(self) -> str:
        """Get the base name of the target path."""
        return os.path.basename(self.path)

    @property
    def fs(self) -> RemoteFileSystem:
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
        raise NotImplementedError("RemoteTarget does not support open yet.")

    @contextmanager
    def get_temporary_input(self, task: Optional[luigi.Task] = None, **tmp_file_kwargs) -> Generator[str, None, None]:
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

                target = RemoteTarget("remote://server/path/data.root", fs)
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
