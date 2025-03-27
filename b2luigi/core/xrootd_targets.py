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
    XRootDFileSystem for b2luigi Targets. Inspiration taken from rhofsaess
    https://github.com/RHofsaess/xrd-interactive/blob/main/XRootD_utils.py
    It implements some standard file system operations, which can be used by the XRootDTarget.
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
        Will return True if the file or directory exists and Fals if it can not be found. This might also include cases, where the server is not reachable.

        Args:
            path: Path to the file or directory to check.
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
            local_path: Path to the file on the local file system.
            remote_path: Path to the file on the remote file system.
            force: If True, the file will be overwritten if it already exists. Default is False.
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

        """
        status, _ = self.client.copy(self.server_path + "//" + remote_path, local_path, force=force)
        if not status.ok:
            logging.warning(status.message)
            if "file exists" in status.message:
                logging.warning(f"File already exists: {local_path}")
                status.ok = True
        assert status.ok

    def copy_dir_from_remote(self, remote_path: str, local_path: str, force: bool = False) -> None:
        """
        A function to copy a directory with all its files from the remote file system to the local file system.
        Nested directories are not supported.

        Args:
            remote_path: Path to the directory on the remote file system.
            local_path: Path to the directory on the local file system.
            force: If True, the files will be overwritten if they already exist. Default is False.

        """
        _, list_dir = self.listdir(remote_path)
        for file in list_dir:
            if file.statinfo.size != 512:  # adhoc test, if the `file` is not a directory
                logging.warning(file.name)
                self.copy_file_from_remote(remote_path + "/" + file.name, local_path + file.name, force=force)

    def move(self, source_path: str, dest_path: str) -> None:
        """
        A function to move a file from one location to another on the XRootD server.
        In case the move fails, a warning will be printed and a assertion will fail.
        Args:
            source_path: Path to the file on the remote file system.
            dest_path: Path to the file on the remote file system.
        """
        status, _ = self.client.mv(source_path, dest_path)
        if not status.ok:
            logging.warning(status.message)
        assert status.ok

    def mkdir(self, path: str) -> None:
        """
        A function to create a directory on the remote file system.
        In case the creation fails, a warning will be printed and a assertion will fail.
        """

        dir_path, file_path = os.path.split(path)
        if self.exists(dir_path):
            logging.warning(f"dir already exists: {dir_path}")
            return
        status, _ = self.client.mkdir(dir_path, self.mk_dir_flags.MAKEPATH)
        if not status.ok:
            logging.warning(status.message, path)
            if "File exists" in status.message:
                status.ok = True
        assert status.ok

    def locate(self, path: str) -> bool:
        status, locations = self.client.locate(path, self.open_flags.REFRESH)
        if not status.ok:
            logging.warning(status.message)
        assert status.ok
        return True

    def remove(self, path: str) -> None:
        """
        A function to remove a file from the remote file system.
        This function can not remove directories. Use remove_dir for that.
        In case the removal fails, a warning will be printed and a assertion will fail.
        Args:
            path: Path to the file on the remote file system.

        """
        status, _ = self.client.rm(path)
        if not status.ok:
            logging.warning(status.message)
        assert status.ok

    def listdir(self, path: str) -> Tuple[Dict[str, int], Any]:
        """
        A function to list the content of a directory on the remote file system.
        In case the listing fails, a warning will be printed and a assertion will fail.
        Args:
            path: Path to the directory on the remote file system.
        """
        dir_dict = {}
        status, listing = self.client.dirlist(path, self.dir_list_flags.STAT)
        if not status.ok:
            logging.warning(f"[get_directory_listing] Status: {status.message}")
        assert status.ok  # directory or redirector faulty

        for entry in listing:
            # faster way to check if file or dir: less DDOS with only one query
            if entry.statinfo.flags == 51 or entry.statinfo.flags == 19:
                # directories have a size of 512
                assert entry.statinfo.size == 512  # just to make sure for the recursive stuff
                dir_dict[f"{listing.parent + entry.name}/"] = 1
            elif entry.statinfo.flags == 48 or entry.statinfo.flags == 16:
                dir_dict[f"{listing.parent + entry.name}"] = 0
            else:
                logging.warning(f"[get_directory_listing] Info: {entry}")
                exit("Unknown flags. RO files, strange permissions?")
            print(entry.name, f"{entry.statinfo.size/1024/1024} MB")
        return dir_dict, listing

    def remove_dir(self, path: str) -> None:
        """
        A function to iteratively remove a directory and all its content from the remote file system.
        In case the removal fails, a warning will be printed and a assertion will fail.
        Args:
            path: Path to the directory on the remote file system.
        """
        status, listing = self.client.dirlist(path, self.dir_list_flags.STAT)
        if not status.ok:
            logging.warning(f"Status: {status.message}")
        assert status.ok  # directory does not exists
        for file in listing:  # unfortunately, there is no recursive way in xrd...
            logging.info(f"{path}{listing.parent}{file.name}")
            if file.statinfo.size == 512:  # check if "file" is a directory -> delete recursively
                logging.warning(f"[rm dir] list entry: {file}")
                assert (
                    file.statinfo.flags == 51 or file.statinfo.flags == 19
                )  # make sure it is a directory; evtl wrong permissions?
                self.remove_dir(listing.parent + file.name)
            else:
                self.remove(listing.parent + file.name)
        status, _ = self.client.rmdir(path)  # when empty, remove empty dir
        if not status.ok:
            logging.info(f"Status: {status.message}")
        assert status.ok  # dir removal failed: check path or redirector

    def rename_dont_move(self, path: str, dest: str) -> None:
        self.copy_file_to_remote(path, dest, force=True)


class XRootDTarget(FileSystemTarget):
    """
    Implementation of luigi targets based on the XRootD file system.
    """

    def __init__(self, path: str, file_system: XRootDSystem, scratch_dir: str = "/tmp"):
        """
        Args:
            path: Path to the file on the remote file system.
            file_system: Instance of the XRootDSystem.
            scratch_dir: Directory to store temporary files.
        """
        self._scratch_dir = scratch_dir
        self._file_system = file_system
        super().__init__(path)

    @property
    def base_name(self) -> str:
        return os.path.basename(self.path)

    @property
    def fs(self) -> XRootDSystem:
        return self._file_system

    def makedirs(self) -> None:
        """
        Function to create the targets directory on the remote file system.
        """
        self.fs.mkdir(self.path)

    def move(self) -> None:
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
        raise NotImplementedError("XRootDTarget does not support open yet")

    @contextmanager
    def get_temporary_input(self, task: Optional[Task] = None) -> Generator[str, None, None]:
        with tempfile.TemporaryDirectory(dir=get_setting("scratch_dir", task=task, default="/tmp")) as tmp_path:
            tmp_path = os.path.join(tmp_path, self.tmp_name)
            self.fs.copy_file_from_remote(self.path, tmp_path)
            yield tmp_path
