from pathlib import Path
import shutil
import tempfile
import os
from typing import Any, Dict, List, Tuple, Union
from webdav3.client import Client

from b2luigi.core.remote_target import RemoteFileSystem, RemoteTarget
from b2luigi.core.settings import get_setting, _no_value


def ensure_requests_ca_bundle(extra: str) -> Path:
    """
    Ensure REQUESTS_CA_BUNDLE includes `extra` (file or directory).
    """

    extra_path = Path(extra).expanduser().resolve()
    if not extra_path.exists():
        raise FileNotFoundError(extra_path)

    existing = os.environ.get("REQUESTS_CA_BUNDLE")

    # If nothing is set, just use extra directly
    if not existing:
        os.environ["REQUESTS_CA_BUNDLE"] = str(extra_path)
        return extra_path

    existing_path = Path(existing).expanduser().resolve()
    if existing_path == extra_path:
        return extra_path

    # Always merge into a new temporary directory
    # (slow if directories are well populated,
    # but ensures a clean state and works for both files and directories)
    scratch_dir = Path(get_setting("scratch_dir", default=tempfile.gettempdir())).expanduser().resolve()
    cache_root = scratch_dir / "requests-ca"
    if cache_root.exists() and any(cache_root.iterdir()):
        return cache_root

    cache_root.mkdir(parents=True, exist_ok=True)

    def copy_into_dir(src: Path):
        if src.is_dir():
            shutil.copytree(src, cache_root, dirs_exist_ok=True)
        else:
            shutil.copy2(src, cache_root / src.name)

    copy_into_dir(existing_path)
    copy_into_dir(extra_path)

    os.environ["REQUESTS_CA_BUNDLE"] = str(cache_root)

    return cache_root


class WebDAVSystem(RemoteFileSystem):
    """
    WebDAV file system for ``b2luigi`` Targets. The ``davs`` protocol is not supported, but ``https``.
    """

    def __init__(self, server_path: str) -> None:
        """
        Args:
            server_path: Path to the server, e.g. https://eosuser.cern.ch/
        """
        super().__init__(server_path)

        options = {
            "webdav_hostname": self.server_path,
        }
        self.client = Client(options)

        # Get the X509 proxy path from settings or environment variables
        x509_proxy = get_setting("X509_USER_PROXY", os.environ.get("X509_USER_PROXY", _no_value))
        if x509_proxy is _no_value:
            raise ValueError("X509_USER_PROXY is not set in environment variables or settings.")

        self.client.session.cert = x509_proxy

        # Get the X509 certificate directory from settings or environment variables
        x509_cert_dir = get_setting("X509_CERT_DIR", os.environ.get("X509_CERT_DIR", _no_value))
        if x509_cert_dir is _no_value:
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

        Args:
            local_path (str): The path to the file on the local file system.
            remote_path (str): The destination path for the file on the remote file system.
            force (bool, optional): If True, overwrites the file on the remote system if it
                already exists. Defaults to False.
        """
        if force and self.client.check(remote_path):
            self.client.clean(remote_path=remote_path)
        self.client.upload_sync(remote_path=remote_path, local_path=local_path)

    def copy_file_from_remote(self, remote_path: str, local_path: str, force: bool = False) -> None:
        """
        This method uses the client to perform the file transfer.

        Args:
            remote_path (str): The path to the file on the remote file system.
            local_path (str): The destination path for the file on the local file system.
            force (bool, optional): If True, overwrites the file on the local system if it
                already exists. Defaults to False.
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

        Args:
            source_path (str): The current path of the file on the remote file system.
            dest_path (str): The target path for the file on the remote file system.
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
        This method queries the remote file system to locate the specified file.

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

        Args:
            path (str): The path to the file on the remote file system.
        """
        self.client.clean(remote_path=path)

    def listdir(self, path: str, print_entries: bool = False) -> Union[Tuple[Dict[str, int], Any], List[str]]:
        """
        Lists the contents of a directory on the remote file system.

        This method retrieves the directory listing and categorizes entries as files or directories.

        Args:
            path (str): Path to the directory on the remote file system.
            print_entries (bool, optional): Unused

        Returns:
            List[str]: A list of paths to the entries in the directory.
        """
        return self.client.list(path)

    def remove_dir(self, path: str) -> None:
        """
        A function to iteratively remove a directory and all its content from the remote file system.

        Args:
            path (str): Path to the directory on the remote file system.
        """
        self.remove(path=path)


class WebDAVTarget(RemoteTarget):
    """
    WebDAV target for ``b2luigi``. It has the same interface as the :obj:`RemoteTarget`.
    The use of the class is mainly to not brake the naming scheme of the first implementation of remote targets.
    """

    pass
