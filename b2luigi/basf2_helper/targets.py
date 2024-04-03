import b2luigi

from luigi.target import FileSystemTarget, FileSystem
import os
from pathlib import Path
from contextlib import contextmanager
import logging

from XRootD import client 
from XRootD.client.flags import DirListFlags, OpenFlags, MkDirFlags

class ROOTLocalTarget(b2luigi.LocalTarget):
    def exists(self):
        if not super().exists():
            return False

        path = self.path

        import ROOT

        tfile = ROOT.TFile.Open(path)
        return tfile and len(tfile.GetListOfKeys()) > 0



class XrootDSystem(FileSystem):
    '''
    XrootDFileSystem for b2luigi Targets. Inspiration taken from rhofsaess https://github.com/RHofsaess/xrd-interactive/blob/main/xrootd_utils.py
    '''

    def __init__(self, server_path: str, user: str = None) -> None:
        self.server_path = server_path
        self.client = client.FileSystem(self.server_path)
        self.user = user #small security feature
        if self.user is None:
            logging.warn("No user set. Some features might not work correctly")

    def exists(self, path: str) -> bool:
        '''
        Implementation of the exists function for the XrootDSystem.
        '''
        
        status, _ = self.client.stat(path, DirListFlags.STAT)
        if not status.ok:
            return False
        else:
            return True

    def copy_file_to_remote(self, local_path: str, remote_path:str , force = False) -> None:
        status, _ = self.client.copy('file://' + local_path, self.server_path + remote_path, force = force)
        if not status.ok:
            logging.warn(status.message)
        assert status.ok

    def copy_file_from_remote(self, remote_path: str, local_path: str, force = False) -> None:
        status, _ = self.client.copy(self.server_path +"//"+ remote_path, local_path, force = force)
        if not status.ok:
            logging.warn(status.message)
            if "file exists" in status.message:
                logging.warn(f"File already exists: {local_path}")
                status.ok = True
        assert status.ok

    def copy_dir_from_remote(self, remote_path: str, local_path: str, force = False) -> None:
        _, l = self.listdir(remote_path)
        for file in l:
            if file.statinfo.size != 512: # adhoc test, if the `file` is not a directory
                logging.warn(file.name)
                self.copy_file_from_remote(remote_path + "/" + file.name, local_path + file.name, force = force)
    
    def move(self, source_path: str, dest_path: str) -> None:
        status, _ = self.client.mv(source_path, dest_path)
        if not status.ok:
            logging.warn(status.message)
        assert status.ok

    def mkdir(self, path: str) -> None:
        dir_path, file_path = os.path.split(path)
        if self.exists(dir_path):
            logging.warn(f"dir already exists: {dir_path}")
            return
        status, _ = self.client.mkdir(dir_path, MkDirFlags.MAKEPATH)
        if not status.ok:
            logging.warn(status.message, path)
            if "File exists" in status.message:
                status.ok = True
        assert status.ok

    def locate(self, path: str) -> None:
        status, locations = self.client.locate(path, OpenFlags.REFRESH)
        if not status.ok:
            logging.warn(status.message)
        assert status.ok
        return True

    def remove(self, path: str) -> None:
        status, _ = self.client.rm(path)
        if not status.ok:
            logging.warn(status.message)
        assert status.ok

    def listdir(self, path) -> None:
        dir_dict = {}
        status, listing = self.client.dirlist(path, DirListFlags.STAT)
        if not status.ok:
            logging.warn(f'[get_directory_listing] Status: {status.message}')
        assert status.ok  # directory or redirector faulty

        for entry in listing:
            # faster way to check if file or dir: less DDOS with only one query
            if entry.statinfo.flags == 51 or entry.statinfo.flags == 19:
                # directories have a size of 512
                assert (entry.statinfo.size == 512)  # just to make sure for the recursive stuff
                dir_dict[f"{listing.parent + entry.name}/"] = 1
            elif entry.statinfo.flags == 48 or entry.statinfo.flags == 16:
                dir_dict[f"{listing.parent + entry.name}"] = 0
            else:
                logging.warn(f'[get_directory_listing] Info: {entry}')
                exit("Unknown flags. RO files, strange permissions?")
            logging.warn(entry.name, f"{entry.statinfo.size/1024/1024} MB")
        return dir_dict, listing
        
    def remove_dir(self, path)-> None:
        '''
        iteratively remove a directory and its content
        '''
        if not self.user in path:
            logging.warn('Permission denied. Your username was not found in the directory path!')
            exit(-1)
        status, listing = self.client.dirlist(path, DirListFlags.STAT)
        if not status.ok:
            logging.warn(f'Status: {status.message}')
        assert status.ok  # directory does not exists
        for file in listing:  # unfortunately, there is no recursive way in xrd...
            logging.info(f'{path}{listing.parent}{file.name}')
            if file.statinfo.size == 512:  # check if "file" is a directory -> delete recursively
                logging.warn(f'[rm dir] list entry: {file}')
                assert (file.statinfo.flags == 51 or file.statinfo.flags == 19)  # make sure it is a directory; evtl wrong permissions?
                self.remove_dir(listing.parent + file.name)
            else:
                self.remove(listing.parent + file.name)
        status, _ = self.client.rmdir(path)  # when empty, remove empty dir
        if not status.ok:
            logging.info(f'Status: {status.message}')
        assert status.ok  # dir removal failed: check path or redirector
        
    def rename_dont_move(self, path: str, dest: str) -> None:
        self.copy_file_to_remote(path, dest, force = True)

class XrootDTarget(FileSystemTarget):

    def __init__(self, path: str, file_system: XrootDSystem, scratch_dir: str = "/tmp"):
        self._scratch_dir = scratch_dir
        self._file_system = file_system
        super().__init__(path)
    
    @property
    def base_name(self):
        return os.path.basename(self.path)

    @property
    def fs(self):
        return self._file_system
    
    def open(self, mode):
        raise NotImplementedError()
    
    def makedirs(self):
        self.fs.mkdir(self.path)
    
    def move(self):
        self.fs.move(self.path)
        
    def get(self, path: str = "~"):
        self.fs.copy_file_from_remote(self.path, f"{path}/{self.base_name}")
        return f"{path}/{self.base_name}"
    
    @contextmanager
    def temporary_path(self):
        tmp_path = f"{self._scratch_dir}/{self.base_name}"
        yield tmp_path
        self.fs.copy_file_to_remote(tmp_path, self.path, force = True)