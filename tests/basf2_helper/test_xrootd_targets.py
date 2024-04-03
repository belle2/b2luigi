import unittest
from unittest.mock import MagicMock
from b2luigi.basf2_helper.xrootd_targets import XrootDTarget, XrootDSystem


class TestXrootDTarget(unittest.TestCase):
    def setUp(self):
        self.file_system = XrootDSystem("server_path")
        self.target = XrootDTarget("path", self.file_system)

    def test_base_name(self):
        self.assertEqual(self.target.base_name, "path")

    def test_fs(self):
        self.assertEqual(self.target.fs, self.file_system)

    def test_open(self):
        # Mock the open method of XrootDSystem
        self.file_system.open = MagicMock(return_value="file_object")

        # Call the open method of XrootDTarget
        file_object = self.target.open("r")

        self.assertEqual(file_object, "file_object")
        self.file_system.open.assert_called_once_with("path", "r")

    def test_makedirs(self):
        # Mock the mkdir method of XrootDSystem
        self.file_system.mkdir = MagicMock()

        # Call the makedirs method of XrootDTarget
        self.target.makedirs()

        self.file_system.mkdir.assert_called_once_with("path")

    def test_move(self):
        # Mock the move method of XrootDSystem
        self.file_system.move = MagicMock()

        # Call the move method of XrootDTarget
        self.target.move("dest_path")

        self.file_system.move.assert_called_once_with("path", "dest_path")

    def test_get(self):
        # Mock the copy_file_from_remote method of XrootDSystem
        self.file_system.copy_file_from_remote = MagicMock()

        # Call the get method of XrootDTarget
        self.target.get("remote_path")

        self.file_system.copy_file_from_remote.assert_called_once_with("remote_path", "path")

    def test_temporary_path(self):
        # Mock the temporary_path method of XrootDSystem
        self.file_system.temporary_path = MagicMock(return_value="temp_path")

        # Use the temporary_path method of XrootDTarget as a context manager
        with self.target.temporary_path() as temp_path:
            self.assertEqual(temp_path, "temp_path")

        self.file_system.temporary_path.assert_called_once()
