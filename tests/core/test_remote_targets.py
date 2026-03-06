import unittest
from unittest.mock import MagicMock

from b2luigi.core.remote_target import RemoteFileSystem, RemoteTarget


class TestRemoteFileSystem(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_server_path: str = "https://mockserver.cern.ch/"
        self.system: RemoteFileSystem = RemoteFileSystem(self.mock_server_path)

    def test_not_implemented_exist(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.exists("")

    def test_not_implemented_copy_file_to_remote(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.copy_file_to_remote("", "")

    def test_not_implemented_copy_file_from_remote(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.copy_file_from_remote("", "")

    def test_not_implemented_copy_dir_from_remote(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.copy_dir_from_remote("", "")

    def test_not_implemented_move(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.move("", "")

    def test_not_implemented_mkdir(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.mkdir("")

    def test_not_implemented_locate(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.locate("")

    def test_not_implemented_remove(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.remove("")

    def test_not_implemented_listdir(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.listdir("")

    def test_not_implemented_remove_dir(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.remove_dir("")

    def test_not_implemented_rename_dont_move(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.system.rename_dont_move("", "")


class TestRemoteTarget(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_server_path: str = "https://mockserver.cern.ch/"
        self.mock_path: str = "foo/bar"
        self.system: RemoteFileSystem = MagicMock()
        self.target: RemoteTarget = RemoteTarget(self.mock_path, self.system)

    def test_base_name(self):
        self.assertEqual(self.target.base_name, "bar")

    def test_fs(self):
        self.assertEqual(self.target.fs, self.system)

    def test_makedirs(self):
        self.target.makedirs()
        self.system.mkdir.assert_called_once_with(self.mock_path)

    def test_get(self):
        output = self.target.get("zoo")
        self.system.copy_file_from_remote.assert_called_once_with(self.mock_path, "zoo/bar")
        self.assertEqual(output, "zoo/bar")

    def test_not_implemented_open(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.target.open("")

    def test_get_temporary_input(self):
        with self.target.get_temporary_input() as output:
            self.assertTrue(output.endswith("bar"))
            self.assertTrue(output.startswith("/tmp"))


if __name__ == "__main__":
    unittest.main()
