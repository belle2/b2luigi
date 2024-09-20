import unittest
from unittest.mock import MagicMock
from b2luigi.core.xrootd_targets import XrootDSystem, XrootDTarget


class TestXrootDSystem(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_server_path: str = "root://mockserver.cern.ch/"
        self.xrootd_system: XrootDSystem = XrootDSystem(self.mock_server_path)
        self.xrootd_system.client = MagicMock()
        self.xrootd_system.copy_file_to_remote = MagicMock()

        self.xrootd_target: XrootDTarget = XrootDTarget(self.mock_server_path, self.xrootd_system, "/scratch")

    def test_exists_true(self) -> None:
        # Simulate a successful stat call
        self.xrootd_system.client.stat.return_value = (MagicMock(ok=True), None)
        self.assertTrue(self.xrootd_system.exists("/path/to/existing/file"))

    def test_exists_false(self) -> None:
        # Simulate a failed stat call
        self.xrootd_system.client.stat.return_value = (MagicMock(ok=False), None)
        self.assertFalse(self.xrootd_system.exists("/path/to/nonexistent/file"))

    def test_temporary_path(self) -> None:
        # Mock the copy_file_to_remote method
        with self.xrootd_target.temporary_path() as temp_path:
            # Assert that the temporary path is not empty
            self.assertIsNotNone(temp_path)

            # Assert that the temporary path starts with the scratch directory
            self.assertTrue(temp_path.startswith(self.xrootd_target._scratch_dir))


if __name__ == "__main__":
    unittest.main()
