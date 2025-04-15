import unittest
from unittest.mock import MagicMock
from b2luigi.core.xrootd_targets import XRootDSystem, XRootDTarget


def XRootD_available():
    try:
        import XRootD  # noqa

        return True
    except ModuleNotFoundError:
        return False


@unittest.skipIf(not XRootD_available(), "XRootD is not available. Skipping tests.")
class TestXRootDSystem(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_server_path: str = "root://mockserver.cern.ch/"
        self.XRootD_system: XRootDSystem = XRootDSystem(self.mock_server_path)
        self.XRootD_system.client = MagicMock()
        self.XRootD_system.copy_file_to_remote = MagicMock()

        self.XRootD_target: XRootDTarget = XRootDTarget(self.mock_server_path, self.XRootD_system)

    def test_exists_true(self) -> None:
        # Simulate a successful stat call
        self.XRootD_system.client.stat.return_value = (MagicMock(ok=True), None)
        self.assertTrue(self.XRootD_system.exists("/path/to/existing/file"))

    def test_exists_false(self) -> None:
        # Simulate a failed stat call
        self.XRootD_system.client.stat.return_value = (MagicMock(ok=False), None)
        self.assertFalse(self.XRootD_system.exists("/path/to/nonexistent/file"))

    def test_temporary_path(self) -> None:
        # Mock the copy_file_to_remote method
        with self.XRootD_target.temporary_path() as temp_path:
            # Assert that the temporary path is not empty
            self.assertIsNotNone(temp_path)

            # Assert that the temporary path starts with the scratch directory
            self.assertTrue(temp_path.startswith("/tmp"))


if __name__ == "__main__":
    unittest.main()
