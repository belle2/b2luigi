import os
import sys
import shutil
import warnings

from unittest import TestCase
from unittest.mock import patch

from b2luigi.basf2_helper.utils import get_basf2_git_hash


class TestGetBasf2GitHash(TestCase):
    @classmethod
    def setUpClass(cls):
        basf2_vars = shutil.which("basf2")
        cls.basf2_is_set = True if basf2_vars else False

    def test_return_no_set(self):
        """
        In the case basf2 is not installed or set up, so ``get_basf2_git_hash``
        should return \"not set\".
        """
        if not self.basf2_is_set:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=ImportWarning)
                self.assertEqual(get_basf2_git_hash(), "not_set")

        else:
            self.skipTest("basf2 is set, skipping test_return_no_set")

    def test_return_no_set_warning(self):
        """
        If basf2 is not installed or set up, so ``get_basf2_git_hash``
        should print an ``ImportWarning``.
        """
        if not self.basf2_is_set:
            with self.assertWarns(ImportWarning):
                get_basf2_git_hash()

        else:
            self.skipTest("basf2 is  set, skipping test_return_no_set_warning")

    def test_return_set(self):
        """
        If basf2 is installed and set up, so ``get_basf2_git_hash``
        should return the current basf2 release stored in the environment
        variale ``BELLE2_RELEASE``.
        """
        if self.basf2_is_set:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=ImportWarning)
                self.assertNotEqual(get_basf2_git_hash(), "not_set")

        else:
            self.skipTest("basf2 is not set, skipping test_return_set")

    @patch.dict(os.environ, {"BELLE2_RELEASE": "belle2_release_xy"})
    def test_belle2_release_env_is_set(self):
        self.assertEqual(get_basf2_git_hash(), "belle2_release_xy")

    @patch.dict(os.environ, {"BELLE2_RELEASE": "head"})
    def test_basf2_hash_from_get_version(self):
        """
        If basf2 is installed but a local version is used, ``get_basf2_git_hash``
        should detect basf2 and use the ``basf2.version.get_version()`` method
        to return the version hash.
        """

        class MockVersion:
            @staticmethod
            def get_version():
                return "some_basf2_version"

        class MockBasf2:
            version = MockVersion

        with patch.dict(sys.modules, {"basf2": MockBasf2, "basf2.version": MockVersion}):
            self.assertEqual(get_basf2_git_hash(), "some_basf2_version")
