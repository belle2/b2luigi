import unittest
import warnings

import luigi

import b2luigi


class TestBuildWarnsAboutProcess(unittest.TestCase):
    """``b2luigi.build`` must not silently alias ``luigi.build``."""

    def test_b2luigi_build_is_not_luigi_build(self):
        self.assertIsNot(b2luigi.build, luigi.build)

    def test_build_warns_to_use_process(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            b2luigi.build([], local_scheduler=True)

        messages = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        self.assertTrue(
            any("b2luigi.process" in m for m in messages),
            f"expected a UserWarning pointing at b2luigi.process, got {messages!r}",
        )

    def test_build_warning_points_at_caller(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            b2luigi.build([], local_scheduler=True)

        hits = [w for w in caught if "b2luigi.process" in str(w.message)]
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].filename, __file__)
