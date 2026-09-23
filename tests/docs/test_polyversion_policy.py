import importlib.util
import pathlib
import unittest

_POLICY = pathlib.Path(__file__).resolve().parents[2] / "docs" / "_polyversion" / "policy.py"
_spec = importlib.util.spec_from_file_location("policy", _POLICY)
policy = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(policy)


class TestRetainedTags(unittest.TestCase):
    def test_keeps_newest_and_pinned_tags_only(self):
        tags = ["v1.2.7", "v1.2.8", "v1.2.9", "v1.3.0", "v1.10.0", "v0.9.0"]
        self.assertEqual(policy.retained_tags(tags), ["v1.10.0", "v1.3.0", "v1.2.9"])

    def test_pinned_tag_is_not_duplicated_when_among_newest(self):
        self.assertEqual(policy.retained_tags(["v1.2.8", "v1.2.9"]), ["v1.2.9", "v1.2.8"])

    def test_ignores_non_release_tags(self):
        self.assertEqual(policy.retained_tags(["v1.3.0rc1", "build-2026", "v1.3.0"]), ["v1.3.0"])

    def test_sorts_numerically_not_lexically(self):
        self.assertEqual(policy.retained_tags(["v1.9.0", "v1.10.0", "v1.2.9"]), ["v1.10.0", "v1.9.0", "v1.2.9"])


class TestNames(unittest.TestCase):
    def test_development_branch_is_published_as_dev(self):
        self.assertEqual(policy.published_name("main"), "dev")
        self.assertEqual(policy.published_name("v1.3.0"), "v1.3.0")

    def test_newest_release(self):
        self.assertEqual(policy.newest_release(["dev", "v1.2.9", "v1.3.0"]), "v1.3.0")
        self.assertIsNone(policy.newest_release(["dev"]))


class TestSwitcherEntries(unittest.TestCase):
    def test_newest_release_is_preferred_and_served_from_root(self):
        entries = policy.switcher_entries(["dev", "v1.2.9", "v1.3.0"], "https://example.org/")
        self.assertEqual([e["version"] for e in entries], ["v1.3.0", "v1.2.9", "dev"])
        self.assertEqual(entries[0]["name"], "v1.3.0 (stable)")
        self.assertEqual(entries[0]["url"], "https://example.org/")
        self.assertTrue(entries[0]["preferred"])
        self.assertEqual(entries[1]["name"], "v1.2.9 (last Python 3.8)")
        self.assertEqual(entries[1]["url"], "https://example.org/v1.2.9/")
        self.assertEqual(entries[2]["name"], "dev (unreleased)")
        self.assertNotIn("preferred", entries[2])

    def test_local_build_only_lists_dev(self):
        entries = policy.switcher_entries(["dev"])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["url"], "https://b2luigi.belle2.org/dev/")
