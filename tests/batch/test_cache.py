"""
Tests for :obj:`b2luigi.batch.cache.BatchJobStatusCache`.
"""

import time
import unittest

import b2luigi
from b2luigi.batch.cache import DEFAULT_BATCH_STATUS_CACHE_TTL, BatchJobStatusCache


class MockJobStatusCache(BatchJobStatusCache):
    """Cache whose batch system reports every job in ``known_jobs`` as running."""

    def __init__(self, known_jobs=()):
        super().__init__()
        self.known_jobs = known_jobs

    def _ask_for_job_status(self, job_id=None):
        for known_job in self.known_jobs:
            self[known_job] = "running"


class TestBatchStatusCacheTTL(unittest.TestCase):
    def tearDown(self):
        b2luigi.clear_setting("batch_status_cache_ttl")

    def test_default_ttl(self):
        cache = MockJobStatusCache()
        self.assertEqual(cache._expiry_time(1, "status", now=0), DEFAULT_BATCH_STATUS_CACHE_TTL)

    def test_ttl_setting_applies_to_seeded_statuses(self):
        # the cache already exists before the setting is changed, like the module-level caches do
        cache = MockJobStatusCache()
        b2luigi.set_setting("batch_status_cache_ttl", 0.2)
        cache.seed_submitted([1, 2], "pending")

        self.assertEqual(cache[1], "pending")
        self.assertEqual(cache[2], "pending")
        time.sleep(0.3)
        self.assertNotIn(1, cache)
        with self.assertRaises(KeyError):
            cache[1]

    def test_ttl_setting_applies_to_queried_statuses(self):
        """Batch systems that never seed must pick up the setting when they query the batch system."""
        cache = MockJobStatusCache(known_jobs=[1])
        b2luigi.set_setting("batch_status_cache_ttl", 0.2)

        self.assertEqual(cache[1], "running")  # cache miss, queries the batch system
        self.assertEqual(cache._ttl, 0.2)
        self.assertIn(1, cache)
        time.sleep(0.3)
        self.assertNotIn(1, cache)

    def test_invalid_ttl_is_rejected(self):
        cache = MockJobStatusCache()
        for invalid_ttl in (0, -5, "60", True):
            with self.subTest(ttl=invalid_ttl):
                b2luigi.set_setting("batch_status_cache_ttl", invalid_ttl)
                with self.assertRaises(ValueError):
                    cache.seed_submitted([1], "pending")
