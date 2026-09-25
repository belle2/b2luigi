import abc
from functools import cached_property

from cachetools import TLRUCache

from b2luigi.core.settings import get_setting

DEFAULT_BATCH_STATUS_CACHE_TTL = 120


class BatchJobStatusCache(abc.ABC, TLRUCache):
    """
    Abstract base class for job status caches.
    Useful if the batch system provides the status of all jobs
    as a list, which might be faster than asking for each job
    separately.

    Override the function ``_ask_for_job_status``, which should
    set the job status for the specific job if
    specified or for all accessible jobs (e.g. for all of this user).
    Having too much information (e.g. information on jobs
    which are not started by this b2luigi instance) does not matter.

    Cached statuses expire after the number of seconds given by the ``batch_status_cache_ttl`` setting
    (default 120). The setting is read once, when the first status is cached.
    """

    def __init__(self):
        super().__init__(maxsize=100000, ttu=self._expiry_time)
        # List to store all the job_ids that are currently handled by running tasks
        self._job_ids = []

    @cached_property
    def _ttl(self):
        # Read on the first cached status, not in ``__init__``: the caches are created on import,
        # before the user can change any setting.
        ttl = get_setting("batch_status_cache_ttl", default=DEFAULT_BATCH_STATUS_CACHE_TTL)
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or ttl <= 0:
            raise ValueError(f"The setting batch_status_cache_ttl must be a positive number of seconds, got {ttl!r}")
        return ttl

    def _expiry_time(self, job_id, status, now):
        return now + self._ttl

    @abc.abstractmethod
    def _ask_for_job_status(self, job_id=None):
        """
        Abstract method to query the status of a job.

        This method must be implemented by subclasses to define how to
        retrieve the status of a job based on its unique identifier.

        Args:
            job_id (str, optional): The unique identifier of the job. Defaults to ``None``.
        """
        pass

    def seed_submitted(self, job_ids, status):
        """
        Cache ``status`` for freshly submitted jobs.

        Without this, the first status check of every new job misses the cache and queries the
        batch system for all jobs, i.e. one full query per replaced job once all worker slots are busy,
        instead of one query per cache TTL. The seeded status is replaced by the real one on the
        next query.

        Args:
            job_ids: The ids of the submitted jobs.
            status: The status to cache, which must count as running for the batch process
                (e.g. idle or pending).
        """
        for job_id in job_ids:
            self[job_id] = status

    def add_job_ids(self, job_ids):
        self._job_ids.append(job_ids)

    def remove_job_ids(self, job_ids):
        self._job_ids.remove(job_ids)

    def __missing__(self, job_id):
        """
        Handle missing keys in the cache.

        This method is called when a key (``job_id``) is not found in the cache.
        It attempts to retrieve the job status by first querying for all jobs
        and then specifically for the missing job. If the job is found during
        these queries, it is added to the cache and returned.

        Args:
            job_id (str): The identifier of the job to retrieve.

        Returns:
            The job status associated with the given ``job_id`` if found.

        Raises:
            KeyError: If the ``job_id`` cannot be found after querying.
        """
        # First, ask for all jobs
        self._ask_for_job_status(job_id=None)
        if job_id in self:
            return self[job_id]

        # Then, ask specifically for this job
        self._ask_for_job_status(job_id=job_id)
        if job_id in self:
            return self[job_id]

        raise KeyError
