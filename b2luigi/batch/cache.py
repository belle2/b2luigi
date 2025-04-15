import abc
from cachetools import TTLCache


class BatchJobStatusCache(abc.ABC, TTLCache):
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
    """

    def __init__(self):
        super().__init__(maxsize=1000, ttl=20)

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
