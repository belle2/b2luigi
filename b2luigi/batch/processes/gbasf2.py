import warnings

warnings.warn(
    "The gbasf2 batch process has been moved to b2luigi.contrib.gbasf2.process. "
    "In a future release, this move will be finalized and old imports from b2luigi.batch.processes.gbasf2 will no longer work."
)

from b2luigi.contrib.gbasf2.process import *  # noqa: E402, F401, F403
