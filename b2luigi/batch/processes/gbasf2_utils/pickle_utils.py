import warnings

warnings.warn(
    "b2luigi.batch.processes.gbasf2_utils.pickle_utils has been moved to b2luigi.contrib.gbasf2.gbasf2_utils.pickle_utils.\n"
    "In a future release, this move will be finalized and old imports from b2luigi.batch.processes.gbasf2_utils.pickle_utils will no longer work."
)

from b2luigi.contrib.gbasf2.gbasf2_utils.pickle_utils import *  # noqa: E402, F401, F403
