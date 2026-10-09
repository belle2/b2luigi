import warnings

warnings.warn(
    "b2luigi.basf2_helper.targets has been moved to b2luigi.contrib.basf2.targets.\n"
    "In a future release, this move will be finalized and old imports from b2luigi.basf2_helper.targets will no longer work."
)

from b2luigi.contrib.basf2.targets import *
