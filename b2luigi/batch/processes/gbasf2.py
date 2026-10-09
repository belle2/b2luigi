import warnings

warnings.warn(
    "The gbasf2 batch process has been moved to b2luigi.contrib.gbasf2.process.\n"
    "In a future release, this move will be finalized and old imports from b2luigi.batch.processes.gbasf2 will no longer work."
)

from b2luigi.contrib.gbasf2.process import *  # noqa: E402, F401, F403

# Private helpers are not covered by the star import above
from b2luigi.contrib.gbasf2.process import (  # noqa: E402, F401
    _get_lfn_upto_reschedule_number,
    _move_downloaded_dataset_to_output_dir,
    _split_all_extensions,
)
