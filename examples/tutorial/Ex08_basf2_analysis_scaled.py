"""
.. _exercise08_label:

Scaling up your analysis
========================

.. hint::
    This example demonstrates how to locally scale the analysis task from
    the previous example :ref:`exercise07_label`. This example is very similar
    to the basic ``luigi`` wrapper task example in :ref:`exercise03_label`. Additionally, we
    introduce the ``inherits`` method to inherit parameters from another task
    without requiring the task.
"""

import b2luigi
from Ex07_basf2_analysis import AnalysisTask


# %%
# The ``inherits`` decorater works similar to the already known ``requires``
# decorator. It will also propagate the parameters of the inherited task to
# the inheriting task. The difference is that the inheriting task does not
# require the inherited task. It can e.g. be used in tasks that merge the
# output of the tasks they require. These merger tasks don't need the
# parameter they resolve anymore but should keep the same order of parameters,
# therefore simplifying the directory


# %%
@b2luigi.inherits(AnalysisTask, without="identifier")
class AnalysisWrapperTask(b2luigi.WrapperTask):
    n_loops = b2luigi.IntParameter(default=1)

    def requires(self):
        for i in range(self.n_loops):
            yield AnalysisTask(n_events=self.n_events, identifier=f"loop_{i}")


if __name__ == "__main__":
    b2luigi.process(AnalysisWrapperTask(n_events=10, n_loops=5), workers=5)
