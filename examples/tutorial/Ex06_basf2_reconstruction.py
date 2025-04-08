"""
.. _exercise06_label:

The ``requires`` decorator
==========================

.. hint::
    This example demonstrates how to create a simple ``b2luigi`` task that
    executes the reconstruction of previously simulated events. The key point of
    this example is the ``requires`` decorator.

We start with the usual imports. Note that we import the task ``SimulationTask``
class defined in the previous example :ref:`exercise05_label`: it's generally recommended
to keep your code as much modular as possible.
"""

import basf2 as b2
import reconstruction as re
import mdst

import b2luigi
from b2luigi.basf2_helper import Basf2PathTask
from Ex05_basf2_simulation import SimulationTask


# %%
# Here me make use of the ``requires`` decorator to require the ``SimulationTask``.
# This functionality is very useful when the dependencies of tasks are very
# easily defined. The task with the ``requires`` decorator will automatically
# create the ``requires`` method that returns the required task. Additionally,
# the task also inherits all the parameters of the required task.


# %%
@b2luigi.requires(SimulationTask)
class ReconstructionTask(Basf2PathTask):
    result_dir = "results/Reconstruction"
    batch_system = "local"

    def output(self):
        yield self.add_to_output("reconstruction.root")

    def create_path(self):
        main = b2.Path()

        # Read the simulated events that were created in the simulation task.
        main.add_module("RootInput", inputFileNames=self.get_input_file_names("simulation.root"))

        # Run reconstruction
        main.add_module("Gearbox")
        main.add_module("Geometry")
        re.add_reconstruction(main)

        # Add MDST output.
        mdst.add_mdst_output(path=main, filename=self.get_output_file_name("reconstruction.root"))
        return main


# %%
# As already pointed out in the second example, here we only need to
# call the ``ReconstructionTask``: the scheduler will resolves all the
# required dependencies.
if __name__ == "__main__":
    b2luigi.process(ReconstructionTask(n_events=10))
