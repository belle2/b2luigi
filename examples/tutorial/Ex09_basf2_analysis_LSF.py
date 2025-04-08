"""
.. _exercise09_label:

Submitting a task to the LSF batch system
=========================================

.. hint::
    This example demonstrates how to submit a task to the LSF batch
    system. In it's core, the task is the same as in :ref:`exercise07_label`.
    The key difference is the ``batch_system`` parameter that is set to ``"lsf"``
    and additional settings that are used to steer the submission to LSF.

To define the batch system on which this task should run, the only
change needed is to set the ``batch_system`` parameter to the desired
batch system. The available options are:

- ``"local"`` for no batch submission

- ``"lsf"`` for LSF (see the `LSF documentation <https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=overview-lsf-introduction>`_)

- ``"htcondor"`` for HTCondor (see the `HTCondor documentation <https://htcondor.readthedocs.io/en/latest/users-manual/quick-start-guide.html>`_)

- ``"slurm"`` for Slurm (see the `Slurm documentation <https://slurm.schedmd.com/quickstart.html>`_)

- ``"gbasf2"`` for the client to submit grid-based Belle II jobs (see the `gbasf2 documentation <https://gbasf2.belle2.org/>`_)

In ``b2luigi`` there are some dedicated parameters for each supported batch system,
see :ref:`batch-label`.
For LSF, these parameters are:

- ``queue``: the queue to submit the job to (default: ``"s"``)

- ``job_name``: the name of the job to be displayed in the queue
"""

import basf2 as b2
import modularAnalysis as ma
import variables.utils as vu
import vertex as vx


import b2luigi
from b2luigi.basf2_helper import Basf2PathTask
from Ex06_basf2_reconstruction import ReconstructionTask


@b2luigi.requires(ReconstructionTask)
class AnalysisTask(Basf2PathTask):
    treeFit = b2luigi.BoolParameter(default=True)
    result_dir = "results/AnalysisLSF"

    batch_system = "lsf"
    queue = "s"

    def output(self):
        yield self.add_to_output(f"ntuple_LSF_{self.identifier}.root")

    def create_path(self):
        main = b2.Path()

        particle = "mu" if str(self.event_type) == "mumu" else "e"

        ma.inputMdstList(
            environmentType="default", filelist=self.get_input_file_names("reconstruction.root"), path=main
        )

        # Fill final state particles
        ma.fillParticleList(decayString=f"{particle}+:fsp", cut="[abs(dr) < 2.0] and [abs(dz) < 4.0]", path=main)

        ma.fillParticleList(
            decayString="K+:fsp", cut="[abs(dr) < 2.0] and [abs(dz) < 4.0] and [kaonID > 0.2]", path=main
        )

        # reconstruct J/psi -> l+ --
        ma.reconstructDecay(
            decayString=f"J/psi:ll -> {particle}+:fsp {particle}-:fsp", cut="[2.92 < M < 3.22]", path=main
        )

        # reconstruct B+ -> J/psi K+
        ma.reconstructDecay(
            decayString="B+:jpsik -> J/psi:ll K+:fsp", cut="[Mbc > 5.24] and [abs(deltaE) < 0.2]", path=main
        )

        # Run truth matching
        ma.matchMCTruth(list_name="B+:jpsik", path=main)

        # Perform tree fit if requested
        if self.treeFit:
            vx.treeFit(list_name="B+:jpsik", conf_level=0.00, massConstraint=["J/psi"], path=main)

        # Define variables to save
        variables = ["Mbc", "deltaE", "chiProb", "isSignal"]
        variables += vu.create_aliases_for_selected(
            list_of_variables=["InvM", "M"], decay_string="B+:jpsik -> ^J/psi:ll K+:fsp", prefix="jpsi"
        )

        # Write variables into ntuple
        ma.variablesToNtuple(
            decayString="B+:jpsik",
            variables=variables,
            filename=self.get_output_file_name(f"ntuple_LSF_{self.identifier}.root"),
            treename="Bp",
            path=main,
            storeEventType=False,
        )

        return main


# %%
# To execute tasks in batch mode, use the ``batch`` argument of the
# ``process`` method.


# %%
if __name__ == "__main__":
    b2luigi.process(AnalysisTask(n_events=10), batch=True)


# %%
# Either pass ``batch=True`` as shown above or use the
# CLI option ``--batch`` when executing the script, e.g.:
#
# .. code-block:: bash
#
#   python3 Ex09_basf2_analysis_LSF.py --batch
