"""
.. _exercise07_label:

Writing a ``basf2`` analysis steering file
===========================================

.. hint::
    This example shows a basic ``basf2`` analysis steering file within ``b2luigi``.
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
    result_dir = "results/Analysis"

    def output(self):
        yield self.add_to_output(f"ntuple_{self.identifier}.root")

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
            filename=self.get_output_file_name(f"ntuple_{self.identifier}.root"),
            treename="Bp",
            path=main,
            storeEventType=False,
        )

        return main


if __name__ == "__main__":
    b2luigi.process(AnalysisTask(n_events=10))
