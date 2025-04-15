"""
.. _exercise10_label:

Submitting a task to the HTCondor batch system
==============================================

.. hint::
    This example demonstrates how to submit a task to the HTCondor batch
    system. In in it's core, the task is the same as in :ref:`exercise07_label`.
    The key difference is the ``batch_system`` parameter that is set to ``"htcondor"``
    and additional settings that are used to steer the submission to htcondor.

HTCondor specific settings that can be automatically provided to a
htcondor task are:

- ``htcondor_settings``:
  A dictionary with the settings for the HTCondor job.
  The specific settings need to be supported by the
  HTCondor system. At NAF, some settings are already
  predefined and can be used directly.
- ``env_script``:
  The path to the environment script that should be sourced.
  This script will be ``source``'d before the execution of the
  task on the host machine.
- ``executable``:
  The executable that should be run. This needs to be a list.
- ``transfer_files``:
  HTCondor supports copying files from submission to
  workers. This means if the folder of your
  script(s)/python project/etc. is not accessible on the
  worker, you can copy it from the submission machine by
  adding it to the setting ``transfer_files``. This list can
  host both folders and files. Please note that due to
  HTCondors file transfer mechanism, all specified
  folders and files will be copied into the worker node
  flattened, so if you specify `a/b/c.txt` you will end up
  with a file `c.txt`. If you use the ``transfer_files``
  mechanism, you need to set the ``working_dir`` setting
  to ``“.”`` as the files will end up in the current worker
  scratch folder. All specified files/folders should be
  absolute paths.
"""

import os
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
    result_dir = "results/AnalysisHTCondor"

    batch_system = "htcondor"

    @property
    def htcondor_settings(self):
        return {
            "request_cpus": "1",
            "accounting_group": "belle",
            "universe": "docker",
            "docker_image": "giacomoxt/belle2-base-el9",
            "stream_output": "true",
            "stream_error": "true",
            "requirements": "(TARGET.ProvidesCPU == True) && " + "(TARGET.ProvidesEKPResources == True)",
            "request_memory": "2048",
        }

    @property
    def env_script(self):
        return "setup.sh"

    @property
    def executable(self):
        # Here we make use of the MY_PYTHON_PATH environment variable that is
        # set in the setup.sh script.
        return [os.environ.get("MY_PYTHON_PATH", "python3")]

    def output(self):
        yield self.add_to_output(f"ntuple_htcondor_{self.identifier}.root")

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
            filename=self.get_output_file_name(f"ntuple_htcondor_{self.identifier}.root"),
            treename="Bp",
            path=main,
            storeEventType=False,
        )

        return main


if __name__ == "__main__":
    b2luigi.process(AnalysisTask(n_events=10), batch=True)
