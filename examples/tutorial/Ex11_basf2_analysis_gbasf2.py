"""
.. _exercise11_label:

``gbasf2`` Analysis Task
========================

.. hint::
    This example demonstrates how to submit a ``basf2`` analysis task via ``gbasf2``.
    Here, we do not rely on the self simulated and reconstructed data but use a MC dataset that is available on the grid as input.
    In addition, this exercise explains every setting that can be set for a :class:`Basf2PathTask <b2luigi.basf2_helper.tasks.Basf2PathTask>` that is submitted via ``gbasf2``.

To submit jobs via ``gbasf2``, the user will need to have a valid grid certificate.
In principle, ``b2luigi`` will execute the ``gbasf2`` setup with

.. code-block: bash

  source /cvmfs/belle.kek.jp/grid/gbasf2/pro/bashrc

which will also initialize the proxy.
However, it is recommended to have an active proxy before submitting the job since otherwise the proxy password will be required for each submitting project.

**gbasf2 Project Submission**

The ``gbasf2`` batch process takes the ``basf2`` path returned by the
:meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>` method of the task, saves it into a pickle file to the
disk and creates a wrapper steering file that executes the saved path.
Any ``basf2`` variable aliases added in the ``basf2.Path()`` or :meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>`
method are also stored in the pickle file. It then sends both the pickle
file and the steering file wrapper to the grid via the Belle II-specific
DIRAC-wrapper ``gbasf2``.

**Project Status Monitoring**

After the project submission, the ``gbasf`` batch process regularly checks
the status of all the jobs belonging to a ``gbasf2`` project returns a
success if all jobs had been successful, while a single failed job
results in a failed project. You can close a running ``b2luigi`` process and
then start your script again and if a task with the same project name is
running, this ``b2luigi`` ``gbasf2`` wrapper will recognize that and instead of
resubmitting a new project, continue monitoring the running project.

**Automatic Download of Datasets and Logs**

If all jobs had been successful, it automatically downloads the output
dataset and the log files from the job sandboxes and automatically
checks if the download was successful before moving the data to the
final location. On failure, it only downloads the logs.

**Automatic Rescheduling of Failed Jobs**

Whenever a job fails, ``gbasf2`` reschedules it as long as the number of
retries is below the value of the setting ``gbasf2_max_retries``. It keeps
track of the number of retries in a local file in the ``log_file_dir``, so
that it does not change if you close ``b2luigi`` and start it again. Of
course it does not persist if you remove that file or move to a
different machine.

**Required settings for gbasf2**

- ``gbasf2_input_dataset``:
  String with the logical path of a dataset on the
  grid to use as an input to the task. You can provide multiple inputs by
  having multiple paths contained in this string, separated by commas
  without spaces. An alternative is to just instantiate multiple tasks
  with different input datasets, if you want to know in retrospect which
  input dataset had been used for the production of a specific output.

- ``gbasf2_input_dslist``:
  Alternatively to ``gbasf2_input_dataset``, you can
  use this setting to provide a text file containing the logical grid path
  names, one per line.

- ``gbasf2_project_name_prefix``:
  A string with which your ``gbasf2`` project
  names will start. To ensure the project associate with each unique task
  (i.e. for each of luigi parameters) is unique, the unique ``b2luigi.Task.task_id``
  is hashed and appended to the prefix to create the actual ``gbasf2`` project
  name. Should be below 22 characters so that the project name with the
  hash can remain under 32 characters.

.. warning::
    The chosen dataset is only an example and might not produce exact results.
"""

import basf2 as b2
import modularAnalysis as ma
import variables.utils as vu
import vertex as vx


import b2luigi
from b2luigi.basf2_helper import Basf2PathTask


class AnalysisTask(Basf2PathTask):
    treeFit = b2luigi.BoolParameter(default=True)
    event_type = b2luigi.Parameter(default="mumu")
    result_dir = "results/AnalysisGbasf2"

    batch_system = "gbasf2"

    @property
    def gbasf2_input_dslist(self):
        return "charged.txt"

    @property
    def gbasf2_project_name_prefix(self):
        # Keep projectname shorter than grid requirement
        projectname = "Gbasf2Task"
        return projectname[-21:]

    @property
    def gbasf2_release(self):
        return "light-2409-toyger"

    @property
    def gbasf2_download_logs(self):
        return False

    # Don't use `add_to_output` here, but define the output explicitly.
    def output(self):
        return {"ntuple_gbasf2.root": b2luigi.LocalTarget("ntuple_gbasf2.root")}

    def create_path(self):
        main = b2.Path()

        particle = "mu" if str(self.event_type) == "mumu" else "e"

        ma.inputMdstList(
            environmentType="default",
            # Empty input file list, will be filled by the grid
            filelist=[],
            path=main,
        )

        # The rest of the steeringfile is identical to the local submission
        # with the difference of the correct outputfile name.

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
            filename="ntuple_gbasf2.root",
            treename="Bp",
            path=main,
            storeEventType=False,
        )

        return main


if __name__ == "__main__":
    b2luigi.process(AnalysisTask(), batch=True)

# %%
# **Optional but useful settings for gbasf2**
#
# - ``gbasf2_release``:
#   Defaults to the release of your currently set up ``basf2``
#   release. Set this if you want the jobs to use another release on the
#   grid.
# - ``gbasf2_max_retries``:
#   Default to ``0``. Maximum number of times that each
#   job in the project can be automatically rescheduled until the project
#   is declared as failed.
# - ``gbasf2_proxy_group``:
#   Default to ``"belle"``. If provided, the ``gbasf2``
#   wrapper will work with the custom ``gbasf2`` group, specified in this
#   parameter. No need to specify this parameter in case of usual
#   physics analysis at Belle II. If specified, one has to provide
#   ``gbasf2_project_lpn_path parameter``.
# - ``gbasf2_project_lpn_path``:
#   Path to the LPN folder for a specified gbasf2
#   group. The parameter has no effect unless the gbasf2_proxy_group is used
#   with non-default value.
#
# It is further possible to append arbitrary command line arguments to the
# gbasf2 submission command with the ``gbasf2_additional_params`` setting. If
# you want to blacklist a grid site, you can e.g. add
#
# .. code-block:: python
#
#   b2luigi.set_setting(
#         "gbasf2_additional_params",  "--banned_site LCG.KEK.jp"
#   )
#
# More settings can be found in the :class:`Gbasf2Process <b2luigi.batch.processes.gbasf2.Gbasf2Process>` section.
#
# .. hint::
#   Changing the batch to ``gbasf2`` means you also have to adapt
#   how you handle the output of your ``gbasf2`` task in tasks depending on it,
#   because the output will not be a single root file anymore, but a
#   collection of root files, one for each file in the input data set, in a
#   directory with the base name of the root files
#
# .. hint::
#   For ``gbasf2`` submission, provide the input dataset as an empty
#   list since this will be automatically filled by
#   ``gbasf2_input_dataset``.
