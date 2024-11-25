# ----------------------------------------------------------------------------
# Starter Kit: b2luigi (B2GM 2024)
# Authors: Alexander Heidelbach, Jonas Eppelt, Giacomo De Pietro
#
# Scope: This example demonstrates how to submit a basf2 analysis task via
# gbasf2. Here, we do not rely on the self simulated and reconstructed data
# but use a MC dataset that is avaible on the grid as input. In addition, this
# exercise explains every setting that can be set for a Basf2PathTask that is
# submitted via gbasf2.
#
# Caution: The chosen dataset is only an example and might not produce exact
# results.
#
# ----------------------------------------------------------------------------

import basf2 as b2
import modularAnalysis as ma
import variables.utils as vu
import vertex as vx


import b2luigi
from b2luigi.basf2_helper import Basf2PathTask


# To submit jobs via gbasf2, the user will need to have a valid grid
# certificate. In principle, b2luigi will execute the gbasf2 setup with
# `source /cvmfs/belle.kek.jp/grid/gbasf2/pro/bashrc` which will also
# initialize the proxy. However, it is recommended to have an active proxy
# before submitting the job since otherwise the proxy password will be
# required for each submitting project.
class AnalysisTask(Basf2PathTask):
    # gbasf2 project submission
    # The gbasf2 batch process takes the basf2 path returned by the
    # `create_path()` method of the task, saves it into a pickle file to the
    # disk and creates a wrapper steering file that executes the saved path.
    # Any basf2 variable aliases added in the `Path()` or `create_path()`
    # method are also stored in the pickle file. It then sends both the pickle
    # file and the steering file wrapper to the grid via the Belle II-specific
    # DIRAC-wrapper gbasf2.
    #
    # Project status monitoring
    # After the project submission, the gbasf batch process regularly checks
    # the status of all the jobs belonging to a gbasf2 project returns a
    # success if all jobs had been successful, while a single failed job
    # results in a failed project. You can close a running b2luigi process and
    # then start your script again and if a task with the same project name is
    # running, this b2luigi gbasf2 wrapper will recognize that and instead of
    # resubmitting a new project, continue monitoring the running project.
    #
    # Automatic download of datasets and logs
    # If all jobs had been successful, it automatically downloads the output
    # dataset and the log files from the job sandboxes and automatically
    # checks if the download was successful before moving the data to the
    # final location. On failure, it only downloads the logs.
    #
    # Automatic rescheduling of failed jobs
    # Whenever a job fails, gbasf2 reschedules it as long as the number of
    # retries is below the value of the setting gbasf2_max_retries. It keeps
    # track of the number of retries in a local file in the log_file_dir, so
    # that it does not change if you close b2luigi and start it again. Of
    # course it does not persist if you remove that file or move to a
    # different machine.

    treeFit = b2luigi.BoolParameter(default=True)
    event_type = b2luigi.Parameter(default="mumu")
    result_dir = "results/AnalysisGbasf2"

    batch_system = "gbasf2"
    # Required settings for gbasf2
    # - gbasf2_input_dataset: String with the logical path of a dataset on the
    # grid to use as an input to the task. You can provide multiple inputs by
    # having multiple paths contained in this string, separated by commas
    # without spaces. An alternative is to just instantiate multiple tasks
    # with different input datasets, if you want to know in retrospect which
    # input dataset had been used for the production of a specific output.
    #
    # - gbasf2_input_dslist: Alternatively to gbasf2_input_dataset, you can
    # use this setting to provide a text file containing the logical grid path
    # names, one per line.
    #
    # - gbasf2_project_name_prefix: A string with which your gbasf2 project
    # names will start. To ensure the project associate with each unique task
    # (i.e. for each of luigi parameters) is unique, the unique task.task_id
    # is hashed and appended to the prefix to create the actual gbasf2 project
    # name. Should be below 22 characters so that the project name with the
    # hash can remain under 32 characters.

    @property
    def gbasf2_input_dslist(self):
        return "charged.txt"

    @property
    def gbasf2_project_name_prefix(self):
        # Keep projectname shorter than grid requirement
        projectname = "Gbasf2TaskFOO3"
        return projectname[-21:]

    # Optional settings for gbasf2
    #
    # - gbasf2_setup_path: Path to gbasf2 environment setup script that needs
    # so be sourced to run gbasf2 commands. Defaults to
    # /cvmfs/belle.kek.jp/grid/gbasf2/pro/bashrc.
    #
    # - gbasf2_release: Defaults to the release of your currently set up basf2
    # release. Set this if you want the jobs to use another release on the
    # grid.
    #
    # - gbasf2_proxy_lifetime: Defaults to 24. When initializing a proxy, set
    # the lifetime to this number of hours.
    #
    # - gbasf2_min_proxy_lifetime: Defaults to 0. During processing, prompt
    # user to reinitialize proxy if remaining proxy lifetime drops below this
    # number of hours.
    # - gbasf2_print_status_updates: Defaults to True. By setting it to False
    # you can turn off the printing of of the job summaries, that is the
    # number of jobs in different states in a gbasf2 project.
    #
    # - gbasf2_max_retries: Default to 0. Maximum number of times that each
    # job in the project can be automatically rescheduled until the project
    # is declared as failed.
    #
    # - gbasf2_proxy_group: Default to "belle". If provided, the gbasf2
    # wrapper will work with the custom gbasf2 group, specified in this
    # parameter. No need to specify this parameter in case of usual
    # physics analysis at Belle II. If specified, one has to provide
    # gbasf2_project_lpn_path parameter.
    #
    # - gbasf2_project_lpn_path: Path to the LPN folder for a specified gbasf2
    # group. The parameter has no effect unless the gbasf2_proxy_group is used
    # with non-default value.
    #
    # - gbasf2_download_dataset: Defaults to True. Disable this setting if you
    # don’t want to download the output dataset from the grid on job success.
    # As you can’t use the downloaded dataset as an output target for luigi,
    # you should then use the provided Gbasf2GridProjectTarget.
    #
    # - gbasf2_download_logs: Whether to automatically download the log output
    # of gbasf2 projects when the task succeeds or fails.
    #
    # The following optional settings correspond to the equally named gbasf
    # command line options (without the gbasf_ prefix) that you can set to
    # customize your gbasf2 project:
    # gbasf2_noscout, gbasf2_additional_files, gbasf2_input_datafiles,
    # gbasf2_n_repition_job, gbasf2_force_submission, gbasf2_cputime,
    # gbasf2_evtpersec, gbasf2_priority, gbasf2_jobtype, gbasf2_basf2opt
    #
    # It is further possible to append arbitrary command line arguments to the
    # gbasf2 submission command with the gbasf2_additional_params setting. If
    # you want to blacklist a grid site, you can e.g. add
    # ````
    # b2luigi.set_setting(
    #       "gbasf2_additional_params",  "--banned_site LCG.KEK.jp"
    # )
    # ```

    @property
    def gbasf2_release(self):
        return "light-2409-toyger"

    @property
    def gbasf2_proxy_group(self):
        return "belle_neutrals"

    @property
    def gbasf2_project_lpn_path(self):
        return "/belle/group/performance/neutrals/PERC"

    @property
    def gbasf2_download_logs(self):
        return False

    # Output format: Changing the batch to gbasf2 means you also have to adapt
    # how you handle the output of your gbasf2 task in tasks depending on it,
    # because the output will not be a single root file anymore, but a
    # collection of root files, one for each file in the input data set, in a
    # directory with the base name of the root files
    def output(self):
        return {"ntuple_gbasf2.root": b2luigi.LocalTarget("ntuple_gbasf2.root")}

    # The gbasf2 batch process for luigi can only be used for tasks inhereting
    # from Basf2PathTask or other tasks with a create_path() method that returns
    # a basf2 path.
    def create_path(self):
        main = b2.Path()

        particle = "mu" if str(self.event_type) == "mumu" else "e"

        ma.inputMdstList(
            environmentType="default",
            # For gbasf2 submission, provide the input dataset as an empty
            # list since this will be automatically filled by
            # gbasf2_input_dataset.
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
    # b2luigi.set_setting("gbasf2_proxy_group", "belle_neutrals")
    # b2luigi.set_setting("gbasf2_project_lpn_path", "/belle/group/performance/neutrals/PERC")
    b2luigi.process(AnalysisTask(), batch=True)
