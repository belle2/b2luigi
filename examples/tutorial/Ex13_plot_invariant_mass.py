"""
.. _exercise13_label:

Common Analysis Workflow Continued
==================================

.. hint::
    This example demonstrates another post ``basf2`` task. Examplary, it uses
    the merged ntuple files from the previous task to perform an invariant mass
    fit. For demonstartion purposes, the task does not contain the actual run
    logic but calls a fitting module that is defined at a different location. In
    this way, the pipeline gains more readability since each task only contains
    a method call instead of the actual, potentianally long logic.

In a separate module, we define the ``PlotInvariantMass`` class that
performs the actual plotting of the invariant mass.
"""
import numpy as np
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
from plothist import make_hist, plot_hist

variable_dict = {
    "deltaE": {"label": "$\\Delta E$ [GeV]", "range": (-0.1, 0.1)},
    "Mbc": {"label": "$M_\\mathrm{{bc}}$ [GeV/c$^2$]", "range": (5.25, 5.29)},
}


class PlotMetaData:
    _label_store = {
        "mumu": "MC $\\mu^+ \\mu^-$",
        "ee": "MC $e^+ e^-$",
        "mumu_treeFit": "MC $\\mu^+ \\mu^-$ (TreeFit)",
        "ee_treeFit": "MC $e^+ e^-$ (TreeFit)",
    }

    def __init__(self, filename, event_type, treeFit) -> None:
        self.filename = filename
        self.event_type = event_type
        self.treeFit = treeFit

    @staticmethod
    def read_parquet(filename):
        table = pq.read_table(filename, use_pandas_metadata=True)
        return table.to_pandas(strings_to_categorical=True, deduplicate_objects=True, self_destruct=False)

    @property
    def df(self):
        return self.read_parquet(self.filename)

    @property
    def plot_label(self):
        return self._label_store[self.event_type + ("_treeFit" if self.treeFit else "")]


class PlotInvariantMass:
    def __init__(self, input_data, resultpaths):
        # Expect the output of the following structure:
        # {"event_type": "mumu", "treeFit": False, "filename": "results/Merged/merged.parquet"}
        self.input_data = input_data
        self.resultpaths = resultpaths
        self.plotmetadata = [PlotMetaData(**data) for data in input_data]

    def plot(self):
        for resultpath in self.resultpaths:
            fig, ax = plt.subplots()
            variable = resultpath.split("/")[-1].split(".")[0]
            bins = np.linspace(*variable_dict[variable]["range"], 100)
            for data in self.plotmetadata:
                hist = make_hist(data.df[variable], bins=bins)
                plot_hist(hist, ax=ax, histtype="step", linewidth=1.2, label=data.plot_label)

            ax.set_xlabel(variable_dict[variable]["label"])
            ax.set_ylabel("Entries")
            ax.legend()
            fig.savefig(resultpath, bbox_inches="tight")


# %%
# In the workflow pipeline, we only define the ``PlotInvariantMassTask`` which handles the
# input and output files. The actual plotting is outsourced to the ``PlotInvariantMass``
# class defined in a separate location and can potentially be reused in other tasks.
# The task inherits from the ``MergerTask`` and uses the output files of the
# ``MergerTask`` as input files.

# %%
from itertools import product
import b2luigi
from Ex12_merge_files import MergerTask


@b2luigi.inherits(MergerTask, without=["event_type", "treeFit"])
class PlotInvariantMassTask(b2luigi.Task):
    result_dir = "results/Plot"

    def requires(self):
        for event_type, tree_fit in product(["mumu", "ee"], [True, False]):
            yield MergerTask(n_events=self.n_events, n_loops=self.n_loops, event_type=event_type, treeFit=tree_fit)

    def output(self):
        yield self.add_to_output("deltaE.png")
        yield self.add_to_output("Mbc.png")

    def run(self):
        input_data = []
        for input in self.get_all_input_file_names():
            components = input.split("/")

            # Extract event_type and treeFit values
            event_type = [comp.split("=")[1] for comp in components if comp.startswith("event_type=")][0]
            tree_fit = [comp.split("=")[1] == "True" for comp in components if comp.startswith("treeFit=")][0]

            # Append the dictionary to the result list
            input_data.append({"event_type": event_type, "treeFit": tree_fit, "filename": input})

        # Call "general" plotting module that contains the actual task and
        # produces files according to the prefined `output` method
        plotter = PlotInvariantMass(
            input_data=input_data,
            resultpaths=[
                self.get_output_file_name("deltaE.png"),
                self.get_output_file_name("Mbc.png"),
            ],
        )

        plotter.plot()


if __name__ == "__main__":
    b2luigi.process(PlotInvariantMassTask(n_events=100, n_loops=40), workers=40)
