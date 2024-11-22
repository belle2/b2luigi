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
