# ----------------------------------------------------------------------------
# Starter Kit: b2luigi (B2GM 2024)
# Authors: Alexander Heidelbach, Jonas Eppelt, Giacomo De Pietro
#
# Scope: This example demonstrates another post basf2 task. Examplary, it uses
# the merged ntuple files from the previous task to perform an invariant mass
# fit. For demonstartion purposes, the task does not contain the actual run
# logic but calls a fitting module that is defined at a different location. In
# this way, the pipeline gains more readabilty since each task only contains
# a method call instead of the actual, potentianally long logic.
#
# ----------------------------------------------------------------------------

import b2luigi
from Ex12_merge_files import MergerTask
from Plotter import PlotInvariantMass


@b2luigi.inherits(MergerTask, without="event_type")
class PlotInvariantMassTask(b2luigi.Task):
    result_dir = "results/Plot"

    def requires(self):
        yield MergerTask(n_events=self.n_events, n_loops=self.n_loops, event_type="mumu", treeFit=True)
        yield MergerTask(n_events=self.n_events, n_loops=self.n_loops, event_type="mumu", treeFit=False)
        yield MergerTask(n_events=self.n_events, n_loops=self.n_loops, event_type="ee", treeFit=True)
        yield MergerTask(n_events=self.n_events, n_loops=self.n_loops, event_type="ee", treeFit=False)

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

        # Call "general" ploting module that contains the actual task and
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
