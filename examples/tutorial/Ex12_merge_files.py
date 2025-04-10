"""
.. _exercise12_label:

Common Analysis Workflow
========================

.. hint::
    This example demonstrates the transition from a ``basf2`` analysis to the
    part independent of ``basf2`` as this the usual usecase. Examplary, this
    exercise merges the ntuple files from the ``basf2`` analysis. Additionally, the
    task demonstarates that every task, for the most part, works as a normal
    Python class and the run method can be for example functionally structured.

One of the command line tools in ``b2luigi`` is the ``dry_run`` method. This
will only execute the :meth:`b2luigi.Task.dry_run` method of each task. This can be useful
when developing a pipeline and one wants to preview the output of the
pipeline without actually running it.

In the ``dry_run`` method one can perform preparotory operations for
the actual run such as evaluating input and output files. This can
be reused during the actual ``run`` method to make the code more
readable and maintainable.
"""

import uproot
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import b2luigi
from Ex07_basf2_analysis import AnalysisTask


@b2luigi.inherits(AnalysisTask, without="identifier")
class MergerTask(b2luigi.Task):
    n_loops = b2luigi.IntParameter()

    result_dir = "results/Merged"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filenames = []

    def requires(self):
        for i in range(self.n_loops):
            yield AnalysisTask(
                n_events=self.n_events, identifier=f"loop_{i}", event_type=self.event_type, treeFit=self.treeFit
            )

    def output(self):
        yield self.add_to_output("merged.parquet")

    def read_root(self, filename):
        """Reads a ROOT file and returns the content as a pandas DataFrame.

        Args:
            filename (str): Filename of the input root file

        Returns:
            pd.DataFrame: Ntuples from the input ROOT file as a DataFrame
        """
        events = uproot.open(filename)
        variables = events["Bp"]
        return variables.arrays(library="pd")

    def write_parquet(self, dataframe, output_path) -> None:
        """Writes a pandas DataFrame to a parquet file.

        Args:
            dataframe (pd.DataFrame): Ntuple to be written to the parquet file
            output_path (str): Path to the output parquet file
        """
        pq.write_table(
            pa.Table.from_pandas(dataframe),
            output_path,
        )

    def dry_run(self):
        print("Merging the analysis files:")
        for filename in self.get_all_input_file_names():
            self.filenames.append(filename)
            print(f"  - {filename}")

    def run(self):
        self.dry_run()

        # Read all the input root files into pandas dataframes
        dataframes = [self.read_root(filename) for filename in self.filenames]

        # Concatenate all the dataframes into a single dataframe
        # Caution: Don't use this method for large and/or many dataframes
        #          since the performance is very bad
        merged_df = pd.concat(dataframes)

        # Write the merged dataframe to a parquet file
        self.write_parquet(dataframe=merged_df, output_path=self.get_output_file_name("merged.parquet"))


if __name__ == "__main__":
    b2luigi.process(MergerTask(n_events=100, n_loops=40), workers=40)
