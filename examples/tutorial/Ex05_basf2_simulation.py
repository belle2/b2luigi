"""
.. _exercise05_label:

Introduction to ``Basf2PathTask``
=================================

.. hint::
    This example demonstrates how to create a simple b2luigi task that executes a
    ``basf2`` simulation.
    The key points to be learned from this example are the
    :class:`Basf2PathTask <b2luigi.basf2_helper.tasks.Basf2PathTask>` class
    and :meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>` method.

For directly working with the Belle II Analysis Software Framework (``basf2``),
``b2luigi`` provides the :class:`Basf2PathTask <b2luigi.basf2_helper.tasks.Basf2PathTask>`
task class.
This class provides help for the user to create and execute a ``basf2`` path.
The :class:`Basf2PathTask <b2luigi.basf2_helper.tasks.Basf2PathTask>` also provides the
following parameters that can be useful when executing ``basf2`` processes:

- ``num_processes``: number of parallel processes to use for the ``basf2`` execution.

- ``max_event``: maximum number of events to process. If set to ``0``, all events
  will be processed.

In contrast to the normal ``(b2)luigi`` tasks, the execution logic of a
:class:`Basf2PathTask <b2luigi.basf2_helper.tasks.Basf2PathTask>` is not defined in
a ``run`` method but in
:meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>`.
The :meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>` method needs
to return the ``basf2`` path that is created in the steering file.
Furthermore, the ``Progress`` module is automatically added and ``print(b2.statistics)``
is called after the path is processed.

.. warning::
    Due to technical reasons, the path needs to be created within the :meth:`create_path <b2luigi.basf2_helper.tasks.Basf2PathTask.create_path>`
    method. The path can be used in further objects, however, it is not possible
    for it to originate from an outer scope.

Additional tips:

- For later porpuses, we add a parameter (``identifier``) to identify the output file
  uniqley. This is useful when running multiple tasks in parallel.

- We make use of the task own settings via `result_dir`. It is sometimes more useful
  to structure the output of your tasks in task specific directories than solely in the
  parameter based directories. Additionally, good practice is to provide absolute paths
  instead of relative paths as we do in this example.

- No batch submission of this task will be done (``batch_system = "local"``), more will
  come in the next examples.

- The example relies again on the task's own
  :meth:`get_output_file_name <b2luigi.Task.get_output_file_name>` method to generate
  the output file name.
"""

import basf2 as b2
import generators as ge
import simulation as si

import b2luigi
from b2luigi.basf2_helper import Basf2PathTask


class SimulationTask(Basf2PathTask):
    n_events = b2luigi.IntParameter()
    event_type = b2luigi.Parameter(default="mumu")
    identifier = b2luigi.Parameter(default="sample")

    result_dir = "results/Simulation"
    batch_system = "local"

    def output(self):
        yield self.add_to_output("simulation.root")

    def create_path(self):
        main = b2.Path()

        # Set number of events (optional: also set experiment and run number)
        main.add_module("EventInfoSetter", evtNumList=[self.n_events])
        decfile = (
            "./decfiles/BptoKpJpsi_JpsitoMuMu.dec"
            if str(self.event_type) == "mumu"
            else "./decfiles/BptoKpJpsi_JpsitoEE.dec"
        )
        # Signal generation
        ge.add_evtgen_generator(
            path=main, finalstate="signal", signaldecfile=b2.find_file(decfile), eventType=self.event_type
        )

        # Simulation
        si.add_simulation(path=main)

        # Write out simulation output to root file.
        main.add_module("RootOutput", outputFileName=self.get_output_file_name("simulation.root"))

        return main


if __name__ == "__main__":
    b2luigi.process(SimulationTask(n_events=10))
