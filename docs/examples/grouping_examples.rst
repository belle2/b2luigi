.. _parameter-grouping-example-label:

Parameter Grouping
==================

This is a minimal example on how to enable the :ref:`parameter-grouping-label` feature in b2luigi.

.. warning::
   This feature currently only works with the HTCondor batch system, but we plan to extend it to other batch systems in the future.
   Help is very welcome here, so if you want to contribute, please check out the :ref:`development-label`.

.. code-block:: python

    import b2luigi


    class MySubTask(b2luigi.Task):
        pir = b2luigi.IntParameter(default=0)
        per = b2luigi.IntParameter(default=0, grouping=True)
        max_grouping_size = 10
        batch_system = "htcondor"

        @property
        def htcondor_settings(self):
            # Return your htcondor_settings here
            pass

        def output(self):
            yield self.add_to_output("MySubTask.txt")

        @b2luigi.on_temporary_files
        def run(self):
            with open(self.get_output_file_name("MySubTask.txt"), "w") as f:
                f.write("something")


    class MyTask(b2luigi.Task):
        par = b2luigi.IntParameter(default=0)

        def requires(self):
            ll = []
            for i in range(1):
                for e in range(10):
                    ll.append(MySubTask(pir=i, per=e))
            return ll

        def output(self):
            yield self.add_to_output("MyTask.txt")

        @b2luigi.on_temporary_files
        def run(self):
            with open(self.get_output_file_name("MyTask.txt"), "w") as f:
                f.write("something")


    class MyWrapperTask(b2luigi.WrapperTask):
        por = b2luigi.IntParameter()

        def requires(self):
            return [MyTask(par=value) for value in range(self.por)]


    if __name__ == "__main__":
        b2luigi.set_setting("result_dir", "results")

        b2luigi.process(MyWrapperTask(por=10), workers=10)

In this example ``MySubTask`` is grouped in chunks of 10 tasks consuming only one worker each.
