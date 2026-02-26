import b2luigi


class MySubTask(b2luigi.Task):
    parameter0 = b2luigi.IntParameter(default=0)
    parameter1 = b2luigi.IntParameter(default=0, grouping=True)

    max_grouping_size = 10
    batch_system = "htcondor"

    @property
    def htcondor_settings(self):
        return {
            "request_cpus": "1",
            "accounting_group": "belle",
            "container_image": "/cvmfs/belle.cern.ch/images/belle2-base-el8",
            "stream_output": "true",
            "stream_error": "true",
            "requirements": "(TARGET.ProvidesCPU == True) && " + "(TARGET.ProvidesEKPResources == True)",
            "request_memory": "2048",
        }

    def output(self):
        yield self.add_to_output("MySubTask.txt")

    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("MySubTask.txt"), "w") as f:
            f.write("something")


class MyTask(b2luigi.Task):
    parameter2 = b2luigi.IntParameter(default=0)

    def requires(self):
        tasks = []
        for par0 in range(1):
            for par1 in range(10):
                tasks.append(
                    MySubTask(
                        parameter0=par0,
                        parameter1=par1,
                    )
                )
        return tasks

    def output(self):
        yield self.add_to_output("MyTask.txt")

    @b2luigi.on_temporary_files
    def run(self):
        with open(self.get_output_file_name("MyTask.txt"), "w") as f:
            f.write("something")


class MyWrapperTask(b2luigi.WrapperTask):
    parameter3 = b2luigi.IntParameter(default=0)

    def requires(self):
        return [MyTask(parameter2=par2) for par2 in range(self.parameter3)]


if __name__ == "__main__":
    b2luigi.set_setting("result_dir", "results")
    b2luigi.process(MyWrapperTask(parameter3=10), workers=10)
