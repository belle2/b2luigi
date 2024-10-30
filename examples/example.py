import b2luigi


class MyTask(b2luigi.Task):
    parameter = b2luigi.Parameter()
    apptainer_image = "/cvmfs/belle.cern.ch/images/belle2-base-el9"
    apptainer_mount = ["/cvmfs"]

    @property
    def executable(self):
        return ["python3"]

    @property
    def env_script(self):
        return "setup.sh"

    def run(self):
        with open(self.get_output_file_name("output.txt"), "w") as f:
            f.write(f"{self.parameter}")

    def output(self):
        yield self.add_to_output("output.txt")


if __name__ == "__main__":
    b2luigi.process(MyTask(parameter=1))
