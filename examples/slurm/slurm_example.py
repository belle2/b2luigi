import b2luigi
import random


class MyNumberTask(b2luigi.Task):
    some_parameter = b2luigi.IntParameter()
    batch_system = "slurm"
    slurm_settings = {"export": "NONE", "ntasks": 1, "mem": "100MB"}

    def output(self):
        yield self.add_to_output("output_file.txt")

    def run(self):
        print("I am now starting a task")
        random_number = random.random()

        with open(self.get_output_file_name("output_file.txt"), "w") as f:
            f.write(f"{random_number}\n")


class MyAverageTask(b2luigi.Task):
    batch_system = "slurm"
    slurm_settings = {"export": "NONE", "ntasks": 1, "mem": "100MB"}

    def requires(self):
        for i in range(10):
            yield self.clone(MyNumberTask, some_parameter=i)

    def output(self):
        yield self.add_to_output("average.txt")

    def run(self):
        print("I am now starting the average task")

        # Build the mean
        summed_numbers = 0
        counter = 0
        for input_file in self.get_input_file_names("output_file.txt"):
            with open(input_file, "r") as f:
                summed_numbers += float(f.read())
                counter += 1

        average = summed_numbers / counter

        with open(self.get_output_file_name("average.txt"), "w") as f:
            f.write(f"{average}\n")


if __name__ == "__main__":
    b2luigi.process(MyAverageTask(), workers=200, batch=True)
