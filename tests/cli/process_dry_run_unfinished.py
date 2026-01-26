import b2luigi


class MyTask(b2luigi.Task):
    """
    A simple task that does nothing but can be used to test dry run.
    """

    def output(self):
        return b2luigi.LocalTarget("my_task_output.txt")

    def dry_run(self):
        print("This is a dummy output.\n")


if __name__ == "__main__":
    b2luigi.process(MyTask())
