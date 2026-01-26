import b2luigi


class MyTask(b2luigi.Task):
    """
    A simple task that does nothing but can be used to test remove output.
    """

    def output(self):
        return b2luigi.LocalTarget("my_task_output.txt")

    def remove_output(self) -> None:
        return self._remove_output()


@b2luigi.requires(MyTask)
class MyOtherTask(b2luigi.Task):
    """
    Another simple task that depends on MyTask.
    """

    def output(self):
        return b2luigi.LocalTarget("my_other_task_output.txt")

    def remove_output(self) -> None:
        return self._remove_output()


if __name__ == "__main__":
    b2luigi.process(MyOtherTask())
