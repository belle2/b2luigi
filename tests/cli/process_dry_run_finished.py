import b2luigi


class AlwaysExistingTarget(b2luigi.Target):
    """
    Target that always exists
    """

    def exists(self):
        return True


class MyTask(b2luigi.Task):
    """
    A simple task that does nothing but can be used to test dry run.
    """

    def output(self):
        return AlwaysExistingTarget()

    def dry_run(self):
        print("This is a dummy output.\n")


if __name__ == "__main__":
    b2luigi.process(MyTask())
