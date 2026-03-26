from cyclopts import App, Parameter
from rich.console import Console
from typing import Annotated

import b2luigi
from b2luigi.cli.utils import process_task_instance

test_app = App(name="test", help="Build a task from the given python script and execute it as b2luigi task.")
console = Console()


def test_task(
    exec_script: str,
    output: str,
    input: str | None = None,
) -> None:
    if input is not None:

        class FastReqTask(b2luigi.Task):
            def output(self):
                return self.add_to_output(input)

            def run(self):
                pass

        @b2luigi.requires(FastReqTask)
        class FastTask(b2luigi.Task):
            def output(self):
                return self.add_to_output(output)

            def run(self):
                with open(exec_script) as f:
                    code = f.read()
                    exec(code)
    else:

        class FastTask(b2luigi.Task):
            def output(self):
                return self.add_to_output(output)

            def run(self):
                with open(exec_script) as f:
                    code = f.read()
                    exec(code)

    process_task_instance(FastTask())


@test_app.default
def test(
    exec_script: Annotated[
        str,
        Parameter(name=["-s"], help="The name of the task class to run"),
    ],
    output: Annotated[
        str,
        Parameter(name=["-o"], help="Task definitions file (or $B2LUIGI_TASK_FILE)"),
    ],
    input: Annotated[
        str | None,
        Parameter(name=["-i"], help="Parameters file (or $B2LUIGI_PARAMS_FILE)"),
    ] = None,
):
    test_task(exec_script=exec_script, output=output, input=input)
