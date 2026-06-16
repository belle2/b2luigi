from typing import Annotated, Optional

import typer
from rich.console import Console

import b2luigi
from b2luigi.cli.utils import process_task_instance

test_app = typer.Typer(name="test", help="Build a task from the given python script and execute it as b2luigi task.")
console = Console()


def test_task(
    exec_script: str,
    output: str,
    input: Optional[str] = None,
) -> None:
    """Build and run a one-off b2luigi task wrapping *exec_script*.

    :param exec_script: Path to the Python script to execute inside the task.
    :param output: Output filename registered as the task's output target.
    :param input: Optional input filename; if given, a prerequisite task is created.
    """
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


@test_app.callback(invoke_without_command=True)
def test(
    exec_script: Annotated[
        str,
        typer.Option("-s", help="Path to the Python script to execute as a b2luigi task."),
    ],
    output: Annotated[
        str,
        typer.Option("-o", help="Output filename for the task target."),
    ],
    input: Annotated[
        Optional[str],
        typer.Option("-i", help="Optional input filename; creates a prerequisite task."),
    ] = None,
) -> None:
    """Build and run a one-off b2luigi task wrapping a Python script.

    :param exec_script: Path to the Python script to execute as a b2luigi task.
    :param output: Output filename for the task target.
    :param input: Optional input filename; creates a prerequisite task.
    """
    test_task(exec_script=exec_script, output=output, input=input)
