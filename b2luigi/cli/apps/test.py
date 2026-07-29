from typing import Annotated

import typer

from b2luigi.cli.runner import test_task

test_app = typer.Typer(name="test", help="Build a task from the given python script and execute it as b2luigi task.")


@test_app.callback(invoke_without_command=True)
def test(
    script: Annotated[
        str,
        typer.Option("-s", help="Path to the Python script to execute as a b2luigi task."),
    ],
    output: Annotated[
        str,
        typer.Option("-o", help="Output filename for the task target."),
    ],
    input: Annotated[
        str | None,
        typer.Option("-i", help="Optional input filename; creates a prerequisite task."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Always re-run even if the output already exists."),
    ] = False,
    batch: Annotated[
        bool,
        typer.Option("--batch", help="Submit task via batch system (batch_system='auto')."),
    ] = False,
    env_script: Annotated[
        str | None,
        typer.Option(
            "--env-script",
            help=(
                "Path to an environment setup script, sourced by the batch submission wrapper. "
                "Only takes effect combined with --batch; a no-op otherwise."
            ),
        ),
    ] = None,
    setting: Annotated[
        list[str] | None,
        typer.Option(
            "--setting",
            help="Override any b2luigi setting as key=value (JSON-aware, repeatable), e.g. "
            "--setting apptainer_image=my_image.sif. Submission-host-scoped, like settings.json.",
        ),
    ] = None,
    extra_args: Annotated[
        list[str] | None,
        typer.Argument(help="Extra arguments forwarded verbatim to the script subprocess."),
    ] = None,
) -> None:
    """Build and run a one-off b2luigi task wrapping a Python script.

    :param script: Path to the Python script to execute as a b2luigi task.
    :param output: Output filename for the task target.
    :param input: Optional input filename; creates a prerequisite task.
    :param force: Always re-run even if the output already exists.
    :param batch: Submit task via batch system.
    :param env_script: Path to an environment setup script; only takes effect with --batch.
    :param setting: List of key=value overrides applied via set_setting() before the run.
    :param extra_args: Extra arguments forwarded verbatim to the script subprocess.
    """
    test_task(
        exec_script=script,
        output=output,
        input_file=input,
        force=force,
        batch=batch,
        extra_args=extra_args or [],
        env_script=env_script,
        settings=setting or [],
    )
