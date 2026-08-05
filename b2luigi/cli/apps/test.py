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
            help="Override a b2luigi setting as key=value (JSON-aware, repeatable), e.g. "
            "--setting apptainer_image=my_image.sif (must be combined with --env-script). "
            "Applied only on the submission host and NEVER forwarded to the batch worker "
            "(unlike settings.json); use it for submission-side-only settings, and "
            "settings.json for anything (e.g. result_dir, log_dir) both sides must agree "
            "on. Cannot override batch_system or env_script — use --batch/--env-script "
            "for those, since FastTask already sets them as class attributes which take "
            "priority over --setting.",
        ),
    ] = None,
    literal_path: Annotated[
        bool,
        typer.Option(
            "--literal-path/--no-literal-path",
            help=(
                "Write -o to the literal path given, exactly as -i already does (the default). "
                "Use --no-literal-path to nest the output under result_dir/param dirs via "
                "add_to_output() instead. No-op with --force (no output() is declared either way)."
            ),
        ),
    ] = True,
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
    :param literal_path: When True (the default), -o writes to the literal path given,
        bypassing result_dir nesting; --no-literal-path restores the nesting.
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
        literal_path=literal_path,
    )
