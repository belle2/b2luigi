import os
import stat
import subprocess

from b2luigi.core.settings import get_setting
from b2luigi.core.utils import (
    add_on_failure_function,
    create_cmd_from_task,
    get_filename,
    get_log_file_dir,
    get_task_file_dir,
    create_apptainer_command,
)


def create_executable_wrapper(task):
    """
    To incorporate all settings (environment, working paths, remote or locally)
    we create an executable bash script which is called instead of the application
    and which will setup everything accordingly before doing the actual work.
    """
    shell = get_setting("shell", task=task, default="bash")
    executable_wrapper_content = [f"#!/bin/{shell}", "set -e"]
    apptainer_image = get_setting("apptainer_image", task=task, default="")

    # 1. First part is the folder we need to change if given
    working_dir = get_setting("working_dir", task=task, default=os.path.abspath(os.path.dirname(get_filename())))
    executable_wrapper_content.append(f"cd {working_dir}")

    executable_wrapper_content.append("echo 'Working in the folder:'; pwd")

    # 2. Second part of the executable wrapper, the environment.
    # (a) If given, use the environment script
    env_setup_script = get_setting("env_script", task=task, default="")
    if env_setup_script:
        if not apptainer_image:
            executable_wrapper_content.append("echo 'Setting up the environment'")
            executable_wrapper_content.append(f"source {env_setup_script}")

    # (b) Now override with any environment from the task or settings
    env_overrides = get_setting("env", task=task, default={})
    for key, value in env_overrides.items():
        value = value.replace("'", "'''")
        value = f"'{value}'"
        executable_wrapper_content.append(f"export {key}={value}")

    executable_wrapper_content.append("echo 'Current environment:'; env")

    # 3. Third part is to build the actual program
    command = " ".join(create_cmd_from_task(task))

    # 4. Forth part is to create the correct execution command
    # (a) If a valid apptainer image is provided, build an apptainer command
    if apptainer_image:
        executable_wrapper_content.append(f"echo 'Will now execute the program with the image {apptainer_image}'")
        apptainer_command_list = create_apptainer_command(command, task=task)
        apptainer_command = " ".join(apptainer_command_list[:-1])
        apptainer_command += f" '{apptainer_command_list[-1]}'"

        executable_wrapper_content.append(apptainer_command)

    # (b) Otherwise, just execute the command
    else:
        executable_wrapper_content.append("echo 'Will now execute the program'")
        executable_wrapper_content.append(f"exec {command}")

    # Now we can write the file
    executable_file_dir = get_task_file_dir(task)
    os.makedirs(executable_file_dir, exist_ok=True)

    executable_wrapper_path = os.path.join(executable_file_dir, "executable_wrapper.sh")

    with open(executable_wrapper_path, "w") as f:
        f.write("\n".join(executable_wrapper_content))

    # make wrapper executable
    st = os.stat(executable_wrapper_path)
    os.chmod(executable_wrapper_path, st.st_mode | stat.S_IEXEC)

    return executable_wrapper_path


def run_task_remote(task):
    """
    Run a given task "remotely", which means
    create an executable script and call it via a subprocess
    call.
    """
    log_file_dir = get_log_file_dir(task)
    os.makedirs(log_file_dir, exist_ok=True)
    stdout_log_file = os.path.join(log_file_dir, "stdout")
    stderr_log_file = os.path.join(log_file_dir, "stderr")

    executable_file = create_executable_wrapper(task)

    add_on_failure_function(task)

    with open(stdout_log_file, "w") as stdout_file:
        with open(stderr_log_file, "w") as stderr_file:
            return_code = subprocess.call([executable_file], stdout=stdout_file, stderr=stderr_file)

    if return_code:
        raise RuntimeError(f"Execution failed with return code {return_code}")
