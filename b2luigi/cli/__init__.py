import argparse
import importlib.util
import os
import sys

import b2luigi


def import_from_file(filename, module_name):
    """Dynamically import a Python file as a module."""
    path = os.path.join(os.getcwd(), filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} not found in the current directory")
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_parameters():
    """Load parameters from parameters.py (must define 'config')"""
    params_module = import_from_file("parameters.py", "user_parameters")
    if not hasattr(params_module, "config"):
        raise AttributeError("parameters.py must define a 'config' variable")
    return params_module.config


def load_task_class(class_name):
    """Load a class from tasks.py"""
    tasks_module = import_from_file("tasks.py", "user_tasks")
    if not hasattr(tasks_module, class_name):
        raise AttributeError(f"Class '{class_name}' not found in tasks.py")
    return getattr(tasks_module, class_name)


def run_task(class_name):
    params = load_parameters()
    TaskClass = load_task_class(class_name)
    task_instance = TaskClass(**params)
    b2luigi.process(task_instance, ignore_additional_command_line_args=False)


def main():
    parser = argparse.ArgumentParser(prog="mytool", description="Run user-defined tasks")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run a task class from tasks.py")
    run_parser.add_argument("classname", help="Name of the task class to run")

    args = parser.parse_args()

    if args.command == "run":
        run_task(args.classname)
    else:
        parser.print_help()
