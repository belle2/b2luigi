import importlib
import inspect
import os
from typing import Annotated, Any, Dict

import cyclopts
from b2luigi.cli.process import process
from b2luigi.core.task import Task
from luigi.parameter import _no_value as luigi_no_value


def import_from_file(filename: str, module_name: str) -> Any:
    path = os.path.join(os.getcwd(), filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} not found in the current directory")

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {filename}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_parameters(filename="parameters.py") -> Dict[str, Any]:
    params_module = import_from_file(filename, "user_parameters")
    if not hasattr(params_module, "config"):
        raise AttributeError(f"{filename} must define a 'config' variable")
    return params_module.config


def build_annotation(
    name: str,
) -> Any:
    """Allow both kebab and underscore CLI spellings for a parameter."""
    option_names = [f"--{name.replace('_', '-')}", f"--{name}"]
    return Annotated[Any, cyclopts.Parameter(name=option_names)]


def register_task_command(task: Task, app: cyclopts.App):
    def command_factory(**kwargs):
        process(**kwargs)

    task_name = task.__name__
    task_params = task.get_params()
    required_params = []
    default_params = []
    configured_params = load_parameters()  # TODO: pass the file path for the parameters.py properly here
    for name, param in task_params:
        if name in configured_params:
            default_params.append(
                inspect.Parameter(
                    name,
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=build_annotation(name),
                    default=configured_params[name],
                )
            )
        elif param._default != luigi_no_value:
            default_params.append(
                inspect.Parameter(
                    name, inspect.Parameter.KEYWORD_ONLY, annotation=build_annotation(name), default=param._default
                )
            )
        else:
            required_params.append(
                inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=build_annotation(name))
            )

    signature = inspect.Signature(parameters=required_params + default_params)
    command_factory.__signature__ = signature
    command_factory.__name__ = task_name
    command_factory.__doc__ = f"Run the {task_name} task." + "\n" + task.__doc__ if task.__doc__ else ""
    app.command(command_factory)
