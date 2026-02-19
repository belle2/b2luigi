import inspect
from typing import Annotated, Any

import b2luigi
from b2luigi.cli.utils import load_parameters
import cyclopts
from luigi.parameter import _no_value as luigi_no_value


def build_annotation(
    name: str,
) -> Any:
    """Allow both kebab and underscore CLI spellings for a parameter."""
    option_names = [f"--{name.replace('_', '-')}", f"--{name}"]
    return Annotated[Any, cyclopts.Parameter(name=option_names)]


def register_task_command(task: b2luigi.Task, app: cyclopts.App):
    def command_factory(**kwargs):
        b2luigi.process(**kwargs)

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
