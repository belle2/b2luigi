import inspect
from typing import Annotated, Any

import cyclopts
from b2luigi.cli.process import process
from b2luigi.core.task import Task
from luigi.parameter import _no_value as luigi_no_value


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
    for name, param in task_params:
        print(param, type(name))
        if param._default != luigi_no_value:
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
