import collections
import os

import luigi
import luigi.server
import luigi.configuration
from rich.console import Console
from rich.prompt import Confirm

from b2luigi.batch.workers import SendJobWorkerSchedulerFactory
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator
from b2luigi.core.utils import create_output_dirs, flatten_to_dict, flatten_to_file_paths

console = Console()


def run_batch_worker(task):
    """
    Executes a single task directly as a batch worker.

    This is the new-CLI counterpart to :obj:`run_as_batch_worker`.  Instead of searching
    a task graph by ID, the caller is responsible for passing the already-reconstructed
    task instance.  This avoids the need for ``cli_args`` and a full dependency-tree
    traversal.

    :param task: The task instance to execute.
    :raises BaseException: If execution fails, the exception is re-raised after calling
        the task's failure handler.
    """
    set_setting("_dispatch_local_execution", True)
    try:
        create_output_dirs(task)
        task.run()
        task.on_success()
    except BaseException as ex:
        task.on_failure(ex)
        raise ex


def run_as_batch_worker(task_list, cli_args, kwargs):
    """
    Executes a specific task from a list of tasks as a batch worker.

    This function iterates through a list of root tasks and their dependencies
    to find and execute the task specified by the ``cli_args.task_id``. If the task
    is found, it sets up the environment, runs the task, and handles success or
    failure events. If the task is not found, an error is raised.

    :param task_list: A list of tasks to search for the specified task.
    :type task_list: list
    :param cli_args: Command-line arguments containing the ``task_id`` of the task to execute.
    :param kwargs: Additional keyword arguments (currently unused).
    :type kwargs: dict
    :raises ValueError: If the specified ``task_id`` does not exist in the task graph.
    :raises BaseException: If the task execution fails, the exception is raised after
        invoking the task's failure handler.
    """
    found_task = False
    for root_task in task_list:
        for task in task_iterator(root_task):
            if task.task_id != cli_args.task_id:
                continue

            found_task = True
            set_setting("_dispatch_local_execution", True)

            # TODO: We do not process the information if (a) we have a new dependency and (b) why the task has failed.
            # TODO: Would be also nice to run the event handlers
            try:
                create_output_dirs(task)
                task.run()
                task.on_success()
            except BaseException as ex:
                task.on_failure(ex)
                raise ex

            return

    if not found_task:
        raise ValueError(
            f"The task id {cli_args.task_id} to be executed by this batch worker "
            f"does not exist in the locally reproduced task graph."
        )


def run_batched(task_list, cli_args, kwargs):
    """
    Executes a batch of Luigi tasks with the provided command-line arguments and keyword arguments.

    :param task_list: A list of task instances to be executed.
    :type task_list: list
    :param cli_args: A list of command-line arguments (unused; kept for legacy callers).
    :param kwargs: A dictionary of additional keyword arguments to pass to the Luigi runner.
    :type kwargs: dict
    """
    run_luigi(task_list, kwargs)


def run_local(task_list, cli_args, kwargs):
    """
    Executes a list of Luigi tasks locally by setting the batch system to ``local``.

    :param task_list: A list of Luigi task instances to be executed.
    :type task_list: list
    :param cli_args: Command-line arguments (unused; kept for legacy callers).
    :param kwargs: Additional keyword arguments for task execution.
    :type kwargs: dict
    """
    set_setting("batch_system", "local")
    run_luigi(task_list, kwargs)


def run_luigi(task_list: list, kwargs: dict):
    """
    Executes Luigi tasks with the specified configuration.

    This function sets up the ``luigi`` scheduler and worker configurations based
    on the provided keyword arguments, then runs the specified list of tasks.

    :param task_list: A list of task instances to be executed.
    :type task_list: list
    :param kwargs: Additional keyword arguments to configure :obj:`luigi.build`.
        Supported keys: ``scheduler_host``, ``scheduler_port``, and any argument
        accepted by :func:`luigi.build`.
    :type kwargs: dict
    """
    scheduler_host = kwargs.pop("scheduler_host", None)
    scheduler_port = kwargs.pop("scheduler_port", None)

    if scheduler_host or scheduler_port:
        if scheduler_host:
            kwargs["scheduler_host"] = scheduler_host
        if scheduler_port:
            kwargs["scheduler_port"] = scheduler_port
    else:
        kwargs["local_scheduler"] = True

    kwargs["worker_scheduler_factory"] = SendJobWorkerSchedulerFactory()

    kwargs.setdefault("log_level", "INFO")
    luigi.build(task_list, **kwargs)


def run_test_mode(task_list, cli_args, kwargs):
    """
    Executes the given tasks in test mode with local execution enabled.

    This function sets ``_dispatch_local_execution`` (see :obj:`dispatch`) to enable local execution and then
    builds the provided list of tasks using the local scheduler.

    :param task_list: A list of task instances to be executed.
    :type task_list: list
    :param cli_args: Command-line arguments passed to the CLI (not used in this function).
    :param kwargs: Additional keyword arguments to be passed to :obj:`luigi.build`.
    :type kwargs: dict
    """
    set_setting("_dispatch_local_execution", True)
    luigi.build(task_list, log_level="DEBUG", local_scheduler=True, **kwargs)


def get_task_outputs(task):
    """Return the same dict structure as :obj:`get_all_output_files_in_tree` but
    for a single task without traversing its dependency tree.

    :param task: The task instance whose outputs should be collected.
    :returns: Mapping of output key to list of output-entry dicts.
    :rtype: collections.defaultdict
    """
    from b2luigi.core.utils import get_serialized_parameters

    result = collections.defaultdict(list)
    output_dict = flatten_to_dict(task.output())
    for target_key, target in output_dict.items():
        converted = flatten_to_file_paths({target_key: target})
        file_key, file_names = converted.popitem()
        result[file_key].append(
            dict(
                exists=target.exists(),
                parameters=get_serialized_parameters(task),
                file_name=os.path.abspath(file_names.pop()),
            )
        )
    return result


def _build_parent_map(root_tasks: list) -> dict[str, list[str]]:
    """Build a mapping from task_id to the list of immediate parent class names.

    Performs a BFS from ``root_tasks``. For each visited task, each of its
    direct requirements is mapped to the requirer's class name. Duplicate
    class names (e.g. the same parent class with different parameter values)
    are recorded only once per child.

    :param root_tasks: Task instances to start the BFS from.
    :type root_tasks: list
    :returns: Mapping of ``task_id`` → list of parent class names (order is
        BFS discovery order).
    :rtype: dict[str, list[str]]
    """
    parent_map: dict[str, list[str]] = {}
    visited: set[str] = set()
    queue = list(root_tasks)
    while queue:
        task = queue.pop(0)
        if task.task_id in visited:
            continue
        visited.add(task.task_id)
        for req in luigi.task.flatten(task.requires()):
            parents = parent_map.setdefault(req.task_id, [])
            name = task.__class__.__name__
            if name not in parents:
                parents.append(name)
            if req.task_id not in visited:
                queue.append(req)
    return parent_map


def _render_task_outputs(task_output_pairs, required_by_map: dict[str, list[str]] | None = None) -> None:
    """Render a list of (task, output_dict) pairs using Rich.

    :param task_output_pairs: Iterable of ``(task_instance, {key: [{"file_name": ..., "exists": ...}]})``.
    :type task_output_pairs: Iterable[tuple]
    :param required_by_map: Optional mapping of task_id to parent class names,
        produced by :func:`_build_parent_map`. When provided, a ``required by:``
        subtitle is added to each panel whose task_id appears in the map.
    :type required_by_map: dict[str, list[str]] | None
    """
    from rich.table import Table
    from rich.panel import Panel

    for task, outputs in task_output_pairs:
        table = Table(show_header=True, header_style="bold", show_lines=False, box=None)
        table.add_column("Output", style="dim")
        table.add_column("Location")
        table.add_column("", justify="center", no_wrap=True)

        for key, entries in outputs.items():
            for entry in entries:
                status = "[green]✓[/green]" if entry["exists"] else "[red]✗[/red]"
                table.add_row(key, entry["file_name"], status)

        subtitle = None
        if required_by_map is not None:
            parents = required_by_map.get(task.task_id, [])
            if parents:
                subtitle = f"[dim]required by: {', '.join(parents)}[/dim]"

        console.print(
            Panel(table, title=f"[bold]{task.__class__.__name__}[/bold]", subtitle=subtitle, border_style="cyan")
        )


def render_graph_tree(task_list: list, show_params: bool = False, show_status: bool = False) -> None:
    """Render the task dependency graph as a Rich terminal tree.

    Each task is a node in the tree; edges follow ``task.requires()``.  Shared
    nodes (a task required by multiple parents) are shown in full under their
    first occurrence and as a reference marker (``↳ ClassName (already shown
    above)``) under subsequent parents.

    :param task_list: Root task instances to render.
    :type task_list: list
    :param show_params: If ``True``, include parameter values on each node label.
    :type show_params: bool
    :param show_status: If ``True``, check output existence and append a
        completion indicator (✓ / ✗) to each node label.
    :type show_status: bool
    """
    import luigi.task
    from rich.tree import Tree
    from b2luigi.core.utils import get_serialized_parameters

    def _label(task) -> str:
        label = task.__class__.__name__
        if show_params:
            serialized = get_serialized_parameters(task)
            if serialized:
                pairs = ", ".join(f"{k}={v}" for k, v in serialized.items())
                label += f"({pairs})"
        if show_status:
            outputs = luigi.task.flatten(task.output())
            if outputs and all(t.exists() for t in outputs):
                label += " [green]✓[/green]"
            elif outputs:
                label += " [red]✗[/red]"
        return label

    seen: set[str] = set()

    def _add_node(parent: Tree, task) -> None:
        if task.task_id in seen:
            parent.add(f"[dim]↳ {task.__class__.__name__} (already shown above)[/dim]")
            return
        seen.add(task.task_id)
        branch = parent.add(_label(task))
        for req in luigi.task.flatten(task.requires()):
            _add_node(branch, req)

    root = Tree("[bold]Task Graph[/bold]")
    for task in task_list:
        _add_node(root, task)
    console.print(root)


def show_task_outputs(task_list: list) -> None:
    """Show output files for the given tasks only — no dependency-tree traversal.

    :param task_list: Task instances whose outputs should be displayed.
    :type task_list: list
    """
    _render_task_outputs((task, get_task_outputs(task)) for task in task_list)


def show_all_outputs(task_list: list, show_required_by: bool = False) -> None:
    """Show output files for all tasks in the dependency trees rooted at ``task_list``.

    :param task_list: Root task instances; the full dependency tree is traversed.
    :type task_list: list
    :param show_required_by: If ``True``, annotate each requirement panel with
        a ``required by:`` subtitle listing immediate parent class names.
    :type show_required_by: bool
    """
    parent_map = _build_parent_map(task_list) if show_required_by else None
    seen = set()
    pairs = []
    for root_task in task_list:
        for task in task_iterator(root_task):
            if task.task_id in seen:
                continue
            seen.add(task.task_id)
            pairs.append((task, get_task_outputs(task)))
    _render_task_outputs(pairs, required_by_map=parent_map)


def dry_run(task_list):
    """
    Perform a dry run of the given tasks, simulating their execution without
    actually running them. This function iterates through the provided task
    list, identifies tasks that are not yet complete, and executes their ``dry_run`` method.

    :param task_list: A list of tasks to be processed. Each task is expected to be
        iterable and may contain subtasks.
    :type task_list: list
    """
    nonfinished_task_list = collections.defaultdict(set)

    for root_task in task_list:
        for task in task_iterator(root_task, only_non_complete=True):
            nonfinished_task_list[task.__class__.__name__].add(task)

    non_completed_tasks = 0
    for task_class in sorted(nonfinished_task_list):
        console.print(f"[bold]{task_class}[/bold]")
        for task in nonfinished_task_list[task_class]:
            console.print(f"\tWould run {task}")

            # execute the dry_run method of the task if it is implemented
            if hasattr(task, "dry_run"):
                console.print("\tcall: dry_run()")
                task.dry_run()
            console.print()

            non_completed_tasks += 1

    if non_completed_tasks:
        console.print(f"In total {non_completed_tasks}")
        raise SystemExit(0)
    console.print("All tasks are finished!")
    raise SystemExit(0)


def remove_outputs(task_list, target_tasks, auto_confirm=False, keep_tasks=None):
    """Remove the outputs of the specified tasks.

    :param task_list: A list of root tasks to traverse.
    :type task_list: list
    :param target_tasks: Task class names whose outputs should be removed.
    :type target_tasks: list
    :param auto_confirm: If ``True``, skip confirmation prompt.
    :type auto_confirm: bool
    :param keep_tasks: List of task class names to KEEP outputs for.
    :type keep_tasks: list | None
    """
    all_tasks: set = set()
    task_by_class: collections.defaultdict = collections.defaultdict(set)

    def visit(task):
        if task in all_tasks:
            return
        all_tasks.add(task)
        task_by_class[task.__class__.__name__].add(task)
        try:
            children = luigi.task.flatten(task.requires())
        except Exception as e:
            console.print(f"[red]Failed to get requires() for {task}: {e}[/red]")
            children = []
        for child in children:
            visit(child)

    for root in task_list:
        visit(root)

    to_be_removed_tasks: collections.defaultdict = collections.defaultdict(set)
    matched_target_tasks: set = set()
    for target_class in target_tasks:
        matched = task_by_class.get(target_class, set())
        if matched:
            matched_target_tasks.add(target_class)
            to_be_removed_tasks[target_class].update(matched)

    if keep_tasks:
        keep_tasks = set(keep_tasks)
        for keep_class in keep_tasks:
            if keep_class in to_be_removed_tasks:
                console.print(f"[yellow]Keeping {keep_class} outputs.[/yellow]")
                del to_be_removed_tasks[keep_class]
        console.print()

    unseen_tasks = set(target_tasks) - matched_target_tasks

    if not to_be_removed_tasks:
        console.print("Nothing to remove.")
        raise SystemExit(0)

    if not auto_confirm:
        if unseen_tasks:
            console.print("[yellow]The following tasks were not found in the graph and can't be removed:[/yellow]")
            for task in sorted(unseen_tasks):
                console.print(f"\t- [bold]{task}[/bold]")
            console.print()

        console.print("The following task outputs will be removed:")
        for task_class in sorted(to_be_removed_tasks):
            console.print(f"\t- [bold]{task_class}[/bold]")
        console.print()

        confirmed = Confirm.ask("Remove these outputs?")
    else:
        confirmed = True

    if not confirmed:
        console.print("[yellow]No tasks were removed.[/yellow]")
        raise SystemExit(0)

    removed_tasks = 0
    for task_class in sorted(to_be_removed_tasks):
        console.print(f"[bold]{task_class}[/bold]")
        for task in to_be_removed_tasks[task_class]:
            console.print(f"\t[green]Removing...[/green] {task}")
            if hasattr(task, "remove_output"):
                console.print("\tcall: remove_output()")
                task.remove_output()
                removed_tasks += 1
            else:
                console.print(f"\t[yellow]No remove_output() implemented for {task_class}.[/yellow]")
            console.print()

    if removed_tasks:
        console.print(f"[green]Removed outputs for {removed_tasks} tasks.[/green]")
    else:
        console.print("[yellow]No outputs were removed.[/yellow]")

    raise SystemExit(0)


def remove_requirement_outputs(task_list: list, auto_confirm: bool = False) -> None:
    """Remove outputs for the given tasks and all tasks they transitively require.

    Traverses the full dependency tree downward from each task in ``task_list``
    via :func:`task_iterator` and removes outputs for every discovered task.

    :param task_list: Task instances to start traversal from.
    :type task_list: list
    :param auto_confirm: If ``True``, skip confirmation prompt.
    :type auto_confirm: bool
    :returns: None
    :rtype: None
    """
    seen: set[str] = set()
    flat_list: list = []
    for task in task_list:
        for t in task_iterator(task):
            if t.task_id not in seen:
                seen.add(t.task_id)
                flat_list.append(t)

    if not flat_list:
        console.print("Nothing to remove.")
        raise SystemExit(0)

    if not auto_confirm:
        console.print("The following task outputs will be removed:")
        for task_class in sorted({t.__class__.__name__ for t in flat_list}):
            console.print(f"\t- [bold]{task_class}[/bold]")
        console.print()
        confirmed = Confirm.ask("Remove these outputs?")
    else:
        confirmed = True

    if not confirmed:
        console.print("[yellow]No tasks were removed.[/yellow]")
        raise SystemExit(0)

    removed_tasks = 0
    for task in flat_list:
        console.print(f"[bold]{task.__class__.__name__}[/bold]")
        console.print(f"\t[green]Removing...[/green] {task}")
        if hasattr(task, "remove_output"):
            console.print("\tcall: remove_output()")
            task.remove_output()
            removed_tasks += 1
        else:
            console.print(f"\t[yellow]No remove_output() implemented for {task.__class__.__name__}.[/yellow]")
        console.print()

    if removed_tasks:
        console.print(f"[green]Removed outputs for {removed_tasks} tasks.[/green]")
    else:
        console.print("[yellow]No outputs were removed.[/yellow]")

    raise SystemExit(0)


def render_task_list(tasks: list) -> None:
    """Render a Rich table listing all available task classes and their one-line docstrings.

    Wraps the table in a ``b2luigi``-branded panel and prints it to stdout.

    :param tasks: Task class objects to display.
    :type tasks: list
    """
    from rich.panel import Panel
    from rich.table import Table

    table = Table(title="Available Tasks", show_lines=False)
    table.add_column("Task", style="bold")
    table.add_column("Description")

    for cls in tasks:
        doc = (getattr(cls, "__doc__", "") or "").strip().splitlines()
        short = doc[0].strip() if doc else ""
        table.add_row(cls.__name__, short)

    console.print(Panel.fit(table, title="b2luigi", border_style="cyan"))


def render_task_help(cls) -> None:
    """Render a Rich panel with the docstring and parameter table for a single task class.

    :param cls: The task class whose documentation should be displayed.
    """
    import inspect

    import luigi.parameter
    from rich.markdown import Markdown
    from rich.panel import Panel
    from rich.table import Table

    doc = inspect.getdoc(cls) or "(No docstring provided.)"
    console.print(Panel.fit(Markdown(doc), title=f"{cls.__name__}", border_style="cyan"))

    _sentinel = luigi.parameter._no_value

    param_table = Table(show_header=True, header_style="bold", show_lines=False, box=None)
    param_table.add_column("Parameter", style="bold")
    param_table.add_column("Type")
    param_table.add_column("Default")

    for name, param in cls.get_params():
        type_name = type(param).__name__
        default = getattr(param, "default", _sentinel)
        default_str = "(required)" if default is _sentinel else str(default)
        param_table.add_row(name, type_name, default_str)

    if cls.get_params():
        console.print(Panel(param_table, title="Parameters", border_style="cyan"))
