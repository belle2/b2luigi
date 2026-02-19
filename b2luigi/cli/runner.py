import collections

import luigi
import luigi.server
import luigi.configuration

from b2luigi.batch.workers import SendJobWorkerSchedulerFactory
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator, get_all_output_files_in_tree
from b2luigi.core.utils import create_output_dirs


def run_as_batch_worker(task_list, cli_args, kwargs):
    """
    Executes a specific task from a list of tasks as a batch worker.

    This function iterates through a list of root tasks and their dependencies
    to find and execute the task specified by the ``cli_args.task_id``. If the task
    is found, it sets up the environment, runs the task, and handles success or
    failure events. If the task is not found, an error is raised.

    Args:
        task_list (list): A list of tasks to search for the specified task.
        cli_args (Namespace): Command-line arguments containing the ``task_id`` of
            the task to execute.
        kwargs (dict): Additional keyword arguments (currently unused).

    Raises:
        ValueError: If the specified ``task_id`` does not exist in the task graph.
        BaseException: If the task execution fails, the exception is raised after
            invoking the task's failure handler.

    Notes:
        - The function sets the ``_dispatch_local_execution`` setting to ``True``
          before running the task. See :obj:`dispatch`.
        - The task's output directories are created before execution. See :obj:`create_output_dirs`.
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

    Args:
        task_list (list): A list of task instances to be executed.
        cli_args (list): A list of command-line arguments to be passed to :obj:`run_luigi`.
        kwargs (dict): A dictionary of additional keyword arguments to pass to the Luigi runner.
    """
    run_luigi(task_list, cli_args, kwargs)


def run_local(task_list, cli_args, kwargs):
    """
    Executes a list of Luigi tasks locally by setting the batch system to ``local``.

    Args:
        task_list (list): A list of Luigi task instances to be executed.
        cli_args (list): Command-line arguments to be passed to :obj:`run_luigi`.
        kwargs (dict): Additional keyword arguments for task execution.
    """
    set_setting("batch_system", "local")
    run_luigi(task_list, cli_args, kwargs)


def run_luigi(task_list, cli_args, kwargs):
    """
    Executes Luigi tasks with the specified configuration.

    This function sets up the ``luigi`` scheduler and worker configurations based
    on the provided command-line arguments and keyword arguments, then runs
    the specified list of tasks.

    Args:
        task_list (list): A list of task instances to be executed.
        cli_args (Namespace): Parsed command-line arguments containing
            scheduler host and port information.
        kwargs (dict): Additional keyword arguments to configure :obj:`luigi.build`.

    Behavior:
        - If ``scheduler_host`` or ``scheduler_port`` is provided in ``cli_args``,
          they are used to configure the scheduler. Otherwise, a local
          scheduler is used.
        - A custom worker-scheduler factory (:obj:`SendJobWorkerSchedulerFactory`)
          is set in the configuration.
    """
    if cli_args.scheduler_host or cli_args.scheduler_port:
        core_settings = luigi.interface.core()
        host = cli_args.scheduler_host or core_settings.scheduler_host
        port = int(cli_args.scheduler_port) or core_settings.scheduler_port
        kwargs["scheduler_host"] = host
        kwargs["scheduler_port"] = port
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

    Args:
        task_list (list): A list of task instances to be executed.
        cli_args (list): Command-line arguments passed to the CLI (not used in this function).
        kwargs (dict): Additional keyword arguments to be passed to :obj:`luigi.build`.
    """
    set_setting("_dispatch_local_execution", True)
    luigi.build(task_list, log_level="DEBUG", local_scheduler=True, **kwargs)


def show_all_outputs(task_list, *args, **kwargs):
    """
    Displays all output files for a list of tasks, grouped by their respective keys.

    This function iterates through a list of tasks, collects all output files
    from their dependency trees, and prints them grouped by their keys.
    The existence of each file is visually indicated using colored output
    (green for existing files, red for non-existing files).

    Args:
        task_list (list): A list of tasks to process.

    Notes:
        - The function uses :obj:`get_all_output_files_in_tree` to retrieve output files
          for each task.
        - The existence of files is determined but the task status is not checked.
    """
    from colorama import Fore, Style

    all_output_files = collections.defaultdict(list)

    for task in task_list:
        output_files = get_all_output_files_in_tree(task)
        for key, file_names in output_files.items():
            all_output_files[key] += file_names

    for key, file_names in all_output_files.items():
        print(key)

        file_names = {d["file_name"]: d["exists"] for d in file_names}
        for file_name, exists in file_names.items():
            # TODO: this is not correct as it does not check the task status!
            if exists:
                print("\t", Fore.GREEN, file_name, Style.RESET_ALL)
            else:
                print("\t", Fore.RED, file_name, Style.RESET_ALL)
        print()


def dry_run(task_list):
    """
    Perform a dry run of the given tasks, simulating their execution without
    actually running them. This function iterates through the provided task
    list, identifies tasks that are not yet complete, and executes their ``dry_run`` method.

    Args:
        task_list (list): A list of tasks to be processed. Each task is
                          expected to be iterable and may contain subtasks.
    """
    nonfinished_task_list = collections.defaultdict(set)

    for root_task in task_list:
        for task in task_iterator(root_task, only_non_complete=True):
            nonfinished_task_list[task.__class__.__name__].add(task)

    non_completed_tasks = 0
    for task_class in sorted(nonfinished_task_list):
        print(task_class)
        for task in nonfinished_task_list[task_class]:
            print("\tWould run", task)

            # execute the dry_run method of the task if it is implemented
            if hasattr(task, "dry_run"):
                print("\tcall: dry_run()")
                task.dry_run()
            print()

            non_completed_tasks += 1

    if non_completed_tasks:
        print("In total", non_completed_tasks)
        exit(0)
    print("All tasks are finished!")
    exit(0)


def remove_outputs(task_list, target_tasks, only=False, auto_confirm=False, keep_tasks=None):
    """
    Removes the outputs of specified tasks and their dependent tasks.

    Args:
        task_list (list): A list of root tasks to traverse.
        target_tasks (list): Task class names whose outputs should be removed.
        only (bool, optional): If True, remove only the specified tasks' outputs.
                               If False, also remove outputs of dependents.
        auto_confirm (bool, optional): If True, skip confirmation prompt.
        keep_tasks (list, optional): List of task class names to KEEP outputs for.
    """

    # ---------- Build dynamic graph ----------
    all_tasks = set()
    task_by_class = collections.defaultdict(set)
    child_to_parents = collections.defaultdict(set)

    def visit(task):
        if task in all_tasks:
            return
        all_tasks.add(task)
        task_by_class[task.__class__.__name__].add(task)

        try:
            children = luigi.task.flatten(task.requires())
        except Exception as e:
            print(f"Failed to get requires() for {task}: {e}")
            children = []

        for child in children:
            child_to_parents[child].add(task)
            visit(child)

    for root in task_list:
        visit(root)

    # ---------- Determine tasks to remove ----------
    to_be_removed_tasks = collections.defaultdict(set)
    matched_target_tasks = set()

    if only:
        for target_class in target_tasks:
            matched = task_by_class.get(target_class, set())
            if matched:
                matched_target_tasks.add(target_class)
                to_be_removed_tasks[target_class].update(matched)
    else:
        for target_class in target_tasks:
            matched = task_by_class.get(target_class, set())
            if matched:
                matched_target_tasks.add(target_class)
                for task in matched:
                    dependents = collect_all_dependents(task, child_to_parents)
                    for dep in dependents:
                        to_be_removed_tasks[dep.__class__.__name__].add(dep)

    # ---------- Apply keep filter ----------
    if keep_tasks:
        keep_tasks = set(keep_tasks)
        for keep_class in keep_tasks:
            if keep_class in to_be_removed_tasks:
                print(f"Keeping {keep_class} outputs.")
                del to_be_removed_tasks[keep_class]
        print()

    # ---------- Identify unseen ----------
    unseen_tasks = set(target_tasks) - matched_target_tasks

    if not to_be_removed_tasks:
        print("Nothing to remove.")
        exit(0)

    # ---------- Confirm ----------
    if not auto_confirm:
        if unseen_tasks:
            print("The following tasks were not found in the graph and can't be removed:")
            for task in sorted(unseen_tasks):
                print(f"\t- {task}")
            print()

        print("The following task outputs will be removed:")
        for task_class in sorted(to_be_removed_tasks):
            print(f"\t- {task_class}")
        print()

        confirm = input("Are you sure you want to remove these outputs? [y/N]: ").strip().lower()
    else:
        confirm = "y"

    if confirm not in {"y", "yes"}:
        print("No tasks were removed.")
        exit(0)

    # ---------- Execute removal ----------
    removed_tasks = 0
    for task_class in sorted(to_be_removed_tasks):
        print(f"Class: {task_class}")
        for task in to_be_removed_tasks[task_class]:
            print(f"\tRemoving output for {task}")
            if hasattr(task, "remove_output"):
                print("\tcall: remove_output()")
                task.remove_output()
                removed_tasks += 1
            else:
                print(f"\tNo remove_output() implemented for {task_class}.")
            print()

    if removed_tasks:
        print(f"Removed outputs for {removed_tasks} tasks.")
    else:
        print("No outputs were removed.")

    exit(0)


def collect_all_dependents(task, child_to_parents):
    """
    Given a task, walk up the DAG to collect all tasks that depend on it.
    """
    visited = set()
    stack = [task]
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        stack.extend(child_to_parents.get(current, []))
    return visited
