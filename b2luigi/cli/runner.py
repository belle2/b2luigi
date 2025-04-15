import collections

import luigi
import luigi.server
import luigi.configuration

from b2luigi.batch.workers import SendJobWorkerSchedulerFactory
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import find_dependents, task_iterator, get_all_output_files_in_tree
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
        exit(1)
    print("All tasks are finished!")
    exit(0)


def remove_outputs(task_list, target_tasks, only=False, auto_confirm=False):
    """
    Removes the outputs of specified tasks and their dependent tasks.

    Args:
        task_list (list): A list of tasks to iterate over.
        target_tasks (list): A list of target task class names whose outputs should be removed.
        only (bool, optional): If ``True``, removes only the outputs of the specified target tasks.
                               If ``False``, removes the outputs of the target tasks and all their dependent tasks.
                               Defaults to ``False``.
        auto_confirm (bool, optional): If ``True``, skips the confirmation prompt and proceeds with removal.
                                        Defaults to ``False``.

    Notes:
        - The :obj:`find_dependents` function is used to identify dependent tasks for a given target task.
          In large task graphs, this can be time-consuming.
    """
    to_be_removed_tasks = collections.defaultdict(set)

    # Remove the output of this task and all its dependent tasks
    if not only:
        for root_task in task_list:
            for target_task in target_tasks:
                dependent_tasks = find_dependents(task_iterator(root_task), target_task)
                for task in dependent_tasks:
                    to_be_removed_tasks[task.__class__.__name__].add(task)

    # Remove only the output of the tasks that are given in the list
    else:
        for root_task in task_list:
            for task in task_iterator(root_task):
                if task.__class__.__name__ in target_tasks:
                    to_be_removed_tasks[task.__class__.__name__].add(task)

    if not to_be_removed_tasks:
        print("Nothing to remove.")
        exit(0)

    if not auto_confirm:
        print("The following outputs of these tasks are about to be removed:")
        for task in sorted(to_be_removed_tasks):
            print(f"\t- {task}")

        confirm = input("Are you sure you want to remove all of these tasks outputs? [y/N]: ").strip().lower()
    else:
        confirm = "y"

    if confirm not in {"y", "yes"}:
        print("No tasks were removed.")
        exit(0)

    removed_tasks = 0
    for task_class in sorted(to_be_removed_tasks):
        print(task_class)
        for task in to_be_removed_tasks[task_class]:
            print("\tRemoving output for", task)

            # execute the remove_output method of the task if it is implemented
            if hasattr(task, "remove_output"):
                print("\tcall: remove_output()")
                task.remove_output()
                removed_tasks += 1
            else:
                print(f"\tNo remove_output() method implemented for {task_class}. Doing nothing.")
            print()

    if removed_tasks:
        print("In total", removed_tasks)
        exit(1)
    exit(0)
