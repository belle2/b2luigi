from b2luigi.cli.utils import get_task_instance, process_task_instance


def remove_task(class_name: str, task_filename="tasks.py", parameters_file="parameters.py") -> None:
    task_instance = get_task_instance(class_name, task_filename, parameters_file)
    process_task_instance(task_instance, remove=[class_name])
