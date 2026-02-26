#!/usr/bin/env python3

"""
CLI utility to query a Luigi scheduler and print a summary table of task statuses.

The script fetches task data from the Luigi scheduler HTTP API, aggregates task
statuses, and renders a formatted ASCII table showing overall and per-task
status counts.
"""

import argparse
import requests
from collections import Counter, defaultdict
from tabulate import tabulate, tabulate_formats
from typing import Dict, List, Any


"""List of all task statuses to be displayed as table columns."""
ALL_STATUSES = ["DONE", "RUNNING", "PENDING", "FAILED", "DISABLED", "UNKNOWN"]

"""Human-readable label for the summary row in the output table."""
SUMMARY_LABEL = "TOTAL"


def fetch_tasks(scheduler_host: str, scheduler_port: int) -> List[Dict[str, Any]]:
    """
    Fetch the list of tasks from a Luigi scheduler.

    This function queries the Luigi scheduler's ``/api/task_list`` endpoint and
    extracts task metadata from the response.

    Args:
        scheduler_host: Hostname or IP address of the Luigi scheduler.
        scheduler_port: Port number of the Luigi scheduler.

    Returns:
        A list of task dictionaries. If the request fails or the response is
        malformed, an empty list is returned.
    """
    url = f"http://{scheduler_host}:{scheduler_port}/api/task_list"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        tasks = data.get("response", {})
        return list(tasks.values()) if isinstance(tasks, dict) else []
    except Exception as e:
        print(f"Error fetching task list from {url}: {e}")
        return []


def aggregate(
    tasks: List[Dict[str, Any]],
    by_parameter: str | None = None,
) -> Dict[str, Dict[str, int]]:
    """
    Aggregate task statuses by task name, optionally sub-grouped by a parameter.

    For each task name, a total row is always produced. If ``by_parameter``
    is provided, additional subgroup rows are created for tasks that define
    that parameter. Tasks without the parameter contribute only to the total.

    Args:
        tasks: A list of task dictionaries as returned by the Luigi API.
        by_parameter: Optional parameter name used for subgrouping.

    Returns:
        A nested mapping of task name to counters, where each task contains
        a ``__TOTAL__`` entry and zero or more parameter-based subgroup entries.
    """
    table: Dict[str, Dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))

    for task in tasks:
        name = task.get("name", "UNKNOWN_TASK")
        status = task.get("status", "UNKNOWN")

        table[name]["__TOTAL__"][status] += 1

        if by_parameter:
            params = task.get("params")
            if isinstance(params, dict) and by_parameter in params:
                key = f"{by_parameter}={params[by_parameter]}"
                table[name][key][status] += 1

    return table


def print_table(
    table: Dict[str, Dict[str, Counter]],
    table_format: str,
) -> None:
    """
    Print a formatted table summarizing task statuses using ``tabulate``.

    Args:
        table: Aggregated task status data.
        table_format: Table format name supported by ``tabulate``.
    """
    if table_format not in tabulate_formats:
        print(f"Warning: table format '{table_format}' is not supported. " f"Using default 'grid'.")
        table_format = "grid"

    headers = ["TASK"] + ALL_STATUSES + ["TOTAL"]
    rows: List[List[Any]] = []

    summary = Counter()
    for task in table.values():
        summary += task["__TOTAL__"]

    rows.append([SUMMARY_LABEL] + [summary.get(s, 0) for s in ALL_STATUSES] + [sum(summary.values())])

    for task_name in sorted(table):
        total = table[task_name]["__TOTAL__"]
        rows.append([task_name] + [total.get(s, 0) for s in ALL_STATUSES] + [sum(total.values())])

        subgroups = sorted(k for k in table[task_name] if k != "__TOTAL__")
        for i, subgroup in enumerate(subgroups):
            prefix = "└─ " if i == len(subgroups) - 1 else "├─ "
            counter = table[task_name][subgroup]
            rows.append(
                [f"{prefix}{task_name}({subgroup})"]
                + [counter.get(s, 0) for s in ALL_STATUSES]
                + [sum(counter.values())]
            )

    print(tabulate(rows, headers=headers, tablefmt=table_format))


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        An argparse namespace containing scheduler connection parameters and
        display options.
    """
    parser = argparse.ArgumentParser(
        description="CLI utility to query a Luigi scheduler and print a summary table of task statuses."
    )
    parser.add_argument(
        "--scheduler-host", type=str, required=True, help="Hostname or IP address of the Luigi scheduler."
    )
    parser.add_argument("--scheduler-port", type=int, required=True, help="Port number of the Luigi scheduler.")
    parser.add_argument("--by-parameter", type=str, default=None, help="Group tasks by the given Luigi parameter.")
    parser.add_argument("--table-format", type=str, default="grid", help="Table format supported by tabulate.")
    return parser.parse_args()


def main() -> None:
    """
    Entry point for the CLI.
    """
    args = parse_args()
    tasks = fetch_tasks(args.scheduler_host, args.scheduler_port)
    table = aggregate(tasks, args.by_parameter)
    print_table(table, table_format=args.table_format)


if __name__ == "__main__":
    main()
