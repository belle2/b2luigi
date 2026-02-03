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
from typing import Dict, List, Any


"""List of all task statuses to be displayed as table columns."""
ALL_STATUSES = ["DONE", "RUNNING", "PENDING", "FAILED", "DISABLED", "UNKNOWN"]

"""Human-readable label for the summary row in the output table."""
SUMMARY_LABEL = "Summary"


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


def aggregate(tasks: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    """
    Aggregate task statuses by task name and overall summary.

    Each task contributes a count to its own task name as well as to a global
    summary row. Missing task names or statuses are replaced with defaults.

    Args:
        tasks: A list of task dictionaries as returned by the Luigi API.

    Returns:
        A mapping from task name to a dictionary of status counts. The mapping
        always includes a summary entry keyed by ``__SUMMARY__``.
    """
    table: Dict[str, Counter] = defaultdict(Counter)

    for task in tasks:
        name = task.get("name", "UNKNOWN_TASK")
        status = task.get("status", "UNKNOWN")

        table[name][status] += 1
        table["__SUMMARY__"][status] += 1

    return {name: {status: counter.get(status, 0) for status in ALL_STATUSES} for name, counter in table.items()}


def print_table(table: Dict[str, Dict[str, int]]) -> None:
    """
    Print an ASCII table of task status counts. The individual task rows are
    printed below a summary row.

    Args:
        table: Aggregated task status counts produced by :func:`aggregate`.
    """
    rows = ["__SUMMARY__"]
    rows += sorted(name for name in table if name != "__SUMMARY__")

    headers = ["TASK"] + ALL_STATUSES + ["TOTAL"]

    col_widths = {"TASK": max(len("TASK"), *(len(SUMMARY_LABEL if r == "__SUMMARY__" else r) for r in rows))}
    for status in ALL_STATUSES + ["TOTAL"]:
        col_widths[status] = max(
            len(status), *(len(str(sum(table[r].values()) if status == "TOTAL" else table[r][status])) for r in rows)
        )

    def separator() -> str:
        """Return a horizontal separator line for the table."""
        return "+ " + " + ".join("-" * col_widths[h] for h in headers) + " +"

    def row(values):
        """Format a single row of values according to column widths."""
        return (
            "| "
            + " | ".join(
                f"{v:<{col_widths[h]}}" if h == "TASK" else f"{v:>{col_widths[h]}}" for h, v in zip(headers, values)
            )
            + " |"
        )

    print(separator())
    print(row(headers))

    print(separator())
    print(
        row(
            [SUMMARY_LABEL]
            + [str(table["__SUMMARY__"][s]) for s in ALL_STATUSES]
            + [str(sum(table["__SUMMARY__"].values()))]
        )
    )

    print(separator())
    for name in rows[1:]:
        print(row([name] + [str(table[name][s]) for s in ALL_STATUSES] + [str(sum(table[name].values()))]))

    print(separator())


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
    return parser.parse_args()


def main() -> None:
    """
    Entry point for the CLI.
    """
    args = parse_args()
    tasks = fetch_tasks(args.scheduler_host, args.scheduler_port)
    table = aggregate(tasks)
    print_table(table)


if __name__ == "__main__":
    main()
