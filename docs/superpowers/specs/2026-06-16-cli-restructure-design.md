# CLI Restructure: Positional Task Name + `tasks` Sub-App

**Date:** 2026-06-16
**Branch:** `feature/one-cli-to-rule-them-all`

## Problem

`b2luigi run --task MyTask` is not the natural way to invoke a task. Users expect to type `b2luigi run MyTask` (positional). Tab completion for the task name does not work in this form because `classname` is registered as a `typer.Option`, not a `typer.Argument`. Additionally, `b2luigi run list` and `b2luigi run help` are odd sub-sub-commands that belong at a higher level.

## Command Surface

| Command | Behaviour |
|---|---|
| `b2luigi run MyTask` | Run a task by positional name (with tab-complete) |
| `b2luigi tasks` | List all available task classes |
| `b2luigi tasks info` | Show docstring + parameters for every task |
| `b2luigi tasks info MyTask` | Show docstring + parameters for one task (with tab-complete) |
| `b2luigi about` | Show env/install info (renamed from `b2luigi info`) |

Removed: `b2luigi run list`, `b2luigi run help`, `b2luigi run --task`/`-t`.

## Architecture

### `cli/apps/run.py`

- Change `classname` from `typer.Option("--task", ..., shell_complete=complete_task_names)` to `typer.Argument(shell_complete=complete_task_names)`.
- Delete the `@run_app.command("list")` and `@run_app.command("help")` subcommands.
- `run_app` keeps its Typer sub-app shape; `invoke_without_command=True` and the `ctx.invoked_subcommand` guard are no longer needed and can be removed.

### `cli/apps/tasks.py` (new file)

```
tasks_app = typer.Typer(name="tasks", invoke_without_command=True)
```

- **`@tasks_app.callback`**: when invoked without a subcommand, calls `runner.render_task_list(get_task_classes(task_file))`. Accepts `--task-file`/`-f`.
- **`@tasks_app.command("info")`**: optional positional `classname: Annotated[Optional[str], typer.Argument(shell_complete=complete_task_names)] = None`. No arg → `render_task_help` for every class; one arg → `render_task_help` for that class. Accepts `--task-file`/`-f`.

Follows the existing thin-app convention: logic stays in `runner.py`, the app file only parses args and delegates.

### `cli/__init__.py`

- Register `tasks_app` via `app.add_typer(tasks_app, name="tasks")`.
- Rename `@app.command("info")` to `@app.command("about")` and update its `help` string to "Show environment and b2luigi installation info."

## Error Handling

- `b2luigi run` with no argument: Typer's built-in "Missing argument 'CLASSNAME'" message — no custom handling needed.
- `b2luigi tasks info <unknown>`: existing `validate_classnames` raises `CliUserError`.
- `b2luigi tasks` or `b2luigi tasks info` when `tasks.py` has no task classes: catch `ValueError` from `get_task_classes` and re-raise as `CliUserError` with a hint to run `b2luigi init`.
- Tab completion hitting a broken `tasks.py`: `complete_task_names` already silently returns `[]` on any exception.

## Testing

New or updated test cases (subprocess-based, matching existing `tests/cli/` style):

- `b2luigi run MyTask` (positional) — task name is passed through and executed correctly.
- `b2luigi tasks` — output lists available task classes.
- `b2luigi tasks info MyTask` — output includes that task's docstring and parameters.
- `b2luigi tasks info` (no arg) — output includes info for all tasks.
- `b2luigi about` — output includes version and platform fields.

Shell completion behaviour is excluded from the automated test suite.
