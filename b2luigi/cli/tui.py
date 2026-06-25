"""
Progress TUI for b2luigi workflows.

Requires the 'tui' optional dependency: pip install b2luigi[tui]
"""

import logging
import threading

from rich.table import Table
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Footer, Header, Static

from b2luigi.batch.workers import SendJobWorkerSchedulerFactory


# ── Scheduler factory: captures the local scheduler for RPC polling ───────────


class _TUISchedulerFactory(SendJobWorkerSchedulerFactory):
    """Captures the scheduler passed to create_worker so the TUI can poll it."""

    def __init__(self):
        super().__init__()
        self.scheduler = None

    def create_worker(self, scheduler, worker_processes, assistant=False):
        self.scheduler = scheduler
        return super().create_worker(scheduler, worker_processes, assistant=assistant)


# ── Logging ───────────────────────────────────────────────────────────────────


class _TUILogHandler(logging.Handler):
    def __init__(self, app: "ProgressApp"):
        super().__init__()
        self._app = app

    def emit(self, record: logging.LogRecord):
        with self._app._log_lock:
            self._app._log_lines.append(self.format(record))


# ── Data model ────────────────────────────────────────────────────────────────


class TaskInstance:
    def __init__(self, task):
        self.class_name = task.__class__.__name__
        self.params = dict(task.param_kwargs)
        self.status = "PENDING"

    @property
    def params_str(self) -> str:
        return ", ".join(f"{k}={v}" for k, v in self.params.items())


class TaskGroup:
    def __init__(self, class_name: str):
        self.class_name = class_name
        self.instances: dict[str, TaskInstance] = {}
        self.expanded = False

    def get_or_add(self, task) -> TaskInstance:
        key = task.task_id
        if key not in self.instances:
            self.instances[key] = TaskInstance(task)
        return self.instances[key]

    _STATUS_ORDER = {"FAILED": 0, "RUNNING": 1, "DONE": 2, "PENDING": 3}

    @property
    def sorted_instances(self) -> list[TaskInstance]:
        return sorted(self.instances.values(), key=lambda x: (self._STATUS_ORDER.get(x.status, 4), x.params_str))

    @property
    def total(self) -> int:
        return len(self.instances)

    @property
    def counts(self) -> tuple[int, int, int, int]:
        statuses = [t.status for t in self.instances.values()]
        return (
            statuses.count("DONE"),
            statuses.count("FAILED"),
            statuses.count("RUNNING"),
            statuses.count("PENDING"),
        )


# ── Quit confirmation modal ───────────────────────────────────────────────────


class _ConfirmQuitScreen(ModalScreen[bool]):
    """Modal that asks for confirmation before killing an in-progress workflow."""

    CSS = """
    #confirm-box {
        width: 58;
        height: auto;
        border: thick $error;
        background: $surface;
        padding: 1 2;
        margin: 1 2;
    }
    """
    BINDINGS = [
        Binding("y", "confirm_yes", "Yes — terminate"),
        Binding("n", "confirm_no", "No — keep running"),
        Binding("enter", "confirm_no", "No", show=False),
        Binding("escape", "confirm_no", "No", show=False),
    ]

    def __init__(self, message: str):
        super().__init__()
        self._message = message

    def compose(self) -> ComposeResult:
        yield Static(self._message, id="confirm-box")

    def action_confirm_yes(self):
        self.dismiss(True)

    def action_confirm_no(self):
        self.dismiss(False)


# ── Textual app ───────────────────────────────────────────────────────────────


class ProgressApp(App):
    TITLE = "b2luigi Progress TUI"
    CSS = """
    #display {
        padding: 0 1;
    }
    Static {
        overflow-y: auto;
    }
    """
    BINDINGS = [
        Binding("f", "toggle_fold", "Fold/Unfold"),
        Binding("d", "toggle_debug", "Debug"),
        Binding("o", "open_stdout", "Stdout"),
        Binding("e", "open_stderr", "Stderr"),
        Binding("b", "close_log_view", "Back"),
        Binding("escape", "close_log_view", "Back", show=False),
        Binding("q", "quit_tui", "Quit"),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("up", "cursor_up", "Up", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("down", "cursor_down", "Down", show=False),
    ]

    def __init__(self, task_list: list, run_fn, scheduler_factory=None):
        super().__init__()
        self.theme = "ansi-light"
        self._task_list = task_list
        self._run_fn = run_fn
        self._scheduler_factory = scheduler_factory
        self.groups: dict[str, TaskGroup] = {}
        self.group_order: list[str] = []
        self.selected_idx = 0
        self.selected_instance_idx: int | None = None
        self.finished = False
        self._debug_mode = False
        self._log_lines: list[str] = []
        self._log_lock = threading.Lock()
        self._luigi_thread_id: int | None = None
        self._user_quit = False
        self._warning: str = ""
        self._log_view: dict | None = None  # {"title": str, "content": str} when viewing a log
        self._split_classes: set[str] = set()  # classes whose groups are split by first param value
        self._task_id_map: dict[str, object] = {}  # task_id → task object, for log file lookup

    # ── data ──

    _SPLIT_THRESHOLD = 30  # split a class's group when any first-param subgroup exceeds this

    @staticmethod
    def _first_significant_param_val(task) -> str | None:
        """Return the string value of the first significant parameter, or None."""
        try:
            sig_params = [(n, p) for n, p in task.get_params() if p.significant]
        except Exception:
            return None
        if not sig_params:
            return None
        first_name = sig_params[0][0]
        return str(task.param_kwargs[first_name]) if first_name in task.param_kwargs else None

    def _group_key(self, task) -> str:
        """Return the group name for a task, splitting by first significant param when warranted."""
        class_name = task.__class__.__name__
        if class_name in self._split_classes:
            first_val = self._first_significant_param_val(task)
            if first_val is not None:
                return f"{class_name} [{first_val}]"
        return class_name

    def _get_or_create_group(self, class_name: str) -> TaskGroup:
        if class_name not in self.groups:
            self.groups[class_name] = TaskGroup(class_name)
            self.group_order.append(class_name)
        return self.groups[class_name]

    # Maps Luigi scheduler status strings to TUI display statuses.
    _LUIGI_TO_TUI_STATUS = {
        "PENDING": "PENDING",
        "RUNNING": "RUNNING",
        "DONE": "DONE",
        "FAILED": "FAILED",
        "DISABLED": "FAILED",
        "UPSTREAM_FAILED": "FAILED",
        "UPSTREAM_DISABLED": "PENDING",
        "UNKNOWN": "PENDING",
    }

    def _poll_scheduler(self):
        """Query the Luigi scheduler's RPC API and update task statuses."""
        scheduler = getattr(self._scheduler_factory, "scheduler", None)
        if scheduler is None:
            return
        try:
            all_tasks = scheduler.task_list("", "")
        except Exception:
            return
        for task_id, task_info in all_tasks.items():
            luigi_status = task_info.get("status", "UNKNOWN")
            tui_status = self._LUIGI_TO_TUI_STATUS.get(luigi_status, "PENDING")
            task_obj = self._task_id_map.get(task_id)
            if task_obj is None:
                continue
            group_key = self._group_key(task_obj)
            group = self.groups.get(group_key)
            if group is None:
                continue
            inst = group.instances.get(task_id)
            if inst is not None:
                inst.status = tui_status

    def _mark_finished(self):
        self.finished = True

    # ── actions ──────────────────────────────────────────────────────────────

    def action_toggle_fold(self):
        if not self.group_order or self.selected_idx >= len(self.group_order):
            return
        name = self.group_order[self.selected_idx]
        if self.selected_instance_idx is not None:
            # On an instance: fold the group and return focus to its header
            self.groups[name].expanded = False
            self.selected_instance_idx = None
        else:
            self.groups[name].expanded = not self.groups[name].expanded

    def _clear_warning(self):
        self._warning = ""

    def action_cursor_up(self):
        self._clear_warning()
        if self.selected_instance_idx is not None:
            if self.selected_instance_idx > 0:
                self.selected_instance_idx -= 1
            else:
                self.selected_instance_idx = None
        elif self.selected_idx > 0:
            self.selected_idx -= 1
            prev_group = self.groups[self.group_order[self.selected_idx]]
            if prev_group.expanded and prev_group.total > 0:
                self.selected_instance_idx = prev_group.total - 1
            else:
                self.selected_instance_idx = None

    def action_cursor_down(self):
        self._clear_warning()
        if not self.group_order:
            return
        current_group = self.groups[self.group_order[self.selected_idx]]
        if self.selected_instance_idx is not None:
            if self.selected_instance_idx < current_group.total - 1:
                self.selected_instance_idx += 1
            elif self.selected_idx < len(self.group_order) - 1:
                self.selected_idx += 1
                self.selected_instance_idx = None
        else:
            if current_group.expanded and current_group.total > 0:
                self.selected_instance_idx = 0
            elif self.selected_idx < len(self.group_order) - 1:
                self.selected_idx += 1
                self.selected_instance_idx = None

    def action_toggle_debug(self):
        self._debug_mode = not self._debug_mode

    def action_quit_tui(self):
        if self.finished:
            self.exit()
            return
        running, pending = 0, 0
        for group in self.groups.values():
            _, _, r, p = group.counts
            running += r
            pending += p
        parts = []
        if running:
            parts.append(f"{running} running")
        if pending:
            parts.append(f"{pending} pending")
        summary = ", ".join(parts) or "tasks in progress"
        msg = (
            f"[bold]Terminate b2luigi?[/bold] ({summary})\n\n"
            f"  Press y to terminate\n"
            f"  Press n / Esc / Enter to keep running"
        )
        self.push_screen(_ConfirmQuitScreen(msg), self._on_quit_confirmed)

    def _on_quit_confirmed(self, confirmed: bool):
        if confirmed:
            self._user_quit = True
            self._interrupt_luigi()
            self.exit()

    def _interrupt_luigi(self):
        if self._luigi_thread_id is None:
            return
        import ctypes

        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(self._luigi_thread_id),
            ctypes.py_object(KeyboardInterrupt),
        )

    def _selected_task(self):
        """Return the TaskInstance currently under the cursor, or None if on a group header."""
        if not self.group_order or self.selected_instance_idx is None:
            return None
        group = self.groups[self.group_order[self.selected_idx]]
        instances = group.sorted_instances
        if self.selected_instance_idx >= len(instances):
            return None
        return instances[self.selected_instance_idx]

    def _open_log(self, which: str):
        import os
        from b2luigi.core.utils import get_log_file_dir

        inst = self._selected_task()
        if inst is None:
            self._warning = "Select a task instance first (unfold a group and navigate to a row)"
            return

        group_name = self.group_order[self.selected_idx]
        group = self.groups[group_name]
        task_id = next(
            tid for tid, ti in group.instances.items() if group.sorted_instances[self.selected_instance_idx] is ti
        )
        task_obj = self._task_id_map.get(task_id)
        if task_obj is None:
            self._warning = f"Could not locate task object for {inst.params_str}"
            return

        log_path = os.path.join(get_log_file_dir(task_obj), which)
        if not os.path.exists(log_path):
            self._warning = f"Log not found: {log_path}"
            return

        self._warning = ""
        try:
            with open(log_path) as f:
                content = f.read()
        except OSError as ex:
            self._warning = f"Cannot read {log_path}: {ex}"
            return

        self._log_view = {"title": log_path, "content": content}
        self.refresh_bindings()

    def action_open_stdout(self):
        self._open_log("stdout")

    def action_open_stderr(self):
        self._open_log("stderr")

    def action_close_log_view(self):
        self._log_view = None
        self.refresh_bindings()

    def check_action(self, action: str, parameters: tuple) -> bool | None:
        in_log = self._log_view is not None
        if action == "close_log_view":
            return in_log or None
        if action in ("toggle_fold", "toggle_debug", "open_stdout", "open_stderr", "cursor_up", "cursor_down"):
            return None if in_log else True
        return True

    # ── rendering ─────────────────────────────────────────────────────────────

    def _build_progress_view(self) -> Table:
        table = Table(show_header=False, box=None, padding=(0, 0), expand=True)
        table.add_column("", ratio=1)

        for idx, group_name in enumerate(self.group_order):
            group = self.groups[group_name]
            done, failed, running, pending = group.counts
            total = group.total
            selected = idx == self.selected_idx

            bar_width = 20
            done_fill = int(bar_width * done / total) if total else 0
            running_fill = int(bar_width * running / total) if total else 0
            failed_fill = int(bar_width * failed / total) if total else 0
            pending_fill = bar_width - done_fill - running_fill - failed_fill

            bar_parts = []
            if done_fill:
                bar_parts.append(f"[green]{'█' * done_fill}[/]")
            if running_fill:
                bar_parts.append(f"[yellow]{'▌' * running_fill}[/]")
            if failed_fill:
                bar_parts.append(f"[red]{'▒' * failed_fill}[/]")
            if pending_fill:
                bar_parts.append(f"[dim]{'░' * pending_fill}[/]")

            if done == total and total > 0:
                status_str = "[green]DONE[/]"
            elif failed > 0:
                status_str = f"[red]{failed} FAILED[/]"
            elif running > 0:
                status_str = f"[yellow]{running} RUNNING[/]"
            else:
                status_str = "[dim]PENDING[/]"

            fold = "▼" if group.expanded else "▶"
            sel = "▸" if selected else " "
            header = Text.from_markup(
                f"{sel}{fold} [{'reverse bold' if selected else 'bold'}]{group_name:<22}[/]"
                f" [{''.join(bar_parts)}] {done}/{total} {status_str}"
            )
            table.add_row(header)

            if group.expanded:
                for inst_idx, inst in enumerate(group.sorted_instances):
                    inst_selected = selected and inst_idx == self.selected_instance_idx
                    icon, color = {
                        "PENDING": ("⏳", "dim"),
                        "RUNNING": ("▶", "yellow"),
                        "DONE": ("✓", "green"),
                        "FAILED": ("✗", "red"),
                    }.get(inst.status, ("?", "dim"))
                    prefix = "  ▸ " if inst_selected else "    "
                    style = "reverse bold" if inst_selected else "dim"
                    table.add_row(
                        Text.from_markup(
                            f"{prefix}[{style}]{inst.params_str:<28}[/] [{color}]{icon}[/] [bold]{inst.status}[/]"
                        )
                    )

        if self.finished:
            table.add_row(Text.from_markup("[bold green]━━━ b2luigi terminated ━━━[/]"))

        if self._warning:
            table.add_row(Text.from_markup(f"[bold yellow]⚠ {self._warning}[/]"))

        return table

    def _build_log_view(self) -> Text:
        title = self._log_view["title"]
        content = self._log_view["content"]
        text = Text()
        text.append(f" {title} \n\n", style="bold reverse")
        text.append(content)
        if not content.endswith("\n"):
            text.append("\n")
        text.append("\n[b / Escape] back", style="dim")
        return text

    def _build_debug_view(self) -> Text:
        with self._log_lock:
            lines = self._log_lines[-500:]
        return Text("\n".join(lines))

    def _refresh_display(self):
        if self._log_view is not None:
            renderable = self._build_log_view()
        elif self._debug_mode:
            renderable = self._build_debug_view()
        else:
            renderable = self._build_progress_view()
        self.query_one("#display", Static).update(renderable)

    # ── lifecycle ──────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="display")
        yield Footer()

    def on_mount(self):
        self._pre_populate()
        self._attach_log_handler()
        self._run_luigi()
        self.set_interval(0.5, self._poll_scheduler)
        self.set_interval(0.1, self._refresh_display)

    def _pre_populate(self):
        from b2luigi.core.utils import task_iterator

        # Collect unique tasks per class (deduplicated by task_id)
        by_class: dict[str, dict[str, object]] = {}
        for root_task in self._task_list:
            for task in task_iterator(root_task):
                class_name = task.__class__.__name__
                by_class.setdefault(class_name, {})[task.task_id] = task

        # Decide which classes need first-param subgrouping: split when any
        # subgroup (keyed by first significant param value) would exceed the threshold.
        for class_name, task_map in by_class.items():
            subgroup_counts: dict[str, int] = {}
            for task in task_map.values():
                first_val = self._first_significant_param_val(task)
                if first_val is not None:
                    subgroup_counts[first_val] = subgroup_counts.get(first_val, 0) + 1
            if subgroup_counts and max(subgroup_counts.values()) > self._SPLIT_THRESHOLD:
                self._split_classes.add(class_name)

        # Populate groups and build the task_id → task map for log file lookup
        for task_map in by_class.values():
            for task in task_map.values():
                self._task_id_map[task.task_id] = task
                inst = self._get_or_create_group(self._group_key(task)).get_or_add(task)
                try:
                    if task.complete():
                        inst.status = "DONE"
                except Exception:
                    pass

    def _attach_log_handler(self):
        log_handler = _TUILogHandler(self)
        log_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        for logger_name in ("luigi", "luigi-interface", "b2luigi"):
            logging.getLogger(logger_name).addHandler(log_handler)

    @work(thread=True)
    def _run_luigi(self):
        self._luigi_thread_id = threading.current_thread().ident
        # Luigi's Worker installs a SIGUSR1 handler in __init__, but signal.signal()
        # only works in the main thread. Since we run Luigi in a worker thread, tell
        # Luigi to skip that step via its own config flag.
        from luigi import configuration as _luigi_cfg

        _luigi_cfg.get_config().set("worker", "no_install_shutdown_handler", "true")

        try:
            self._run_fn()
        except (KeyboardInterrupt, SystemExit):
            pass
        self.call_from_thread(self._mark_finished)
