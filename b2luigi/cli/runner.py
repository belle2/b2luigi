import collections
import os
import pathlib
import shlex
import subprocess
import sys
from typing import Any

import b2luigi
import luigi
import luigi.server
import luigi.configuration
from rich.console import Console
from rich.prompt import Confirm

from b2luigi.batch.workers import SendJobWorkerSchedulerFactory
from b2luigi.cli.utils import parse_kv_params
from b2luigi.core.settings import get_setting, set_setting
from b2luigi.core.utils import (
    create_output_dirs,
    flatten_to_dict,
    flatten_to_file_paths,
    get_serialized_parameters,
    task_iterator,
)

console = Console()
#: Diagnostics that must not pollute machine-readable stdout. ``show --paths``
#: is designed to be piped and ``graph --format dot`` to be fed to Graphviz, so
#: anything printed alongside them belongs on stderr.
stderr_console = Console(stderr=True)


def _build_fast_req_task(input_file: str) -> type:
    """Build a prerequisite task class asserting that *input_file* already exists.

    The returned ``FastReqTask`` subclasses :class:`b2luigi.ExternalTask`
    (no-op ``run()``, completeness determined solely by ``output().exists()``)
    and declares its target directly at the literal *input_file* path —
    resolved to an absolute path at build time, exactly like ``exec_script``
    in :func:`_build_fast_task` — rather than routing through
    :meth:`~b2luigi.core.task.Task.add_to_output`, which would incorrectly
    nest the resolved path under ``result_dir``.

    :param input_file: Path to the pre-existing input file, as passed to
        ``b2luigi test -i``. Used verbatim as the output dict key so
        :meth:`~b2luigi.core.task.Task.get_input_file_name` on the dependent
        ``FastTask`` can look it up unchanged.
    :type input_file: str
    :returns: A dynamically created :class:`b2luigi.ExternalTask` subclass.
    :rtype: type
    """
    abs_input_file = os.path.abspath(input_file)

    def _output(self):
        yield {input_file: b2luigi.LocalTarget(abs_input_file)}

    return type("FastReqTask", (b2luigi.ExternalTask,), {"output": _output})


def _build_fast_task(
    exec_script: str,
    output: str,
    input_file: str | None,
    force: bool,
    batch: bool,
    extra_args: list[str],
    env_script: str | None = None,
    literal_path: bool = True,
    executable: str | None = None,
) -> type:
    """Build the main task class that runs *exec_script* as a subprocess.

    The script is invoked as ``<executable> <exec_script> -o <output_path> [-i <input_path>]
    [-- extra_args]`` where *output_path* is the full b2luigi-resolved path (under
    ``result_dir``, unless *literal_path* is ``True``) and *executable* defaults to the
    current interpreter. The ``--`` separator appears only when *executable* is set and
    there is at least one extra argument.

    When *force* is ``False`` the class declares ``output()``, so Luigi skips
    the task when the output already exists.  When *force* is ``True`` no
    ``output()`` is declared and Luigi always runs the task.

    :param exec_script: Path to the Python script to run. Resolved to an
        absolute path at build time so batch workers whose working directory
        differs from the submission host's can still find it.
    :type exec_script: str
    :param output: Output filename key (passed to :meth:`add_to_output`, unless
        *literal_path* is ``True``).
    :type output: str
    :param input_file: Optional input filename key; if set, the full resolved
        path is forwarded to the script as ``-i``.
    :type input_file: str | None
    :param force: When ``True``, omit ``output()`` so the task always runs.
    :type force: bool
    :param batch: When ``True``, set ``batch_system = "auto"``; otherwise ``"local"``.
    :type batch: bool
    :param extra_args: Extra CLI arguments forwarded verbatim to the subprocess.
    :type extra_args: list[str]
    :param env_script: Optional path to an environment setup script. Resolved to an
        absolute path and set as the ``env_script`` class attribute, so
        :func:`~b2luigi.core.executable.create_executable_wrapper` sources it on real
        batch systems exactly as it would for a hand-written task. Submission-time-only:
        never forwarded to the batch worker, since the worker inherits the already-sourced
        environment from the submission-host wrapper.
    :type env_script: str | None
    :param literal_path: When ``True`` (the default), ``-o``'s target is the literal
        *output* path (resolved to absolute, exactly like *input_file* is already
        handled), bypassing :meth:`~b2luigi.core.task.Task.add_to_output`'s
        ``result_dir`` nesting entirely. ``False`` restores the nesting. No-op
        when *force* is ``True`` (no ``output()`` is declared either way).
    :type literal_path: bool
    :param executable: Optional command used to execute *exec_script*, split with
        :func:`shlex.split` so multi-token values (``"apptainer exec img.sif basf2"``)
        work. Defaults to the current interpreter (:data:`sys.executable`). Needed for
        basf2 steering files, whose ``-o``/``-i`` are provided by the ``basf2`` wrapper
        binary rather than by the script, and therefore only take effect when the script
        is run as ``basf2 steer.py`` rather than ``python3 steer.py``. When set, ``--`` is
        inserted before *extra_args* so the script's own arguments are not consumed by the
        wrapper. Unrelated to the ``executable`` **setting**, which is the command that
        launches the b2luigi batch worker itself.
    :type executable: str | None
    :returns: A dynamically created ``b2luigi.Task`` subclass.
    :rtype: type

    The generated class also carries a ``task_cmd_additional_args`` class attribute
    encoding all constructor arguments as ``--script``/``--output-file``/``--input-file``/
    ``--force``/``--extra-arg``/``--literal-path`` flags.  :func:`b2luigi.core.utils.create_cmd_from_task`
    appends these to the batch worker command so that ``batch-runner`` can reconstruct
    the task without importing it.

    Every b2luigi-generated value in that list is ``shlex.quote``d, because
    :func:`b2luigi.core.utils.create_cmd_from_task` appends the list verbatim and
    :func:`b2luigi.core.executable.create_executable_wrapper` then flattens the whole
    command into a shell script with ``" ".join(...)`` — an unquoted value containing
    a space would be re-split before reaching the worker.
    """
    exec_script = os.path.abspath(exec_script)
    abs_output = os.path.abspath(output)
    exe_tokens = shlex.split(executable) if executable else [sys.executable]

    def _run(self):
        # force=True declares no output(), so fall back to _get_output_file_target's
        # independent (result_dir-based) path reconstruction in that case. literal_path
        # is handled directly here since it never goes through that reconstruction.
        if literal_path:
            output_path = abs_output
        else:
            output_path = self._get_output_file_target(output).path
        cmd = exe_tokens + [exec_script, "-o", output_path]
        if input_file is not None:
            cmd += ["-i", self.get_input_file_name(input_file)]
        if extra_args:
            # A custom executable (e.g. basf2) consumes -o/-i itself, so the script's
            # own arguments must be separated from it. Never emit a lone trailing --.
            cmd += (["--"] if executable else []) + extra_args
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError(f"Script '{exec_script}' exited with code {result.returncode}")
        if not os.path.exists(output_path):
            raise RuntimeError(f"Script '{exec_script}' ran successfully but did not produce output '{output}'")

    attrs: dict[str, Any] = {
        "batch_system": "auto" if batch else "local",
        "run": _run,
        "task_cmd_additional_args": (
            ["--script", shlex.quote(exec_script), "--output-file", shlex.quote(output)]
            + (["--input-file", shlex.quote(input_file)] if input_file is not None else [])
            + (["--force"] if force else [])
            # Always explicit (never omitted): the worker's own default must never
            # decide this, or a submission-side flip would silently change where
            # the batch job writes its output.
            + (["--literal-path"] if literal_path else ["--no-literal-path"])
            + [arg for e in extra_args for arg in ("--extra-arg", shlex.quote(e))]
            # Emitted only when explicitly given. Unlike --literal-path, this is an
            # environment-dependent path: encoding the resolved default would bake the
            # submission host's sys.executable into the worker command, which does not
            # resolve inside a different container image. Unset means "use the worker's own".
            + (["--executable", shlex.quote(executable)] if executable else [])
        ),
    }

    if env_script is not None:
        attrs["env_script"] = os.path.abspath(env_script)

    if not force:

        def _output(self):
            if literal_path:
                yield {output: b2luigi.LocalTarget(abs_output)}
            else:
                yield self.add_to_output(output)

        attrs["output"] = _output

    return type("FastTask", (b2luigi.Task,), attrs)


def test_task(
    exec_script: str,
    output: str,
    input_file: str | None,
    force: bool,
    batch: bool,
    extra_args: list[str],
    env_script: str | None = None,
    settings: list[str] | None = None,
    literal_path: bool = True,
) -> None:
    """Run a one-off b2luigi task that executes *exec_script* as a subprocess.

    Builds ``FastTask`` (and optionally a prerequisite ``FastReqTask``) via
    :func:`_build_fast_task` and :func:`_build_fast_req_task`, then runs them
    via :func:`run_luigi`. Arms the ``__batch_runner_use_cli`` setting so that,
    if ``batch=True`` and the task is submitted to a real cluster,
    :func:`~b2luigi.core.utils.create_cmd_from_task` emits the new
    ``batch-runner --script`` reconstruction command instead of the legacy
    argparse invocation. Exits non-zero when any task fails so that the CLI
    binary propagates the failure to the shell.

    :param exec_script: Path to the Python script to run.
    :type exec_script: str
    :param output: Output filename for the task target.
    :type output: str
    :param input_file: Optional input filename; if set, a prerequisite task is
        created so Luigi waits for the input before running the main task.
    :type input_file: str | None
    :param force: When ``True``, the task always runs regardless of whether the
        output already exists.
    :type force: bool
    :param batch: When ``True``, submit via batch system (``batch_system="auto"``).
    :type batch: bool
    :param extra_args: Extra CLI arguments forwarded verbatim to the subprocess.
    :type extra_args: list[str]
    :param env_script: Optional path to an environment setup script, forwarded to
        :func:`_build_fast_task`. Only takes effect combined with ``batch=True``;
        see :func:`_build_fast_task` for details.
    :type env_script: str | None
    :param settings: Optional list of ``"key=value"`` strings (JSON-aware, parsed via
        :func:`~b2luigi.cli.utils.parse_kv_params`), applied via
        :func:`~b2luigi.core.settings.set_setting` before the task is built and run.
        Unlike ``settings.json`` (re-read fresh by the batch worker), these
        overrides live only in the submitting process's in-memory settings and
        are **never** forwarded to the batch worker — the worker reconstructs
        ``FastTask`` via ``batch-runner --script`` with no knowledge of
        ``--setting`` values. Safe for submission-side-only settings
        (``apptainer_image``, ``env``, ``env_script``, ``working_dir``); for
        settings both sides must agree on (``result_dir``, ``log_dir``), use
        ``settings.json`` instead. Also cannot override ``batch_system`` or
        ``env_script``, since :func:`_build_fast_task` already sets those as
        class attributes on ``FastTask``, which :func:`~b2luigi.core.settings.get_setting`
        checks before global settings.
    :type settings: list[str] | None
    :param literal_path: Forwarded to :func:`_build_fast_task`. See there for details.
    :type literal_path: bool
    :raises SystemExit: With exit code 1 when any task in the build fails.
    """
    for key, value in parse_kv_params(settings or []).items():
        set_setting(key, value)

    set_setting("__batch_runner_use_cli", True)
    FastTask = _build_fast_task(exec_script, output, input_file, force, batch, extra_args, env_script, literal_path)
    if input_file is not None:
        FastReqTask = _build_fast_req_task(input_file)
        FastTask = b2luigi.requires(FastReqTask)(FastTask)
    success = run_luigi([FastTask()], {"log_level": "INFO"})
    if not success:
        raise SystemExit(1)


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


def run_as_batch_worker(task_list, cli_args):
    """
    Executes a specific task from a list of tasks as a batch worker.

    This function iterates through a list of root tasks and their dependencies
    to find and execute the task specified by the ``cli_args.task_id``. If the task
    is found, it sets up the environment, runs the task, and handles success or
    failure events. If the task is not found, an error is raised.

    :param task_list: A list of tasks to search for the specified task.
    :type task_list: list
    :param cli_args: Command-line arguments containing the ``task_id`` of the task to execute.
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


def run_batched(task_list, kwargs):
    """
    Executes a batch of Luigi tasks with the provided keyword arguments.

    :param task_list: A list of task instances to be executed.
    :type task_list: list
    :param kwargs: A dictionary of additional keyword arguments to pass to the Luigi runner.
    :type kwargs: dict
    """
    run_luigi(task_list, kwargs)


def run_local(task_list, kwargs):
    """
    Executes a list of Luigi tasks locally by setting the batch system to ``local``.

    :param task_list: A list of Luigi task instances to be executed.
    :type task_list: list
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
        Supported keys: ``scheduler_host``, ``scheduler_port``, ``workers``, and any
        argument accepted by :func:`luigi.build`.
    :type kwargs: dict
    :returns: ``True`` if all tasks completed successfully, ``False`` otherwise.
    :rtype: bool
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

    kwargs.setdefault("workers", get_setting("workers", default=1))
    kwargs.setdefault("log_level", "INFO")
    return luigi.build(task_list, **kwargs)


def run_test_mode(task_list, kwargs):
    """
    Executes the given tasks in test mode with local execution enabled.

    This function sets ``_dispatch_local_execution`` (see :obj:`dispatch`) to enable local execution and then
    builds the provided list of tasks using the local scheduler.

    :param task_list: A list of task instances to be executed.
    :type task_list: list
    :param kwargs: Additional keyword arguments to be passed to :obj:`luigi.build`.
    :type kwargs: dict
    """
    set_setting("_dispatch_local_execution", True)
    luigi.build(task_list, log_level="DEBUG", local_scheduler=True, **kwargs)


def get_task_outputs(task):
    """Return the same dict structure as :obj:`get_all_output_files_in_tree` but
    for a single task without traversing its dependency tree.

    Each output-entry dict carries an ``is_local`` key alongside ``exists``,
    ``parameters`` and ``file_name``, recording whether the underlying target
    is a :class:`luigi.LocalTarget`.

    :param task: The task instance whose outputs should be collected.
    :returns: Mapping of output key to list of output-entry dicts.
    :rtype: collections.defaultdict
    """
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
                # Recorded here because the renderer only ever sees this dict — by
                # then the target object is gone and file_name has been abspath'd,
                # so remote-ness is unrecoverable. Positive check: anything that is
                # not demonstrably a local file is treated as remote, so unknown
                # target types default to "no link", the safe direction.
                is_local=isinstance(target, luigi.LocalTarget),
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


def _render_task_outputs(
    task_output_pairs,
    required_by_map: dict[str, list[str]] | None = None,
    details: bool = False,
    paths_only: bool = False,
    links: bool = False,
) -> None:
    """Render a list of (task, output_dict) pairs using Rich.

    Tasks are grouped by class name. By default only the ``Location`` and
    status columns are shown. Pass ``details=True`` to also show the
    ``Output`` key-name column, and (for classes resolving to more than one
    task instance, e.g. via ``ParameterGenerator``) a leftmost ``Params``
    column distinguishing each instance's rows.

    Long paths are folded rather than truncated: the ``Location`` column never
    discards characters. When a panel's natural width exceeds the space available,
    that panel gains horizontal rules between rows so folded rows stay readable;
    panels whose paths all fit render exactly as they did before.

    :param task_output_pairs: Iterable of ``(task_instance, {key: [{"file_name": ..., "exists": ...}]})``.
    :type task_output_pairs: Iterable[tuple]
    :param required_by_map: Optional mapping of task_id to parent class names,
        produced by :func:`_build_parent_map`. When provided, a ``required by:``
        subtitle is added to each panel listing the union of immediate parent
        class names across all instances in the group.
    :type required_by_map: dict[str, list[str]] | None
    :param details: If ``True``, show the ``Output`` key-name column, and (for
        multi-instance classes) the ``Params`` column. Both are hidden by
        default to keep the common case terse.
    :type details: bool
    :param paths_only: If ``True``, print one bare output path per line with no
        panel, status column, or styling, and return without rendering a table.
        Intended for shell substitution and piping. ``details`` is ignored.
    :type paths_only: bool
    :param links: If ``True``, wrap local output paths in OSC 8 hyperlinks so
        supporting terminals can open them. Remote targets are never linked, and
        links are suppressed entirely under ``paths_only``. Note the link resolves
        against the machine the terminal runs on, so it will not find the file when
        viewing over SSH — which is why this is opt-in.
    :type links: bool
    """
    if paths_only:
        # Deliberately plain print(), not console.print(): Rich would wrap long
        # paths, which breaks $(...) substitution and pipes. Same reasoning as
        # graph --format dot.
        for _, outputs in task_output_pairs:
            for entries in outputs.values():
                for entry in entries:
                    print(entry["file_name"])
        return

    from rich.table import Table
    from rich.panel import Panel
    from rich import box as rich_box
    from rich.text import Text
    from rich.style import Style

    groups: dict[str, list] = {}
    for task, outputs in task_output_pairs:
        groups.setdefault(task.__class__.__name__, []).append((task, outputs))

    for class_name, pairs in groups.items():
        multi = len(pairs) > 1
        show_params = details and multi

        table = Table(show_header=True, header_style="bold", show_lines=False, box=None)
        if show_params:
            table.add_column("Params", style="dim")
        if details:
            table.add_column("Output", style="dim")
        table.add_column("Location", overflow="fold")
        table.add_column("", justify="center", no_wrap=True)

        for task, outputs in pairs:
            params_str = ""
            if show_params:
                serialized = get_serialized_parameters(task)
                params_str = ", ".join(f"{k}={v}" for k, v in serialized.items())
            for key, entries in outputs.items():
                for entry in entries:
                    status = "[green]✓[/green]" if entry["exists"] else "[red]✗[/red]"
                    row = []
                    if show_params:
                        row.append(params_str)
                    if details:
                        row.append(key)
                    # Built as a Text object on every path, not just under --links: Text
                    # is never markup-parsed, so a path segment that merely looks like
                    # Rich markup (e.g. a serialized list parameter containing brackets)
                    # can never be misinterpreted or silently dropped.
                    location = Text(entry["file_name"])
                    if links and entry.get("is_local", False):
                        # as_uri() percent-encodes the path (spaces, brackets, etc.) so the
                        # link target is a valid file:// URI per RFC 3986/8089, without
                        # altering the displayed text above. Safe only because file_name is
                        # always absolute (get_task_outputs stores os.path.abspath(...)) —
                        # as_uri() raises ValueError on a relative path.
                        location.stylize(Style(link=pathlib.Path(entry["file_name"]).as_uri()))
                    row += [location, status]
                    table.add_row(*row)

        subtitle = None
        if required_by_map is not None:
            parents_seen: list[str] = []
            for task, _ in pairs:
                for parent in required_by_map.get(task.task_id, []):
                    if parent not in parents_seen:
                        parents_seen.append(parent)
            if parents_seen:
                subtitle = f"[dim]required by: {', '.join(parents_seen)}[/dim]"

        # A folded path spans several lines, and without separators the rows of
        # adjacent outputs run together. Ask Rich for the table's natural width and
        # only turn rules on when it exceeds what the panel can offer (the console
        # width less the panel's two borders and two padding columns). show_lines is
        # a no-op under box=None, so the box style has to change with it.
        if console.measure(table).maximum > console.width - 4:
            table.box = rich_box.HORIZONTALS
            table.show_lines = True

        console.print(Panel(table, title=f"[bold]{class_name}[/bold]", subtitle=subtitle, border_style="cyan"))


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

    def _label(task: Any) -> str:
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

    def _add_node(parent: Tree, task: Any) -> None:
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


def render_graph_dot(task_list: list, show_params: bool = False, show_status: bool = False) -> None:
    """Render the task dependency graph as Graphviz DOT output to stdout.

    Emits valid DOT syntax via :func:`print` (not the Rich console) so the
    output can be piped directly to ``dot -Tpng -o graph.png``.  Each task
    instance is a node keyed by its ``task_id``; edges follow
    ``task.requires()``.  Shared nodes (required by multiple parents) are
    represented as a single DOT node with multiple incoming edges.

    :param task_list: Root task instances to render.
    :type task_list: list
    :param show_params: If ``True``, include parameter values in each node's
        label.
    :type show_params: bool
    :param show_status: If ``True``, check output existence and colour each
        node green (complete) or red (incomplete).
    :type show_status: bool
    """
    import luigi.task

    nodes: dict[str, str] = {}  # task_id -> DOT attribute string
    edges: list[tuple[str, str]] = []
    visited: set[str] = set()

    def _collect(task: Any) -> None:
        if task.task_id in visited:
            return
        visited.add(task.task_id)

        label = task.__class__.__name__
        if show_params:
            serialized = get_serialized_parameters(task)
            if serialized:
                pairs = "\\n".join(f"{k}={v}" for k, v in serialized.items())
                label += f"\\n{pairs}"

        safe_label = label.replace('"', '\\"')
        attrs = [f'label="{safe_label}"']
        if show_status:
            outputs = luigi.task.flatten(task.output())
            if outputs:
                complete = all(t.exists() for t in outputs)
                attrs.append(f'style=filled, fillcolor={"green" if complete else "red"}')

        nodes[task.task_id] = ", ".join(attrs)

        for req in luigi.task.flatten(task.requires()):
            edges.append((task.task_id, req.task_id))
            _collect(req)

    for root_task in task_list:
        _collect(root_task)

    print("digraph {")
    print('    rankdir="TB";')
    for task_id, attrs in nodes.items():
        safe_id = task_id.replace('"', '\\"')
        print(f'    "{safe_id}" [{attrs}];')
    for parent_id, child_id in edges:
        safe_parent = parent_id.replace('"', '\\"')
        safe_child = child_id.replace('"', '\\"')
        print(f'    "{safe_parent}" -> "{safe_child}";')
    print("}")


def show_task_outputs(task_list: list, details: bool = False, paths_only: bool = False, links: bool = False) -> None:
    """Show output files for the given tasks only — no dependency-tree traversal.

    :param task_list: Task instances whose outputs should be displayed.
    :type task_list: list
    :param details: If ``True``, show the ``Output`` key-name column (and, for
        multi-instance classes, the ``Params`` column). Hidden by default.
    :type details: bool
    :param paths_only: If ``True``, print one bare output path per line instead
        of rendering a table. Suitable for piping.
    :type paths_only: bool
    :param links: If ``True``, wrap local output paths in clickable terminal
        hyperlinks. Ignored under ``paths_only``.
    :type links: bool
    """
    _render_task_outputs(
        ((task, get_task_outputs(task)) for task in task_list),
        details=details,
        paths_only=paths_only,
        links=links,
    )


def show_all_outputs(
    task_list: list,
    show_required_by: bool = False,
    details: bool = False,
    paths_only: bool = False,
    links: bool = False,
) -> None:
    """Show output files for all tasks in the dependency trees rooted at ``task_list``.

    :param task_list: Root task instances; the full dependency tree is traversed.
    :type task_list: list
    :param show_required_by: If ``True``, annotate each requirement panel with
        a ``required by:`` subtitle listing immediate parent class names.
    :type show_required_by: bool
    :param details: If ``True``, show the ``Output`` key-name column (and, for
        multi-instance classes, the ``Params`` column). Hidden by default.
    :type details: bool
    :param paths_only: If ``True``, print one bare output path per line instead
        of rendering a table. Suitable for piping.
    :type paths_only: bool
    :param links: If ``True``, wrap local output paths in clickable terminal
        hyperlinks. Ignored under ``paths_only``.
    :type links: bool
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
    _render_task_outputs(pairs, required_by_map=parent_map, details=details, paths_only=paths_only, links=links)


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


def _confirm_and_remove(
    to_be_removed_tasks: dict,
    target_tasks: list[str],
    matched_target_tasks: set,
    auto_confirm: bool = False,
    keep_tasks: list[str] | None = None,
) -> None:
    """Confirm removal of tasks and execute the removal (shared logic).

    Helper function called by both :func:`remove_outputs` and
    :func:`legacy_remove_outputs` after they have computed the set of tasks
    to be removed. Handles keep-task filtering, confirmation prompt,
    and the actual removal loop.

    :param to_be_removed_tasks: Mapping of task class names to sets of task
        instances to remove.
    :type to_be_removed_tasks: dict
    :param target_tasks: The original target task class names (for computing
        unseen tasks).
    :type target_tasks: list[str]
    :param matched_target_tasks: Task class names that were found in the
        graph.
    :type matched_target_tasks: set
    :param auto_confirm: If ``True``, skip confirmation prompt.
    :type auto_confirm: bool
    :param keep_tasks: List of task class names to KEEP outputs for.
    :type keep_tasks: list[str] | None
    :raises SystemExit: Always raises ``SystemExit(0)`` on completion.
    """
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


def remove_outputs(
    task_list: list,
    target_tasks: list[str],
    auto_confirm: bool = False,
    keep_tasks: list[str] | None = None,
) -> None:
    """Remove the outputs of the specified tasks (new ``b2luigi remove`` CLI).

    Removes only the explicitly named tasks — no dependents cascade. For
    downward removal of a task's requirements, see
    :func:`remove_requirement_outputs` (``--with-requirements``). For the
    legacy dependents-cascade behaviour of ``python tasks.py --remove``, see
    :func:`legacy_remove_outputs`.

    :param task_list: A list of root tasks to traverse.
    :type task_list: list
    :param target_tasks: Task class names whose outputs should be removed.
    :type target_tasks: list[str]
    :param auto_confirm: If ``True``, skip confirmation prompt.
    :type auto_confirm: bool
    :param keep_tasks: List of task class names to KEEP outputs for.
    :type keep_tasks: list[str] | None
    :raises SystemExit: Always raises ``SystemExit(0)`` on completion.
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

    _confirm_and_remove(to_be_removed_tasks, target_tasks, matched_target_tasks, auto_confirm, keep_tasks)


def _collect_all_dependents(task: Any, child_to_parents: dict) -> set:
    """Walk up the DAG from *task* to collect every task that depends on it.

    :param task: The task to start the upward walk from.
    :type task: Any
    :param child_to_parents: Mapping of task instance to the set of task
        instances that directly ``require()`` it.
    :type child_to_parents: dict
    :returns: *task* itself plus every task that transitively requires it.
    :rtype: set
    """
    visited: set = set()
    stack = [task]
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        stack.extend(child_to_parents.get(current, []))
    return visited


def legacy_remove_outputs(
    task_list: list,
    target_tasks: list[str],
    only: bool = False,
    auto_confirm: bool = False,
    keep_tasks: list[str] | None = None,
) -> None:
    """Remove the outputs of specified tasks (legacy ``python tasks.py --remove`` path).

    Backs the legacy ``--remove``/``--remove-only`` CLI flags parsed by
    :func:`~b2luigi.cli.arguments.get_cli_arguments`, and the ``remove=``/
    ``remove_only=`` kwargs to :func:`b2luigi.process`, for users who call
    ``process()`` directly from their own scripts instead of using the new
    ``b2luigi remove`` CLI (see :func:`remove_outputs`). Kept only for
    backward compatibility — a candidate for removal in a future major
    version.

    Unlike :func:`remove_outputs`, ``only`` has real effect here: when
    ``False`` (the default, matching ``--remove``), removal cascades to
    every task that transitively depends on (requires, directly or
    indirectly) each named target task. When ``True`` (``--remove-only``),
    only the named task(s) are removed.

    :param task_list: A list of root tasks to traverse.
    :type task_list: list
    :param target_tasks: Task class names whose outputs should be removed.
    :type target_tasks: list[str]
    :param only: If ``True``, remove only the named tasks. If ``False``,
        also remove outputs of every task that depends on them.
    :type only: bool
    :param auto_confirm: If ``True``, skip confirmation prompt.
    :type auto_confirm: bool
    :param keep_tasks: List of task class names to KEEP outputs for.
    :type keep_tasks: list[str] | None
    :raises SystemExit: Always raises ``SystemExit(0)`` on completion.
    """
    all_tasks: set = set()
    task_by_class: collections.defaultdict = collections.defaultdict(set)
    child_to_parents: collections.defaultdict = collections.defaultdict(set)

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
            child_to_parents[child].add(task)
            visit(child)

    for root in task_list:
        visit(root)

    to_be_removed_tasks: collections.defaultdict = collections.defaultdict(set)
    matched_target_tasks: set = set()

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
                    for dependent in _collect_all_dependents(task, child_to_parents):
                        to_be_removed_tasks[dependent.__class__.__name__].add(dependent)

    _confirm_and_remove(to_be_removed_tasks, target_tasks, matched_target_tasks, auto_confirm, keep_tasks)


def remove_requirement_outputs(
    task_list: list, auto_confirm: bool = False, keep_tasks: list[str] | None = None
) -> None:
    """Remove outputs for the given tasks and all tasks they transitively require.

    Traverses the full dependency tree downward from each task in ``task_list``
    via :func:`task_iterator` and removes outputs for every discovered task.

    :param task_list: Task instances to start traversal from.
    :type task_list: list
    :param auto_confirm: If ``True``, skip confirmation prompt.
    :type auto_confirm: bool
    :param keep_tasks: List of task class names to KEEP outputs for.
    :type keep_tasks: list[str] | None
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

    if keep_tasks:
        keep_tasks_set = set(keep_tasks)
        kept_classes = {t.__class__.__name__ for t in flat_list} & keep_tasks_set
        for keep_class in sorted(kept_classes):
            console.print(f"[yellow]Keeping {keep_class} outputs.[/yellow]")
        if kept_classes:
            console.print()
        flat_list = [t for t in flat_list if t.__class__.__name__ not in keep_tasks_set]

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


def render_task_list(entries: list[tuple[type, str]]) -> None:
    """Render a Rich table listing all available task classes.

    Wraps the table in a ``b2luigi``-branded panel and prints it to stdout.

    :param entries: ``(task class, module label)`` pairs to display.
    :type entries: list[tuple[type, str]]
    """
    from rich.panel import Panel
    from rich.table import Table

    table = Table(title="Available Tasks", show_lines=False)
    table.add_column("Task", style="bold")
    table.add_column("Module", style="dim")
    table.add_column("Description")

    for cls, module_label in entries:
        doc = (getattr(cls, "__doc__", "") or "").strip().splitlines()
        short = doc[0].strip() if doc else ""
        table.add_row(cls.__name__, module_label, short)

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
