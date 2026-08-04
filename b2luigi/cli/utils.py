from dataclasses import dataclass
import difflib
import importlib.util
import inspect
import itertools
import json
import os
from typing import Any, Dict, Generator, List, Optional, Tuple, Type

import luigi
import b2luigi
from b2luigi.cli.errors import CliUserError
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator
from click import Context as ClickContext, Parameter as ClickParameter
from click.shell_completion import CompletionItem


@dataclass(frozen=True)
class Defaults:
    task_file: str
    params_file: str


def resolve_defaults(task_file: str | None, params_file: str | None) -> Defaults:
    task = task_file or os.getenv("B2LUIGI_TASK_FILE") or "tasks.py"
    params = params_file or os.getenv("B2LUIGI_PARAMS_FILE") or "parameters.py"
    return Defaults(task_file=task, params_file=params)


def suggest(bad: str, candidates: List[str]) -> str | None:
    m = difflib.get_close_matches(bad, candidates, n=1, cutoff=0.6)
    return m[0] if m else None


def import_from_file(filename: str, module_name: str) -> Any:
    path = os.path.join(os.getcwd(), filename)  # TODO: Replace getcwd
    if not os.path.exists(path):
        raise CliUserError(
            f"'{filename}' not found in the current directory. Run 'b2luigi init' to create a starter project."
        )

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {filename}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def task_generator(task_file: str = "tasks.py") -> Generator[Tuple[str, Any], Any, None]:
    tasks_module = import_from_file(task_file, "TaskClasses")
    for name, obj in inspect.getmembers(tasks_module):
        yield name, obj


def is_from_task_classes(obj: Any) -> bool:
    return inspect.isclass(obj) and issubclass(obj, b2luigi.Task) and obj.__module__ == "TaskClasses"


def get_task_classnames(task_file: str = "tasks.py") -> list[str]:
    task_names = []
    for name, obj in task_generator(task_file):
        if is_from_task_classes(obj):
            task_names.append(name)

    if not task_names:
        task_names.append("NoTaskFound")
    return sorted(task_names)


def get_task_classes(task_file: str = "tasks.py") -> list[Type[b2luigi.Task]]:
    task_classes = []
    for _, obj in task_generator(task_file):
        if is_from_task_classes(obj):
            task_classes.append(obj)

    if not task_classes:
        raise ValueError("No task classes found in the specified file.")
    return sorted(task_classes, key=lambda cls: cls.__name__)


def load_parameters(filename: str = "parameters.py") -> Dict[str, Any]:
    """Load the parameter configuration from a Python file.

    If *filename* does not exist in the current working directory the
    function returns an empty dict, making ``parameters.py`` optional.

    :param filename: Path (relative to ``os.getcwd()``) of the parameters
        file to load.  Defaults to ``"parameters.py"``.
    :type filename: str
    :returns: The ``config`` dict defined in the file, or ``{}`` when the
        file is absent.
    :rtype: Dict[str, Any]
    :raises AttributeError: If the file exists but does not define a
        ``config`` variable.
    """
    path = os.path.join(os.getcwd(), filename)
    if not os.path.exists(path):
        return {}
    params_module = import_from_file(filename, "user_parameters")
    if not hasattr(params_module, "config"):
        raise AttributeError(f"{filename} must define a 'config' variable")
    return params_module.config


def expand_parameters(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand a ``parameters.py`` config dict into a list of concrete param dicts.

    Detects :class:`~b2luigi.cli.parameter_generator.ParameterGenerator` and
    :class:`~b2luigi.cli.parameter_generator.ZippedParameterGenerator` values
    and expands them:

    - Each :class:`ParameterGenerator` contributes one cartesian slot.
    - Each :class:`ZippedParameterGenerator` contributes one cartesian slot
      whose entries are its zipped pairs.
    - All cartesian slots are crossed via :func:`itertools.product`.
    - Scalar values are merged unchanged into every combination.
    - If no generators are present, returns ``[scalars]`` (single-element list)
      so the caller can treat all cases uniformly.

    :param config: Raw config dict from ``parameters.py``, possibly containing
        generator objects.
    :type config: dict[str, Any]
    :returns: List of fully-resolved parameter dicts, one per combination.
    :rtype: list[dict[str, Any]]
    """
    from b2luigi.cli.parameter_generator import ParameterGenerator, ZippedParameterGenerator

    scalars: dict[str, Any] = {}
    cartesian_slots: list[list[dict[str, Any]]] = []

    for key, value in config.items():
        if isinstance(value, ParameterGenerator):
            cartesian_slots.append([{key: v} for v in value.values])
        elif isinstance(value, ZippedParameterGenerator):
            cartesian_slots.append([dict(zip(value.pairs.keys(), combo)) for combo in zip(*value.pairs.values())])
        else:
            scalars[key] = value

    if not cartesian_slots:
        return [scalars]

    result: list[dict[str, Any]] = []
    for combo in itertools.product(*cartesian_slots):
        merged: dict[str, Any] = {**scalars}
        for part in combo:
            merged.update(part)
        result.append(merged)
    return result


def load_task_class(class_name: str, filename="tasks.py") -> Type[b2luigi.Task]:
    tasks_module = import_from_file(filename, "TaskClasses")
    if not hasattr(tasks_module, class_name):
        raise AttributeError(f"Class '{class_name}' not found in {filename}")
    return getattr(tasks_module, class_name)


def try_instantiate(cls: Type[b2luigi.Task], params: Dict[str, Any]) -> Optional[b2luigi.Task]:
    """Try to instantiate ``cls`` using only the params it declares.

    Filters ``params`` to keys declared by ``cls``, then attempts instantiation.
    Returns the instance on success, or ``None`` if a required parameter
    (one without a default) is not present in the filtered params.

    :param cls: The task class to instantiate.
    :type cls: Type[b2luigi.Task]
    :param params: The full merged params dict (may contain keys for other classes).
    :type params: Dict[str, Any]
    :returns: A task instance, or ``None`` if required params are missing.
    :rtype: Optional[b2luigi.Task]
    """
    accepted = {name for name, _ in cls.get_params()}
    filtered = {k: v for k, v in params.items() if k in accepted}
    try:
        return cls(**filtered)
    except luigi.parameter.MissingParameterException:
        return None


def build_task_list(
    target_names: list[str],
    available: dict[str, Type[b2luigi.Task]],
    param_dicts: list[dict[str, Any]],
    direct_mode: bool,
) -> tuple[list[b2luigi.Task], set[str]]:
    """Build the list of task instances for ``show`` and ``remove``.

    Selects between a direct instantiation path (fast, no graph traversal)
    and a discovery path (traverses from all instantiatable root tasks).

    ``param_dicts`` is the output of :func:`expand_parameters` — a list of
    fully-resolved concrete parameter dicts (one per generator combination).
    Every class is attempted against every dict; duplicates are suppressed by
    ``task_id``.

    Path selection:

    - All named targets directly resolvable: returns only those instances
      (no traversal needed).
    - Any named target unresolvable in ``direct_mode``: returns an empty
      list and the set of unresolved names for the caller to raise an error.
    - Any named target unresolvable without ``direct_mode``: falls back to
      all instantiatable roots so the caller can discover the target via
      graph traversal.

    :param target_names: Task class names the caller wants to act on.
    :type target_names: list[str]
    :param available: Mapping of class name to class for all classes in tasks.py.
    :type available: dict[str, Type[b2luigi.Task]]
    :param param_dicts: Expanded parameter dicts from :func:`expand_parameters`.
    :type param_dicts: list[dict[str, Any]]
    :param direct_mode: If ``True``, never fall back to graph traversal.
    :type direct_mode: bool
    :returns: ``(task_list, unresolved)`` — task instances for the runner and
        any target names that could not be directly instantiated.
    :rtype: tuple[list[b2luigi.Task], set[str]]
    """

    def _all_roots() -> list[b2luigi.Task]:
        seen: set[str] = set()
        result: list[b2luigi.Task] = []
        for cls in available.values():
            for pd in param_dicts:
                inst = try_instantiate(cls, pd)
                if inst is not None and inst.task_id not in seen:
                    seen.add(inst.task_id)
                    result.append(inst)
        return result

    direct_instances: list[b2luigi.Task] = []
    seen: set[str] = set()
    unresolved: set[str] = set()
    for name in target_names:
        cls = available.get(name)
        if cls is None:
            unresolved.add(name)
            continue
        found_any = False
        for pd in param_dicts:
            inst = try_instantiate(cls, pd)
            if inst is not None and inst.task_id not in seen:
                seen.add(inst.task_id)
                direct_instances.append(inst)
                found_any = True
        if not found_any:
            unresolved.add(name)

    if not unresolved:
        return direct_instances, set()

    if direct_mode:
        return [], unresolved

    return find_tasks_in_tree(set(target_names), _all_roots()), set()


def find_tasks_in_tree(
    target_names: set[str],
    root_tasks: list[b2luigi.Task],
) -> list[b2luigi.Task]:
    """Walk the dependency tree rooted at ``root_tasks`` and return instances matching ``target_names``.

    Traverses downward through :func:`task_iterator` for each root task, collecting
    instances whose class name is in ``target_names``. Deduplicates by ``task_id``.

    When the result is empty the caller proceeds with an empty task list — ``show``
    renders nothing and ``remove`` removes nothing (silent no-op).

    :param target_names: Set of task class names to search for.
    :type target_names: set[str]
    :param root_tasks: Root task instances to start traversal from.
    :type root_tasks: list[b2luigi.Task]
    :returns: Deduplicated list of matching task instances, or ``[]`` if none found.
    :rtype: list[b2luigi.Task]
    """
    seen: set[str] = set()
    result: list[b2luigi.Task] = []
    for root in root_tasks:
        for task in task_iterator(root):
            if task.__class__.__name__ in target_names and task.task_id not in seen:
                seen.add(task.task_id)
                result.append(task)
    return result


def get_task_instance(
    class_name: str,
    task_filename="tasks.py",
    parameters_file="parameters.py",
    overrides: Optional[Dict[str, object]] = None,
) -> b2luigi.Task:
    params = load_parameters(parameters_file)
    TaskClass = load_task_class(class_name, task_filename)
    if overrides:
        params.update(overrides)
    return TaskClass(**params)


def get_root_tasks(task_list: List) -> List:
    """Return only the tasks in ``task_list`` that are not required by any other task in the list.

    Passing these as traversal roots to ``get_all_output_files_in_tree`` ensures
    each task's outputs appear exactly once.
    """
    required_ids = set()
    for task in task_list:
        for dep in luigi.task.flatten(task.requires()):
            required_ids.add(dep.task_id)
    return [t for t in task_list if t.task_id not in required_ids]


def process_task_instance(task_instance: Any, task_file: str | None = None, **kwargs) -> None:
    """Arm internal batch-runner settings and dispatch the task via :func:`b2luigi.process`.

    Sets ``__batch_runner_use_cli`` so that :func:`~b2luigi.core.utils.create_cmd_from_task`
    emits the new ``batch-runner --classname`` format when submitting to a batch system.
    When *task_file* is given, also sets ``__batch_runner_task_file`` so the worker command
    includes ``--task-file <path>`` and the batch-runner app can locate the task class.

    :param task_instance: The task instance to execute.
    :param task_file: Absolute path to the task-definitions file.  Pass the result of
        ``os.path.abspath(task_filename)`` from the calling CLI app.  ``None`` suppresses
        ``--task-file`` in the worker command (batch-runner falls back to ``tasks.py``).
    :type task_file: str | None
    :param kwargs: Additional keyword arguments forwarded to :func:`b2luigi.process`.
    """
    set_setting("__batch_runner_use_cli", True)
    if task_file is not None:
        set_setting("__batch_runner_task_file", task_file)
    b2luigi.process(task_instance, ignore_additional_command_line_args=True, **kwargs)


def parse_classnames(raw: str | None) -> list[str] | None:
    """Split a comma-separated class name string into a list, or return ``None`` if empty.

    Whitespace-only tokens and blank entries are stripped and ignored.  If every
    token is blank (e.g. ``"  ,  ,  "``), the function returns ``None`` rather
    than an empty list so that callers can rely on a simple ``if names is None``
    check.

    :param raw: Comma-separated task class names, or ``None``.
    :type raw: str | None
    :returns: List of stripped, non-empty names, or ``None`` if ``raw`` is falsy
        or contains only whitespace.
    :rtype: list[str] | None
    """
    if not raw:
        return None
    names = [n.strip() for n in raw.split(",") if n.strip()]
    return names if names else None


def validate_classnames(
    names: list[str],
    available: dict[str, Any],
    hint_cmd: str = "b2luigi tasks",
) -> None:
    """Raise :class:`CliUserError` for any name not present in ``available``.

    :param names: Task class names to validate.
    :type names: list[str]
    :param available: Mapping of class name to class, from :func:`get_task_classes`.
    :type available: dict[str, Any]
    :param hint_cmd: The CLI command shown in the error message to help the user
        discover available tasks.  Defaults to ``"b2luigi tasks"``.
    :type hint_cmd: str
    :raises CliUserError: If any name is unknown, with a typo suggestion when possible.
    """
    for name in names:
        if name not in available:
            suggestion = suggest(name, list(available))
            msg = f"Unknown task '{name}'."
            if suggestion:
                msg += f" Did you mean '{suggestion}'?"
            msg += f" Use '{hint_cmd}' to see available tasks."
            raise CliUserError(msg)


def complete_task_names(ctx: ClickContext, _param: ClickParameter, incomplete: str) -> list[CompletionItem]:
    """Shell completion callback that returns task class names matching *incomplete*.

    Loaded from :func:`get_task_classnames` using the task file resolved from
    ``ctx.params["task_filename"]``, the ``B2LUIGI_TASK_FILE`` env var, or the
    default ``"tasks.py"``.  Any exception during discovery is silently ignored
    so that a missing or broken ``tasks.py`` never breaks tab completion.

    :param ctx: The current Click context (provides already-parsed params).
    :type ctx: click.Context
    :param _param: The Click parameter being completed (unused).
    :type _param: click.Parameter
    :param incomplete: The partial string the user has typed so far.
    :type incomplete: str
    :returns: List of :class:`~click.shell_completion.CompletionItem` whose
        values start with *incomplete*.
    :rtype: list[CompletionItem]
    """
    task_file = ctx.params.get("task_filename") or os.getenv("B2LUIGI_TASK_FILE", "tasks.py")
    try:
        # get_task_classnames returns ["NoTaskFound"] when the file has no tasks; exclude it from completions
        return [
            CompletionItem(name)
            for name in get_task_classnames(task_file)
            if name.startswith(incomplete) and name != "NoTaskFound"
        ]
    except Exception:
        return []


def split_kv_params(items: List[str]) -> Dict[str, str]:
    """Split a list of ``key=value`` strings into a dict of raw strings.

    Performs no type coercion and no value whitespace stripping: the value is
    returned byte-exact.  Use this when the values are destined for
    :meth:`luigi.Task.from_str_params`, which parses each value through the
    owning :class:`luigi.Parameter` and must therefore receive the value
    exactly as it was serialised — stripping here would silently change the
    reconstructed ``task_id`` for any :class:`luigi.Parameter` whose value
    legitimately carries leading/trailing whitespace.

    :param items: List of ``"key=value"`` strings.
    :type items: List[str]
    :returns: Dict mapping parameter names to their raw, unstripped string values.
    :rtype: Dict[str, str]
    :raises CliUserError: If an item is missing ``=`` or the key is empty.
    """
    out: Dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise CliUserError(f"Invalid --param '{item}'. Use key=value.")
        key, raw = item.split("=", 1)
        key = key.strip()
        if not key:
            raise CliUserError(f"Invalid --param '{item}'. Key is empty.")
        out[key] = raw
    return out


def parse_kv_params(items: List[str]) -> Dict[str, object]:
    """Parse a list of ``key=value`` strings into a dict, typing values as JSON.

    Values are interpreted as JSON when possible (covering integers, floats,
    booleans, lists, dicts, and quoted strings).  Plain strings that are not
    valid JSON are kept as-is.

    Use this for user-facing ``--param`` options, whose values are passed
    directly to a task constructor.  For batch-worker reconstruction use
    :func:`split_kv_params` instead — see its docstring.

    :param items: List of ``"key=value"`` strings.
    :type items: List[str]
    :returns: Dict mapping parameter names to their parsed values.
    :rtype: Dict[str, object]
    :raises CliUserError: If an item is missing ``=`` or the key is empty.
    """
    out: Dict[str, object] = {}
    for key, raw in split_kv_params(items).items():
        raw = raw.strip()
        try:
            out[key] = json.loads(raw)
        except Exception:
            out[key] = raw
    return out
