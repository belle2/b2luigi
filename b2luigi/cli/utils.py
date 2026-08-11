from contextlib import contextmanager
from dataclasses import dataclass
import difflib
import importlib
import importlib.util
import inspect
import itertools
import json
import os
import sys
from typing import Any, Dict, Generator, List, Optional, Tuple, Type

import luigi
import b2luigi
from b2luigi.cli.errors import CliUserError, _render_cli_error
from b2luigi.core.settings import set_setting
from b2luigi.core.utils import task_iterator, SYNTHETIC_TASK_MODULE
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


@contextmanager
def cli_error_boundary() -> Generator[None, None, None]:
    """Render a :class:`CliUserError` raised inside the block and exit with its code.

    The console-script entry point (:func:`b2luigi.cli.main`) already catches
    ``CliUserError`` around the whole ``app()`` call. But
    :class:`typer.testing.CliRunner` invokes the Typer app directly, bypassing
    ``main()`` entirely, so a ``CliUserError`` raised there would otherwise
    surface as an uncaught exception (exit code 1, no rendered message)
    instead of its intended exit code. Command callbacks that resolve task
    names in-process (``show``, ``remove``, ``graph``) wrap their body in this
    context manager so both the real CLI and in-process test invocation exit
    identically.

    :yields: Control to the wrapped block.
    :raises SystemExit: With the error's ``exit_code``, after rendering it, if
        a :class:`CliUserError` is raised inside the block.
    """
    try:
        yield
    except CliUserError as e:
        _render_cli_error(str(e))
        raise SystemExit(e.exit_code) from e


def suggest(bad: str, candidates: List[str]) -> str | None:
    m = difflib.get_close_matches(bad, candidates, n=1, cutoff=0.6)
    return m[0] if m else None


def import_from_file(filename: str, module_name: str) -> Any:
    """Load a module from a file and ensure its directory is importable.

    Loads the file as *module_name* and guarantees the file's directory is on
    ``sys.path`` so ``tasks.py`` can import sibling packages.

    :param filename: Path (relative to ``os.getcwd()``) of the module file.
    :type filename: str
    :param module_name: The name to assign to the loaded module.
    :type module_name: str
    :returns: The loaded module object.
    :rtype: Any
    :raises CliUserError: If *filename* is not found in the current directory.
    :raises ImportError: If the module spec cannot be created or loaded.
    """
    path = os.path.join(os.getcwd(), filename)  # TODO: Replace getcwd
    if not os.path.exists(path):
        raise CliUserError(
            f"'{filename}' not found in the current directory. Run 'b2luigi init' to create a starter project."
        )

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {filename}")

    module_dir = os.path.dirname(path)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def task_generator(task_file: str = "tasks.py") -> Generator[Tuple[str, Any], Any, None]:
    tasks_module = import_from_file(task_file, SYNTHETIC_TASK_MODULE)
    for name, obj in inspect.getmembers(tasks_module):
        yield name, obj


def is_from_task_classes(obj: Any) -> bool:
    """Return True if ``obj`` is a task class the CLI should treat as part of the manifest.

    A CLI task is any :class:`b2luigi.Task` subclass present in the task file's
    namespace — defined there or imported into it — except classes belonging to
    the ``b2luigi`` or ``luigi`` packages themselves (e.g. ``from b2luigi import
    Task`` must not turn the base class into a runnable CLI task).

    :param obj: Any object found in the loaded task module's namespace.
    :type obj: Any
    :returns: True if ``obj`` counts as a CLI task class.
    :rtype: bool
    """
    return (
        inspect.isclass(obj)
        and issubclass(obj, b2luigi.Task)
        and obj.__module__.partition(".")[0] not in ("b2luigi", "luigi")
    )


def _iter_task_subclasses(base: Type | None = None) -> Generator[Type, None, None]:
    """Yield every :class:`b2luigi.Task` subclass known to the interpreter.

    Walks ``__subclasses__`` recursively from *base* (default
    :class:`b2luigi.Task`). May yield duplicates under multiple inheritance;
    callers deduplicate by identity.

    :param base: Class to start the walk from, or ``None`` for ``b2luigi.Task``.
    :type base: Type | None
    :returns: Generator of task classes.
    :rtype: Generator[Type, None, None]
    """
    root = base or b2luigi.Task
    for sub in root.__subclasses__():
        yield sub
        yield from _iter_task_subclasses(sub)


def _project_task_classes(task_file: str) -> list[Type]:
    """Collect loaded task classes defined under the task file's directory.

    Importing the task file loads its whole import graph; this walks every
    known :class:`b2luigi.Task` subclass and keeps the ones whose source file
    lies under ``dirname(task_file)``. ``realpath`` is applied to BOTH sides
    (macOS resolves ``/var`` to ``/private/var``) and the containment check
    uses :func:`os.path.commonpath` so ``/proj-other`` never matches ``/proj``.

    **Membership requirement:** Discovered classes must be currently reachable
    as ``getattr(sys.modules[cls.__module__], cls.__name__)``. Stale class
    objects left in Python's ``Task.__subclasses__()`` registry by previous
    imports are filtered in :func:`build_task_index`, not here.

    **Known limitation:** A class whose module-level binding name differs from
    its ``__name__`` (e.g. a factory-created class bound under an alias) is not
    discovered.

    :param task_file: Path of the task file whose directory scopes the project.
    :type task_file: str
    :returns: Task classes defined under the project directory.
    :rtype: list[Type]
    """
    root = os.path.realpath(os.path.dirname(os.path.abspath(task_file)))
    result: list[Type] = []
    seen: set[int] = set()
    for cls in _iter_task_subclasses():
        if id(cls) in seen:
            continue
        seen.add(id(cls))
        if cls.__module__.partition(".")[0] in ("b2luigi", "luigi"):
            continue
        try:
            source = os.path.realpath(inspect.getfile(cls))
        except (TypeError, OSError):
            continue
        try:
            if os.path.commonpath([root, source]) != root:
                continue
        except ValueError:
            continue
        result.append(cls)
    return result


@dataclass(frozen=True)
class TaskIndex:
    """Membership and name resolution for every by-name CLI surface.

    :param manifest: Classes in the task file's namespace, keyed by the name
        they carry there (so an aliased import stays addressable by its alias).
    :type manifest: Dict[str, Type[b2luigi.Task]]
    :param project: Non-manifest classes discovered under the project
        directory, grouped by bare class name (a name can have several
        candidates from different modules).
    :type project: Dict[str, Tuple[Type[b2luigi.Task], ...]]
    :param task_file: The task file this index was built from.
    :type task_file: str
    """

    manifest: Dict[str, Type[b2luigi.Task]]
    project: Dict[str, Tuple[Type[b2luigi.Task], ...]]
    task_file: str

    def all_classes(self) -> list[Type[b2luigi.Task]]:
        """Return the union of manifest and project classes, deterministically sorted.

        :returns: Every addressable class exactly once.
        :rtype: list[Type[b2luigi.Task]]
        """
        union = list(self.manifest.values())
        manifest_ids = {id(cls) for cls in union}
        for candidates in self.project.values():
            union.extend(cls for cls in candidates if id(cls) not in manifest_ids)
        return sorted(union, key=lambda cls: (cls.__name__, cls.__module__))

    def qualified_name(self, cls: Type[b2luigi.Task]) -> str:
        """Return the canonical user-facing name for *cls*.

        Classes defined in the task file itself have the synthetic
        ``TaskClasses`` module and are named bare; everything else is
        ``module.ClassName``.

        :param cls: The task class.
        :type cls: Type[b2luigi.Task]
        :returns: Bare or dotted name.
        :rtype: str
        """
        if cls.__module__ == SYNTHETIC_TASK_MODULE:
            return cls.__name__
        return f"{cls.__module__}.{cls.__name__}"

    def display_module(self, cls: Type[b2luigi.Task]) -> str:
        """Return the module label shown in ``b2luigi tasks``.

        :param cls: The task class.
        :type cls: Type[b2luigi.Task]
        :returns: ``cls.__module__``, or the task file's basename for classes
            defined in the task file (the synthetic name never leaks).
        :rtype: str
        """
        if cls.__module__ == SYNTHETIC_TASK_MODULE:
            return os.path.basename(self.task_file)
        return cls.__module__

    def completion_names(self) -> list[str]:
        """Return every name tab completion should offer.

        Bare names for manifest classes and unique project classes; qualified
        names for every candidate of an ambiguous (or manifest-shadowed) name.

        :returns: Sorted completion candidates.
        :rtype: list[str]
        """
        names: set[str] = set(self.manifest)
        for name, candidates in self.project.items():
            if name not in self.manifest and len(candidates) == 1:
                names.add(name)
            else:
                names.update(self.qualified_name(cls) for cls in candidates)
        return sorted(names)

    def resolve(self, name: str, hint_cmd: str = "b2luigi tasks") -> Type[b2luigi.Task]:
        """Resolve a bare or dotted task name to exactly one class.

        Bare names: the manifest wins outright; otherwise a unique project
        candidate resolves, several candidates raise an ambiguity error, and
        none raises unknown-task. Dotted names match ``module.ClassName``
        exactly across the union, excluding classes defined in the task
        file itself (their synthetic module is never a valid dotted prefix,
        mirroring ``qualified_name``/``display_module``); such classes stay
        addressable only by their bare name.

        :param name: Bare (``DeepTask``) or dotted (``pkg.mod.DeepTask``) name.
        :type name: str
        :param hint_cmd: CLI command named in the unknown-task error.
        :type hint_cmd: str
        :returns: The resolved class.
        :rtype: Type[b2luigi.Task]
        :raises CliUserError: On unknown or ambiguous names.
        """
        if "." in name:
            for cls in self.all_classes():
                if cls.__module__ == SYNTHETIC_TASK_MODULE:
                    continue
                if f"{cls.__module__}.{cls.__name__}" == name:
                    return cls
        else:
            if name in self.manifest:
                return self.manifest[name]
            candidates = self.project.get(name, ())
            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) > 1:
                quals = ", ".join(sorted(self.qualified_name(cls) for cls in candidates))
                raise CliUserError(f"Ambiguous task name '{name}'. Candidates: {quals}. Use the qualified name.")
        suggestion = suggest(name, self.completion_names())
        msg = f"Unknown task '{name}'."
        if suggestion:
            msg += f" Did you mean '{suggestion}'?"
        msg += f" Use '{hint_cmd}' to see available tasks."
        raise CliUserError(msg)

    def resolve_many(self, names: list[str], hint_cmd: str = "b2luigi tasks") -> list[Type[b2luigi.Task]]:
        """Resolve several names, deduplicating by class identity, order-preserving.

        :param names: Bare or dotted task names.
        :type names: list[str]
        :param hint_cmd: CLI command named in unknown-task errors.
        :type hint_cmd: str
        :returns: The resolved classes, each exactly once.
        :rtype: list[Type[b2luigi.Task]]
        :raises CliUserError: On any unknown or ambiguous name.
        """
        resolved: list[Type[b2luigi.Task]] = []
        seen: set[int] = set()
        for name in names:
            cls = self.resolve(name, hint_cmd=hint_cmd)
            if id(cls) not in seen:
                seen.add(id(cls))
                resolved.append(cls)
        return resolved


def build_task_index(task_file: str = "tasks.py") -> TaskIndex:
    """Import the task file and build the :class:`TaskIndex` for it.

    Discovered project classes must be currently reachable as
    ``getattr(sys.modules[cls.__module__], cls.__name__)``. Stale class
    objects from previous imports are excluded via an identity check against
    the live class in sys.modules.

    :param task_file: The task file, relative to the current directory.
    :type task_file: str
    :returns: The populated index.
    :rtype: TaskIndex
    :raises CliUserError: If the task file is missing (via import).
    """
    manifest: Dict[str, Type[b2luigi.Task]] = {}
    for name, obj in task_generator(task_file):
        if is_from_task_classes(obj):
            manifest[name] = obj
    manifest_ids = {id(cls) for cls in manifest.values()}
    grouped: Dict[str, list] = {}
    for cls in _project_task_classes(task_file):
        if id(cls) in manifest_ids:
            continue
        # When a module is re-imported (e.g., from a different tmpdir in tests),
        # the old class objects linger in Task.__subclasses__() even though
        # inspect.getfile(cls) reports the *current* source file. We must verify
        # the class is the live instance in sys.modules, not a stale one.
        if cls.__module__ not in sys.modules:
            continue
        current_cls = getattr(sys.modules[cls.__module__], cls.__name__, None)
        if current_cls is None or id(current_cls) != id(cls):
            continue
        grouped.setdefault(cls.__name__, []).append(cls)
    project = {name: tuple(sorted(candidates, key=lambda cls: cls.__module__)) for name, candidates in grouped.items()}
    return TaskIndex(manifest=manifest, project=project, task_file=task_file)


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


@dataclass(frozen=True)
class TaskContext:
    """Everything the inspection commands need to address a set of tasks.

    :param index: The task index (manifest + project discovery + resolution).
    :type index: TaskIndex
    :param merged_params: The ``parameters.py`` config with ``--param`` overrides
        applied on top, still unexpanded (generator objects intact).
    :type merged_params: Dict[str, Any]
    :param param_dicts: ``merged_params`` expanded into one concrete dict per
        parameter combination.
    :type param_dicts: List[Dict[str, Any]]
    """

    index: TaskIndex
    merged_params: Dict[str, Any]
    param_dicts: List[Dict[str, Any]]


def resolve_task_context(
    task_filename: str | None,
    parameter_filename: str | None,
    params: Optional[List[str]] = None,
) -> TaskContext:
    """Resolve task/parameter files and build the shared task-addressing context.

    This is the common preamble of the inspection commands (``show``, ``remove``
    and ``graph``): resolve the file locations, import the task classes, load the
    parameter config, apply ``--param`` overrides on top of it, and expand the
    result into concrete parameter combinations.

    ``--param`` overrides take precedence over ``parameters.py`` on a per-key
    basis; keys absent from ``params`` keep their configured values.

    .. note::
        ``b2luigi run`` deliberately does not use this helper. It forwards the
        raw overrides to :func:`~b2luigi.cli.apps.run.run_task`, which loads and
        expands the config itself; routing it through here would load
        ``parameters.py`` twice.

    :param task_filename: Explicit task file, or ``None`` to fall back to
        ``$B2LUIGI_TASK_FILE`` and then ``tasks.py``.
    :type task_filename: str | None
    :param parameter_filename: Explicit parameters file, or ``None`` to fall back
        to ``$B2LUIGI_PARAMS_FILE`` and then ``parameters.py``.
    :type parameter_filename: str | None
    :param params: Raw ``key=value`` strings from repeated ``--param`` flags.
    :type params: Optional[List[str]]
    :returns: The resolved classes, merged parameters and expanded combinations.
    :rtype: TaskContext
    """
    d = resolve_defaults(task_filename, parameter_filename)
    index = build_task_index(d.task_file)
    merged_params = {**load_parameters(d.params_file), **parse_kv_params(params or [])}
    return TaskContext(
        index=index,
        merged_params=merged_params,
        param_dicts=expand_parameters(merged_params),
    )


def load_task_class(class_name: str, filename: str = "tasks.py") -> Type[b2luigi.Task]:
    """Resolve a task class by bare or dotted name for execution.

    Used by the batch worker (``b2luigi batch-runner``) and
    :func:`get_task_instance`. Resolution goes through the task index
    (manifest + project discovery). A dotted name not found in the index —
    possible only if its module was not loaded by importing the task file,
    e.g. an import guarded by a condition — falls back to importing the
    module directly; the task file's directory is already on ``sys.path``
    from the index build.

    :param class_name: Bare (``MyTask``) or dotted (``pkg.mod.MyTask``) name.
    :type class_name: str
    :param filename: The task file to load, relative to the current directory.
    :type filename: str
    :returns: The task class.
    :rtype: Type[b2luigi.Task]
    :raises CliUserError: If the name is unknown, ambiguous, or resolves to
        something that is not a b2luigi task class.
    """
    index = build_task_index(filename)
    try:
        return index.resolve(class_name, hint_cmd="b2luigi tasks")
    except CliUserError:
        if "." not in class_name:
            raise
        module_name, _, bare_name = class_name.rpartition(".")
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise CliUserError(
                f"Unknown task '{class_name}': module '{module_name}' could not be imported ({exc})."
            ) from exc
        obj = getattr(module, bare_name, None)
        if obj is None:
            raise CliUserError(f"Unknown task '{class_name}': '{bare_name}' not found in module '{module_name}'.")
        if not is_from_task_classes(obj):
            raise CliUserError(f"'{class_name}' is not a b2luigi task class.")
        return obj


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
    target_classes: list[Type[b2luigi.Task]],
    index: TaskIndex,
    param_dicts: list[dict[str, Any]],
    direct_mode: bool,
) -> tuple[list[b2luigi.Task], list[Type[b2luigi.Task]]]:
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
      list and the classes that could not be directly instantiated, for the
      caller to raise an error.
    - Any named target unresolvable without ``direct_mode``: falls back to
      all instantiatable roots so the caller can discover the target via
      graph traversal.

    :param target_classes: Task classes the caller wants to act on. Resolution
        (bare/dotted name to class) has already happened at the app boundary.
    :type target_classes: list[Type[b2luigi.Task]]
    :param index: The task index for the current project.
    :type index: TaskIndex
    :param param_dicts: Expanded parameter dicts from :func:`expand_parameters`.
    :type param_dicts: list[dict[str, Any]]
    :param direct_mode: If ``True``, never fall back to graph traversal.
    :type direct_mode: bool
    :returns: ``(task_list, unresolved)`` — task instances for the runner and
        any target classes that could not be directly instantiated.
    :rtype: tuple[list[b2luigi.Task], list[Type[b2luigi.Task]]]
    """

    def _all_roots() -> list[b2luigi.Task]:
        seen: set[str] = set()
        result: list[b2luigi.Task] = []
        for cls in index.all_classes():
            for pd in param_dicts:
                inst = try_instantiate(cls, pd)
                if inst is not None and inst.task_id not in seen:
                    seen.add(inst.task_id)
                    result.append(inst)
        return result

    direct_instances: list[b2luigi.Task] = []
    seen: set[str] = set()
    unresolved: list[Type[b2luigi.Task]] = []
    for cls in target_classes:
        found_any = False
        for pd in param_dicts:
            inst = try_instantiate(cls, pd)
            if inst is not None and inst.task_id not in seen:
                seen.add(inst.task_id)
                direct_instances.append(inst)
                found_any = True
        if not found_any:
            unresolved.append(cls)

    if not unresolved:
        return direct_instances, []

    if direct_mode:
        return [], unresolved

    return find_tasks_in_tree(set(target_classes), _all_roots()), []


def find_tasks_in_tree(
    target_classes: set[type],
    root_tasks: list[b2luigi.Task],
) -> list[b2luigi.Task]:
    """Walk the dependency tree rooted at ``root_tasks`` and return instances of ``target_classes``.

    Traverses downward through :func:`task_iterator` for each root task,
    collecting instances whose exact class is in ``target_classes`` (identity,
    not name — two same-named classes from different modules never conflate).
    Deduplicates by ``task_id``.

    :param target_classes: Set of task classes to search for.
    :type target_classes: set[type]
    :param root_tasks: Root task instances to start traversal from.
    :type root_tasks: list[b2luigi.Task]
    :returns: Deduplicated list of matching task instances, or ``[]`` if none found.
    :rtype: list[b2luigi.Task]
    """
    seen: set[str] = set()
    result: list[b2luigi.Task] = []
    for root in root_tasks:
        for task in task_iterator(root):
            if type(task) in target_classes and task.task_id not in seen:
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


def complete_task_names(ctx: ClickContext, _param: ClickParameter, incomplete: str) -> list[CompletionItem]:
    """Shell completion callback that returns task class names matching *incomplete*.

    Loaded from :func:`build_task_index` using the task file resolved from
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
        return [
            CompletionItem(name)
            for name in build_task_index(task_file).completion_names()
            if name.startswith(incomplete)
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
