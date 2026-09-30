"""
Build the versioned b2luigi documentation with ``sphinx-polyversion``.

Run from anywhere inside the repository::

    sphinx-polyversion docs/poly.py            # every published version
    sphinx-polyversion docs/poly.py --local    # only the working tree, as ``local``

Each revision is checked out into a temporary directory and built with
``sphinx-build`` in its own locked virtual environment, which uv creates with
the Python version ``docs/_polyversion/policy.py`` assigns to the revision.
``docs/_polyversion/prepare.sh`` adds the version switcher to revisions that
predate it. Which versions are published is decided in the policy module, too.

Options can be overridden on the command line, e.g. to serve a build locally::

    sphinx-polyversion docs/poly.py -o BASE_URL=http://localhost:8000/
"""

import asyncio
import os
import shutil
import sys
from asyncio.subprocess import PIPE
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from subprocess import CalledProcessError
from typing import Self

from sphinx_polyversion import DefaultDriver, apply_overrides
from sphinx_polyversion.builder import BuildError
from sphinx_polyversion.git import Git, GitRef, GitRefType, refs_by_type
from sphinx_polyversion.pyvenv import VirtualPythonEnvironment
from sphinx_polyversion.sphinx import Placeholder, SphinxBuilder

HERE = Path(__file__).parent.resolve()
ROOT = Git.root(HERE)
sys.path.insert(0, str(HERE / "_polyversion"))
import policy  # noqa: E402

# Overridable from the command line with ``-o KEY=VALUE``.
BASE_URL = policy.BASE_URL
OUTPUT_DIR = "_docs_build"
MOCK = False
SEQUENTIAL = False
apply_overrides(globals())

SOURCE_DIR = "docs/"
POLYVERSION_DIR = HERE / "_polyversion"


class PublishedGit(Git):
    """Git version provider applying :func:`policy.retained_tags` to the matching refs."""

    async def retrieve(self, root: Path) -> tuple[GitRef, ...]:
        """List the development branch and the retained release tags."""
        branches, tags = refs_by_type(list(await super().retrieve(root)))
        keep = policy.retained_tags([t.name for t in tags])
        return tuple(branches + [t for t in tags if t.name in keep])


def root_data(driver: DefaultDriver) -> dict[str, object]:
    """Variables for the jinja templates rendered into the output root."""
    # a local build lists the mocked revision twice
    names = list(dict.fromkeys(policy.published_name(ref.name) for ref in driver.builds))
    return {"switcher": policy.switcher_entries(names, BASE_URL)}


def copy_newest_release_to_root(output_dir: Path) -> None:
    """Serve the newest release from the site root so that existing deep links keep working."""
    names = [p.name for p in output_dir.iterdir() if p.is_dir()]
    newest = policy.newest_release(names)
    if newest is None:
        return
    shutil.copytree(output_dir / newest, output_dir, dirs_exist_ok=True)


class UvEnvironment(VirtualPythonEnvironment):
    """
    Locked build environment of one revision, created by uv in ``.venv`` of its checkout.

    A revision packaged with uv is installed from its own ``uv.lock``. The flit-era tags up to
    v1.2.9 have none: their environment is pinned per minor version in
    ``<requirements_dir>/<minor version>.txt`` and the package itself is not installed, as their
    ``conf.py`` puts the checkout on ``sys.path``.

    :param path: location of the checked-out revision.
    :param name: published name of the revision.
    :param requirements_dir: directory with the pinned requirements of the flit-era minor versions,
        ``None`` to use the ``uv.lock`` of the revision.
    :param env: environment variables of the build.
    """

    def __init__(
        self, path: Path, name: str, *, requirements_dir: Path | None = None, env: dict[str, str] | None = None
    ) -> None:
        super().__init__(path, name, path / ".venv", env=env)
        self.name = name
        self.requirements_dir = requirements_dir

    async def uv(self, *args: str) -> None:
        """
        Run ``uv`` in the checkout.

        :raises BuildError: if the command fails.
        """
        cmd = ["uv", *args]
        # a venv activated in the calling shell must not leak into the revision's environment
        env = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
        process = await asyncio.create_subprocess_exec(*cmd, cwd=self.path, env=env, stdout=PIPE, stderr=PIPE)
        _, err = await process.communicate()
        if process.returncode != 0:
            self.logger.error("Installation error:\n %s", err.decode(errors="ignore"))
            raise BuildError from CalledProcessError(process.returncode or 1, " ".join(cmd))

    async def __aenter__(self) -> Self:
        """
        Create the environment.

        :raises BuildError: if uv fails, e.g. because a requirements file is missing or a ``uv.lock`` outdated.
        """
        await super().__aenter__()
        python = policy.python_version(self.name)
        venv_python = str(self.venv / "bin" / "python")
        if self.requirements_dir is None:
            # compiling at install time keeps the SyntaxWarnings of third-party packages out of the build log
            await self.uv("sync", "--locked", "--group", "docs", "--python", python, "--compile-bytecode")
        else:
            requirements = self.requirements_dir / f"{policy.minor_version(self.name)}.txt"
            await self.uv("venv", "--python", python, str(self.venv))
            await self.uv("pip", "sync", "--python", venv_python, str(requirements))
        # conf.py reads the data of the driver, so every revision loads it with the driver's version
        polyversion = f"sphinx-polyversion=={version('sphinx-polyversion')}"
        await self.uv("pip", "install", "--python", venv_python, "--no-deps", polyversion)
        return self


def uv_environment(
    requirements_dir: Path | None = None, env: dict[str, str] | None = None
) -> Callable[[Path, str], UvEnvironment]:
    """
    Factory for the locked environment of a revision.

    :param requirements_dir: directory with the pinned requirements of the flit-era minor versions,
        ``None`` to use the ``uv.lock`` of the revision.
    :param env: additional environment variables of the build.
    """
    return UvEnvironment.factory(
        requirements_dir=requirements_dir,
        env={
            "B2LUIGI_DOCS_BASE_URL": BASE_URL,
            # a preview build lists only itself and must work wherever it is served from
            "B2LUIGI_DOCS_SWITCHER_URL": "switcher.json" if MOCK else f"{BASE_URL}switcher.json",
            **(env or {}),
        },
    )


async def select_environment(ref: GitRef, keys: Iterable[str]) -> str:
    """
    Pick how a revision's environment is locked: by its own ``uv.lock`` since the migration
    to uv, by the requirements file of its minor version for the flit-era tags up to v1.2.9.
    """
    if not ref.obj:  # the mocked local revision: the working tree
        pyproject = (ROOT / "pyproject.toml").read_text()
    else:
        proc = await asyncio.create_subprocess_exec(
            "git", "show", f"{ref.obj}:pyproject.toml", stdout=asyncio.subprocess.PIPE, cwd=ROOT
        )
        stdout, _ = await proc.communicate()
        pyproject = stdout.decode()
    return "uv" if "[dependency-groups]" in pyproject else "flit"


def sphinx_builder(verbosity: str) -> SphinxBuilder:
    """Builder running ``sphinx-build`` with the given verbosity flag after ``prepare.sh``."""
    return SphinxBuilder(
        SOURCE_DIR,
        args=[verbosity],
        pre_cmd=["bash", str(POLYVERSION_DIR / "prepare.sh"), Placeholder.SOURCE_DIR],
    )


vcs = PublishedGit(
    branch_regex=f"^{policy.DEVELOPMENT_BRANCH}$",
    tag_regex=policy.TAG_PATTERN.pattern,
)

driver = DefaultDriver(
    ROOT,
    OUTPUT_DIR,
    vcs=vcs,
    # the warnings of a released tag cannot be fixed anymore, so only the development version reports them
    builder={"uv": sphinx_builder("-q"), "flit": sphinx_builder("-Q")},
    env={
        "uv": uv_environment(),
        "flit": uv_environment(POLYVERSION_DIR / "requirements", env={"PYTHONWARNINGS": "ignore"}),
    },
    selector=select_environment,
    namer=lambda ref: policy.published_name(ref.name),
    root_data_factory=root_data,
    template_dir=POLYVERSION_DIR / "templates",
    mock={
        "current": GitRef(policy.DEVELOPMENT_BRANCH, "", "", GitRefType.BRANCH, datetime.now(timezone.utc)),
        "revisions": [],
    },
)
driver.run(MOCK, SEQUENTIAL)
failed = [] if MOCK else [policy.published_name(ref.name) for ref in driver.targets if ref not in driver.builds]
if failed:
    # sphinx-polyversion only logs a failed revision; a website missing versions must not be published
    sys.exit(f"Building the documentation failed for: {', '.join(failed)}")
if MOCK:
    # the preview build fetches the switcher list relative to its own root
    shutil.copy(Path(OUTPUT_DIR) / "switcher.json", Path(OUTPUT_DIR) / "local" / "switcher.json")
else:
    copy_newest_release_to_root(Path(OUTPUT_DIR))
