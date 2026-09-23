"""
Build the versioned b2luigi documentation with ``sphinx-polyversion``.

Run from anywhere inside the repository::

    sphinx-polyversion docs/poly.py            # every published version
    sphinx-polyversion docs/poly.py --local    # only the working tree, as ``local``

Each revision is checked out into a temporary directory, installed into its own
virtual environment together with its ``doc`` extra and built with
``sphinx-build``. ``docs/_polyversion/prepare.sh`` adds the version switcher to
revisions that predate it. Which versions are published is decided in
``docs/_polyversion/policy.py``.

Options can be overridden on the command line, e.g. to serve a build locally::

    sphinx-polyversion docs/poly.py -o BASE_URL=http://localhost:8000/
"""

import asyncio
import shutil
import sys
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path

from sphinx_polyversion import DefaultDriver, apply_overrides
from sphinx_polyversion.git import Git, GitRef, GitRefType, refs_by_type
from sphinx_polyversion.pyvenv import Pip, VenvWrapper
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


def pip_environment(install_args: list[str]) -> Callable[[Path, str], Pip]:
    """Factory for a fresh venv per revision, installing the revision with ``install_args``."""
    return Pip.factory(
        venv=Path(".venv"),
        creator=VenvWrapper(),
        temporary=True,
        args=[*install_args, "sphinx-polyversion"],
        env={
            # old tags still declare the flit 3 metadata table
            "PIP_CONSTRAINT": str(POLYVERSION_DIR / "pip-constraints.txt"),
            "B2LUIGI_DOCS_BASE_URL": BASE_URL,
            # a preview build lists only itself and must work wherever it is served from
            "B2LUIGI_DOCS_SWITCHER_URL": "switcher.json" if MOCK else f"{BASE_URL}switcher.json",
        },
    )


async def select_environment(ref: GitRef, keys: Iterable[str]) -> str:
    """
    Pick how a revision's documentation dependencies are installed.

    Since the migration to uv they are a ``docs`` dependency group (PEP 735); tags up to
    v1.2.9 still declare them as a ``doc`` extra of the flit metadata.
    """
    if not ref.obj:  # the mocked local revision: the working tree
        pyproject = (ROOT / "pyproject.toml").read_text()
    else:
        proc = await asyncio.create_subprocess_exec(
            "git", "show", f"{ref.obj}:pyproject.toml", stdout=asyncio.subprocess.PIPE, cwd=ROOT
        )
        stdout, _ = await proc.communicate()
        pyproject = stdout.decode()
    return "dependency-groups" if "[dependency-groups]" in pyproject else "extras"


vcs = PublishedGit(
    branch_regex=f"^{policy.DEVELOPMENT_BRANCH}$",
    tag_regex=policy.TAG_PATTERN.pattern,
)

driver = DefaultDriver(
    ROOT,
    OUTPUT_DIR,
    vcs=vcs,
    builder=SphinxBuilder(
        SOURCE_DIR,
        args=["-q"],
        pre_cmd=["bash", str(POLYVERSION_DIR / "prepare.sh"), Placeholder.SOURCE_DIR],
    ),
    env={
        "dependency-groups": pip_environment(["--group", "docs", "."]),
        "extras": pip_environment([".[doc]"]),
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
if MOCK:
    # the preview build fetches the switcher list relative to its own root
    shutil.copy(Path(OUTPUT_DIR) / "switcher.json", Path(OUTPUT_DIR) / "local" / "switcher.json")
else:
    copy_newest_release_to_root(Path(OUTPUT_DIR))
