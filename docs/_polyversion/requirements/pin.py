"""
Pin the documentation environment of a flit-era minor version (up to v1.2).

Releases packaged with uv carry their own ``uv.lock``. The older ones do not, so
the environment their documentation is built in is pinned here, in one
requirements file per minor version: the runtime dependencies declared by the
given release of it plus ``docs.in``, resolved for the Python version
``policy.py`` assigns to the minor version. Run again to update a file, e.g.
after a theme upgrade on ``main`` that changes the version switcher::

    python docs/_polyversion/requirements/pin.py v1.2.9    # writes v1.2.txt
"""

import subprocess
import sys
import tomllib
from pathlib import Path

HERE = Path(__file__).parent.resolve()
sys.path.insert(0, str(HERE.parent))
import policy  # noqa: E402


def pin(tag: str) -> Path:
    """
    Write the requirements file of the minor version ``tag`` belongs to.

    :param tag: a release tag with flit metadata in its ``pyproject.toml``.
    :return: path of the written file.
    """
    pyproject = subprocess.run(
        ["git", "show", f"{tag}:pyproject.toml"], check=True, capture_output=True, text=True, cwd=HERE
    ).stdout
    requirements = tomllib.loads(pyproject)["tool"]["flit"]["metadata"]["requires"]
    requirements += (HERE / "docs.in").read_text().splitlines()
    pinned = HERE / f"{policy.minor_version(tag)}.txt"
    subprocess.run(
        ["uv", "pip", "compile", "-", "--universal", "--python-version", policy.python_version(tag)]
        + ["--custom-compile-command", f"python docs/_polyversion/requirements/pin.py {tag}", "-o", str(pinned)],
        input="\n".join(requirements),
        check=True,
        text=True,
        stdout=subprocess.DEVNULL,
    )
    return pinned


if __name__ == "__main__":
    for release in sys.argv[1:]:
        print(pin(release))
