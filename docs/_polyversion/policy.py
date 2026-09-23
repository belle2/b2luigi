"""
Versioning policy of the published b2luigi documentation.

This module holds the pure decisions: which git tags are published, how a
revision is named on the website and what the version switcher lists. It is
imported by ``docs/poly.py`` (the ``sphinx-polyversion`` driver) and has no
dependency on ``sphinx-polyversion`` itself so that it can be unit-tested.
"""

import re

#: Public URL of the documentation. Every version lives below it.
BASE_URL = "https://b2luigi.belle2.org/"

#: Name of the branch published as the development version.
DEVELOPMENT_BRANCH = "main"

#: Name under which the development branch is published.
DEVELOPMENT_NAME = "dev"

#: Release tags that stay published no matter how old they are.
PINNED_TAGS = frozenset({"v1.2.9"})  # last release supporting Python 3.8

#: Number of most recent release tags to publish.
KEEP_NEWEST = 2

#: Release tags look like ``v1.2.3``.
TAG_PATTERN = re.compile(r"^v\d+\.\d+\.\d+$")

#: Extra labels shown in the version switcher next to the version name.
LABELS = {"v1.2.9": "last Python 3.8"}


def version_key(tag: str) -> tuple[int, ...]:
    """Sort key for release tags of the form ``vMAJOR.MINOR.PATCH``."""
    return tuple(int(part) for part in tag[1:].split("."))


def is_release_tag(name: str) -> bool:
    """Whether ``name`` is a release tag this policy publishes."""
    return TAG_PATTERN.match(name) is not None


def retained_tags(tags: list[str]) -> list[str]:
    """
    Select the release tags to publish, newest first.

    Keeps the :data:`KEEP_NEWEST` most recent release tags plus every tag in
    :data:`PINNED_TAGS`. Non-release tags are ignored.
    """
    releases = sorted((t for t in tags if is_release_tag(t)), key=version_key, reverse=True)
    newest = set(releases[:KEEP_NEWEST])
    return [t for t in releases if t in newest or t in PINNED_TAGS]


def published_name(ref_name: str) -> str:
    """Name of a git ref on the website: tags keep their name, the development branch becomes ``dev``."""
    return DEVELOPMENT_NAME if ref_name == DEVELOPMENT_BRANCH else ref_name


def newest_release(names: list[str]) -> str | None:
    """The newest release tag among ``names``, or ``None`` if there is none."""
    releases = [n for n in names if is_release_tag(n)]
    return max(releases, key=version_key) if releases else None


def switcher_entries(names: list[str], base_url: str = BASE_URL) -> list[dict[str, object]]:
    """
    Build the entries of the ``pydata-sphinx-theme`` version switcher.

    The newest release is listed first, marked ``preferred`` and served from
    the site root; older releases follow in descending order and the
    development version comes last.

    :param names: published names of all built revisions (tags and ``dev``).
    :param base_url: URL the versions are served under, with a trailing slash.
    """
    releases = sorted((n for n in names if is_release_tag(n)), key=version_key, reverse=True)
    others = [n for n in names if not is_release_tag(n)]
    entries: list[dict[str, object]] = []
    for index, name in enumerate(releases + others):
        stable = index == 0 and is_release_tag(name)
        if stable:
            label = "stable"
        elif name == DEVELOPMENT_NAME:
            label = "unreleased"
        else:
            label = LABELS.get(name, "")
        entry: dict[str, object] = {
            "name": f"{name} ({label})" if label else name,
            "version": name,
            "url": base_url if stable else f"{base_url}{name}/",
        }
        if stable:
            entry["preferred"] = True
        entries.append(entry)
    return entries
