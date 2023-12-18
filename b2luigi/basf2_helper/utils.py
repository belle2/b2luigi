import os
import warnings


def get_basf2_git_hash():
    """Return name of basf2 release or if local basf2 is used its version hash.

    The version is equivalent to the version returned by ``basf2 --version``.

    Returns ``\"not set\"``, if no basf2 release is set and basf2 cannot be imported.
    """
    basf2_release = os.getenv("BELLE2_RELEASE")

    if basf2_release in ("head", None):
        try:
            import basf2.version

            basf2_release = basf2.version.get_version()

        except ImportError as err:
            warnings.warn(
                f'No basf2 was found. Setting basf2 git hash to "not_set": \n {err}',
                category=ImportWarning,
            )
            basf2_release = "not_set"

    return basf2_release
