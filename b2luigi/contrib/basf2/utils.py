import os
import warnings


def get_basf2_git_hash():
    """
    Retrieve the version or git hash of the ``basf2`` release being used.

    This function determines the version of the ``basf2`` framework in use. If the
    environment variable ``BELLE2_RELEASE`` is set to ``"head"`` or is not defined,
    it attempts to import the ``basf2.version`` module to retrieve the version
    information. If the import fails, a warning is issued, and the version is
    set to ``"not_set"``.

    Returns:
        str: The basf2 release name, its version hash, or "not_set" if basf2
        cannot be imported or no release is configured.

    Warnings:
        ImportWarning: Raised if the `basf2.version` module cannot be imported.
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
