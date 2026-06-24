"""Parameter generator classes for automatic WrapperTask expansion.

:Description: Provides :class:`ParameterGenerator` and
    :class:`ZippedParameterGenerator` for use in ``parameters.py`` ``config``
    dicts. When detected by the CLI, these trigger automatic expansion into
    a :class:`b2luigi.WrapperTask` covering all parameter combinations.
"""

from typing import Any

from b2luigi.cli.errors import CliUserError


class ParameterGenerator:
    """Declare a list of values for a single parameter to be expanded via cartesian product.

    Place as a value in the ``parameters.py`` ``config`` dict. Multiple
    ``ParameterGenerator`` values in the same config are crossed with each
    other (and with any :class:`ZippedParameterGenerator` groups) to produce
    all combinations.

    Example::

        from b2luigi import ParameterGenerator
        config = {"split": ParameterGenerator([1, 2, 3])}

    :param values: Non-empty list of concrete parameter values.
    :type values: list[Any]
    :raises CliUserError: If ``values`` is empty.
    """

    def __init__(self, values: list) -> None:
        if not values:
            raise CliUserError("ParameterGenerator requires at least one value.")
        self.values: list[Any] = values


class ZippedParameterGenerator:
    """Declare multiple parameters to be expanded in lockstep (zip).

    Place as a value in the ``parameters.py`` ``config`` dict under any
    sentinel key (the key is ignored; parameter names come from keyword
    arguments). Multiple ``ZippedParameterGenerator`` instances are
    cartesian-crossed with each other and with any :class:`ParameterGenerator`
    values.

    Example::

        from b2luigi import ZippedParameterGenerator
        config = {
            "zipped": ZippedParameterGenerator(alpha=[1, 2], beta=["a", "b"]),
        }
        # expands to: (alpha=1, beta="a"), (alpha=2, beta="b")

    :param kwargs: Each keyword argument names a task parameter; its value is
        the list of concrete values for that parameter. All lists must have the
        same length.
    :type kwargs: dict[str, list]
    :raises CliUserError: If no keyword arguments are given, list lengths differ, or all lists are empty.
    """

    def __init__(self, **kwargs: list) -> None:
        if not kwargs:
            raise CliUserError("ZippedParameterGenerator requires at least one keyword argument.")
        lengths = {k: len(v) for k, v in kwargs.items()}
        if len(set(lengths.values())) > 1:
            raise CliUserError(f"ZippedParameterGenerator: all lists must have the same length, got {lengths}.")
        if next(iter(lengths.values()), 1) == 0:
            raise CliUserError("ZippedParameterGenerator requires non-empty lists.")
        self.pairs: dict[str, list] = dict(kwargs)
