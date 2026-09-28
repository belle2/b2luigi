"""
Parameters for handling ``polars`` expressions (:class:`polars.Expr`).

To learn more about ``polars`` expressions, visit the
`polars documentation <https://docs.pola.rs/api/python/stable/reference/expressions/index.html>`_.
"""

import base64
import hashlib
import io
import json
from collections.abc import Mapping

from luigi.freezing import FrozenOrderedDict

import b2luigi
import polars as pl

#: Key of the marker dictionary that wraps a serialized :class:`polars.Expr`.
POLARS_EXPR_KEY = "__polars_expr__"


def serialize_with_polars_value(value):
    """
    Recursively convert a value that may be or may contain ``polars`` expressions into a JSON-serializable structure.

    Each :class:`polars.Expr` is serialized with ``Expr.meta.serialize(format="binary")`` and wrapped in a
    marker dictionary ``{"__polars_expr__": <base64 string>}``, so it can be restored by
    :func:`deserialize_with_polars_value`. Mappings, lists and tuples are traversed recursively,
    all other values are returned unchanged.

    Args:
        value: Any value, e.g. a :class:`polars.Expr`, a mapping, list or tuple, or a scalar.

    Returns:
        A JSON-serializable structure in which all ``polars`` expressions are encoded.
    """
    if isinstance(value, pl.Expr):
        serialized_bytes = value.meta.serialize(format="binary")
        return {POLARS_EXPR_KEY: base64.b64encode(serialized_bytes).decode("utf-8")}
    if isinstance(value, Mapping):
        return {key: serialize_with_polars_value(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_with_polars_value(item) for item in value]
    return value


def deserialize_with_polars_value(value):
    """
    Recursively restore the ``polars`` expressions in a structure created by :func:`serialize_with_polars_value`.

    As JSON has no tuples, sequences are always returned as lists.
    The parameters in this module freeze them to tuples anyway when a task is instantiated.

    Args:
        value: A structure previously produced by :func:`serialize_with_polars_value`.

    Returns:
        The original structure with all :class:`polars.Expr` objects reconstructed.
    """
    if isinstance(value, Mapping):
        if POLARS_EXPR_KEY in value:
            decoded_bytes = base64.b64decode(value[POLARS_EXPR_KEY].encode("utf-8"))
            return pl.Expr.deserialize(io.BytesIO(decoded_bytes), format="binary")
        return {key: deserialize_with_polars_value(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [deserialize_with_polars_value(item) for item in value]
    return value


def deterministic_polars_hash_function(value) -> str:
    """
    Create a deterministic hash for a value that may contain ``polars`` expressions.

    The value is serialized with :func:`serialize_with_polars_value` and the first 16 characters of the
    SHA-256 digest of the (key-sorted) JSON representation are returned.
    This is the default ``hash_function`` of all parameters in this module.

    .. caution::
        The serialization format of ``polars`` expressions is not guaranteed to be stable across
        ``polars`` versions, so the hash (and with it the output path of a task) may change when you
        update ``polars``.
    """
    serialized = json.dumps(serialize_with_polars_value(value), sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


class _PolarsSerializationMixin:
    """
    Shared serialization of the ``polars`` expression parameters.

    Unlike other parameters, ``hashed`` defaults to ``True``, as serialized expressions are not suitable for
    file paths. The hash is computed with :func:`deterministic_polars_hash_function` unless you provide your own
    ``hash_function``.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("hashed", True)
        if kwargs.get("hashed", False) and "hash_function" not in kwargs:
            kwargs["hash_function"] = deterministic_polars_hash_function
        super().__init__(*args, **kwargs)

    def serialize(self, x) -> str:
        """Serialize the value (possibly containing ``polars`` expressions) to a JSON string."""
        return json.dumps(serialize_with_polars_value(x))

    def parse(self, x: str):
        """Parse a JSON string created by :meth:`serialize`, restoring all ``polars`` expressions."""
        return deserialize_with_polars_value(json.loads(x, object_pairs_hook=FrozenOrderedDict))


class PolarsExpressionParameter(_PolarsSerializationMixin, b2luigi.Parameter):
    """
    Parameter for a single ``polars`` expression (:class:`polars.Expr`).

    Unlike other parameters, ``hashed`` defaults to ``True``, as the serialized expression is not suitable for a
    file path. The hash is computed with :func:`deterministic_polars_hash_function` unless you provide your own
    ``hash_function``. Be aware that the serialization of ``polars`` expressions, and therefore the hash,
    might not be stable across different versions of ``polars``.
    The same applies to :class:`PolarsExpressionListParameter` and :class:`PolarsExpressionDictParameter`.

    Example:
        .. code-block:: python

            import polars as pl
            import b2luigi
            from b2luigi.contrib.polars import PolarsExpressionParameter

            class MyTask(b2luigi.Task):
                expr = PolarsExpressionParameter(default=pl.col("x") * 2)

                def run(self):
                    df = pl.DataFrame({"x": [1, 2, 3]})
                    result = df.select(self.expr.alias("double_x"))
                    print(result)
    """


class PolarsExpressionListParameter(_PolarsSerializationMixin, b2luigi.ListParameter):
    """
    Parameter for a list that may contain ``polars`` expressions.

    Like :class:`PolarsExpressionParameter`, it is hashed by default, and the same caveats on hashing apply.

    Example:
        .. code-block:: python

            import polars as pl
            import b2luigi
            from b2luigi.contrib.polars import PolarsExpressionListParameter

            class MyTask(b2luigi.Task):
                filters = PolarsExpressionListParameter(
                    default=[
                        pl.col("x") > 0,
                        pl.col("y").is_not_null(),
                    ],
                )

                def run(self):
                    df = pl.DataFrame({"x": [1, -2, 3], "y": [None, 5, 6]})
                    for filter_expr in self.filters:
                        df = df.filter(filter_expr)
                    print(df)
    """


class PolarsExpressionDictParameter(_PolarsSerializationMixin, b2luigi.DictParameter):
    """
    Parameter for a dictionary whose (possibly nested) values may be ``polars`` expressions.

    This allows you to store rich configurations with :class:`polars.Expr` objects alongside regular values.
    Like :class:`PolarsExpressionParameter`, it is hashed by default, and the same caveats on hashing apply.

    Example:
        .. code-block:: python

            import polars as pl
            import b2luigi
            from b2luigi.contrib.polars import PolarsExpressionDictParameter

            class MyTask(b2luigi.Task):
                config = PolarsExpressionDictParameter(
                    default={
                        "compute": pl.col("x") * 3,
                        "filter": pl.col("y").is_between(0, 10),
                        "settings": {"threshold": 0.95},
                    },
                )

                def run(self):
                    df = pl.DataFrame({"x": [1, 2, 3], "y": [5, 15, 7]})
                    result = df.filter(self.config["filter"]).select(self.config["compute"].alias("triple_x"))
                    print(result)
                    print(self.config["settings"]["threshold"])
    """
