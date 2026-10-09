.. _contrib-polars-label:

``polars``
==========

The ``b2luigi.contrib.polars`` module provides parameters that accept
`polars <https://pola.rs>`_ expressions (:class:`polars.Expr`), either on their own or inside
(nested) lists and dictionaries.
This lets you, e.g., define selections or derived columns as task parameters:

.. code-block:: bash

    pip3 install "b2luigi[polars]"

.. code-block:: python

    import polars as pl
    import b2luigi
    from b2luigi.contrib.polars import PolarsExpressionParameter


    class ApplySelection(b2luigi.Task):
        selection = PolarsExpressionParameter()

        def output(self):
            yield self.add_to_output("selected.parquet")

        def run(self):
            df = pl.read_parquet("input.parquet")
            df.filter(self.selection).write_parquet(self.get_output_file_name("selected.parquet"))


    if __name__ == "__main__":
        b2luigi.process(ApplySelection(selection=pl.col("x") > 0))

The expressions are serialized with ``polars``' binary format, so they can be passed to batch jobs.
As serialized expressions are not readable in a file path, all parameters in this package are hashed by default
(see the ``hashed`` flag described in :func:`wrap_parameter <b2luigi.core.parameter.wrap_parameter>`).

.. caution::
    The binary serialization format of ``polars`` expressions is not guaranteed to be stable across
    ``polars`` versions. Updating ``polars`` may therefore change the hashes, and with them the output
    paths, of tasks with ``polars`` expression parameters.

API Reference
-------------

.. automodule:: b2luigi.contrib.polars.parameters
    :members:
    :show-inheritance:
