.. _cli-api-label:

CLI Reference
=============

Command reference
-----------------

.. Keep ``:preferred: text``. The default for the HTML builder is ``svg``, which
   embeds one inline SVG per command: it inflates this page roughly five-fold and
   splits the help text into per-span fragments, so neither browser find nor the
   site search can match a phrase in it. ``:preferred: html`` is worse — against
   this Sphinx/theme combination it emits nothing at all, silently and without a
   build warning.

.. typer:: b2luigi.cli:app
   :prog: b2luigi
   :show-nested:
   :make-sections:
   :preferred: text

Parameter generator classes
----------------------------

.. autoclass:: b2luigi.ParameterGenerator
   :members:

.. autoclass:: b2luigi.ZippedParameterGenerator
   :members:
