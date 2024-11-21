Super-hero Task Classes
-----------------------

If you want to use the default ``luigi.Task`` class or any derivative of it,
you are totally fine.
No need to change any of your scripts!
But if you want to take advantage of some of the recipies we have developed
to work with large ``luigi`` task sets, you can use the drop in replacements
from the ``b2luigi`` package.
All task classes (except the :class:`b2luigi.DispatchableTask`, see below) are superclasses of
a ``luigi`` class.
As we import ``luigi`` into ``b2luigi``, you just need to replace

.. code-block:: python

    import luigi

with

.. code-block:: python

    import b2luigi as luigi

and you will have all the functionality of ``luigi`` and ``b2luigi``
without the need to change anything!

.. autoclass:: b2luigi.Task
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: b2luigi.ExternalTask
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: b2luigi.WrapperTask
    :members:
    :undoc-members:
    :show-inheritance:

.. autofunction:: b2luigi.dispatch

.. autoclass:: b2luigi.DispatchableTask
    :members: process
    :show-inheritance:
