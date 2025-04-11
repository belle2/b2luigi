.. _`api-utilities-label`:

Utilities
=========

``requires`` and ``inherits``
-----------------------------

``b2luigi`` provides two functions to help you with the task creation.
The first one is :meth:`b2luigi.requires`, which is a decorator that allows you to
specify the dependencies of a task easily.
It is similar to the ``requires`` method in ``luigi``, but it allows you to
specify the dependencies in a more flexible way.
The second one is :meth:`b2luigi.inherits`, which is a decorator that allows you to
inherit the parameters of a task easily.
It is similar to the ``inherits`` method in ``luigi``, but it allows you to
inherit without specific parameters.

.. autofunction:: b2luigi.requires
.. autofunction:: b2luigi.inherits

Core Utilities
--------------

.. automodule:: b2luigi.core.utils
    :members:
    :show-inheritance:

Executable
----------

.. automodule:: b2luigi.core.executable
    :members:
    :show-inheritance:
