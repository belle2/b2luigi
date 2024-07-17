.. _`api-documentation-label`:

API Documentation
=================

``b2luigi`` summarizes different topics to help you in your everyday task
creation and processing.
Most important is the :meth:`b2luigi.process` function, which lets you run
arbitrary task graphs on the batch.
It is very similar to ``luigi.build``, but lets you hand in additional parameters
for steering the batch execution.

If you are not yet familiar with ``luigi`` itself, we recommend you to read its current documentation: `Luigi Documentation <https://luigi.readthedocs.io/en/stable/index.html>`_

Top-Level Function
------------------

.. autofunction:: b2luigi.process

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

Parameters
----------

As ``b2luigi`` automatically also imports ``luigi``, you can use all the parameters from ``luigi``
you know and love.
We have just added a single new flag called ``hashed`` to the parameters constructor.
Turning it to true (it is turned off by default) will make ``b2luigi`` use a hashed version
of the parameters value, when constructing output or log file paths.
This is especially useful if you have parameters, which may include "dangerous" characters, like "/" or "{" (e.g.
when using list or dictionary parameters).
If you want to exclude certain parameters from the creation of the directory structure , you can use the ``significant`` flag and set it to ``False``.

See also one of our :ref:`faq-label`.

For more information on ``luigi.Parameters`` and what types there are see `the Luigi Documentation <https://luigi.readthedocs.io/en/stable/parameters.html>`_


Other important features from luigi
-----------------------------------

``luigi`` itself already boasts a number of nice features. Check out some highlights:
- `The central scheduler  <https://luigi.readthedocs.io/en/stable/central_scheduler.html>`_
- `The notification system <https://luigi.readthedocs.io/en/stable/api/luigi.notifications.html#module-luigi.notifications>`_
- `communication with tasks <https://luigi.readthedocs.io/en/stable/luigi_patterns.html#sending-messages-to-tasks>`_

Settings
--------

.. autofunction:: b2luigi.get_setting
.. autofunction:: b2luigi.set_setting
.. autofunction:: b2luigi.clear_setting


Other functions
---------------

.. autofunction:: b2luigi.on_temporary_files
.. autofunction:: b2luigi.core.utils.product_dict

.. toctree::
    :maxdepth: 1

    b2luigi.basf2_helper
