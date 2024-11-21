
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
