b2luigi
=======

.. image:: https://img.shields.io/badge/sphinx-latest-009682
           :target: https://b2luigi.belle2.org//
.. image:: https://img.shields.io/github/license/belle2/b2luigi
           :target: https://github.com/belle2/b2luigi/blob/main/LICENSE
.. image:: https://img.shields.io/pypi/v/b2luigi?logo=pypi
           :target: https://pypi.python.org/pypi/b2luigi/
.. image:: https://zenodo.org/badge/726161674.svg
	   :target: https://zenodo.org/doi/10.5281/zenodo.10853220


``b2luigi`` is a helper package constructed around ``luigi`` that helps you schedule working packages (so-called tasks)
locally or on a batch system.
Apart from the very powerful dependency management system by ``luigi``, ``b2luigi`` extends the user interface
and has a built-in support for the queue systems, e.g. LSF and HTCondor.

You can find more information in the `documentation <https://b2luigi.belle2.org/>`_.
Please note that most of the core features are handled by ``luigi``, which is described in the
separate `luigi documentation <https://luigi.readthedocs.io/en/latest/>`_,
where you can find a lot of useful information.

If you find any bugs or want to add a feature or improve the documentation, please send me a pull request!
Check the `development documentation <https://b2luigi.belle2.org/advanced/development.html>`_
on information how to contribute.

Contributors are listed `here <https://b2luigi.belle2.org/index.html#the-team>`_.

This project is in still beta. Please be extra cautious when using in production mode.

To get notified about new features, (potentially breaking) changes, bugs and
their fixes, I recommend using the ``Watch`` button on GitHub to get
notifications for new releases and/or issues or to subscribe the `releases feed
<https://github.com/belle2/b2luigi/releases.atom>`_ (requires no GitHub
account, just a feed reader).
