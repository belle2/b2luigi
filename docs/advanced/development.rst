.. _development-label:

Development and TODOs
=====================

You want to help developing ``b2luigi``? Great! Have your GitHub account ready and let's go!

If you are a Belle II collaborator, you can contribute directly on the
`DESY GitLab <https://gitlab.desy.de/belle2/software/b2luigi>`_.


Local Development
-----------------

You want to help developing ``b2luigi``? Great! Here are some first steps to help you dive in:

1.  Make sure you uninstall ``b2luigi`` if you have installed if from PyPi

    .. code-block:: bash

        python -m pip uninstall b2luigi

2.  Clone the repository from GitHub

    .. code-block:: bash

        git clone https://github.com/belle2/b2luigi

    or, for Belle II collaborators, clone the repository from DESY GitLab

    .. code-block:: bash

        git clone git@gitlab.desy.de:belle2/software/b2luigi.git

3.  ``b2luigi`` is not using ``setuptools`` but the newer (and better) `flit`_ as a a builder.
    Install it via

    .. code-block:: bash

        python -m pip [ --user ] install flit

    You can now install ``b2luigi`` from the cloned git repository in development mode:

    .. code-block:: bash

        flit install -s

    Now you can start hacking and your changes will be immediately available to you.

4.  Install `pre-commit`_, which automatically checks your code

    .. code-block:: bash

        python -m pip [ --user ] install pre-commit
        pre-commit install  # install the pre-commit hooks
        pre-commit run --all-files  # run pre-commit manually

    In particular, the python files are checked with `ruff`_ for syntax and
    `PEP 8`_ style errors. We recommend using an IDE or editor which
    automatically highlights errors with ruff or a similar python linter (e.g. pylint or flake8).

5.  We use the `pytest`_ package for testing some parts of the code. Install it via

    .. code-block:: bash

        python -m pip install -U [ --user ] pytest pytest-cov python-coveralls

    All tests reside in the ``tests/`` sub-directory. To run all tests, run the command

    .. code-block:: bash

        pytest -v b2luigi tests

    in the root of ``b2luigi`` repository. If you add some functionality, try to add some tests for it.

6.  The documentation is hosted on `b2luigi.belle2.org`_ and build automatically on every commit to main.
    You can (and should) also build the documentation locally by installing ``sphinx``, ``sphinx-book-theme``,
    ``sphinx-autobuild`` and few, additional, dependencies (note they should have already been installed
    when running ``flit install -s``):

    .. code-block:: bash

        flit install --only-deps

    And starting the automatic build process in the projects root folder

    .. code-block:: bash

        sphinx-autobuild docs build

    The autobuild will rebuild the project whenever you change something. It displays a URL where to find
    the created docs now (most likely http://127.0.0.1:8000).
    Please make sure the documentation looks fine before creating a pull request.

7.  If you are a core developer and want to release a new version:

    a.  Make sure all changes are committed and merged on main

    b.  Use the `bump-my-version`_ package to update the version in ``b2luigi/__init__.py``,
	``.bumpversion.cfg`` as well as the git tag. ``flit`` will automatically use this.

        .. code-block:: bash

            python3 -m pip install --upgrade bump-my-version
            bump-my-version bump --no-commit [patch|minor|major]

    c.  Push the new commit and the tags

        .. code-block:: bash

            git push
            git push --tags

    d.  Create a new release on `GitLab <https://gitlab.desy.de/belle2/software/b2luigi/-/releases>`_
	and on `GitHub <https://github.com/belle2/b2luigi/releases>`_ with an appropriate description.

    e.  Check that the new release had been published to PyPi, which should happen automatically via
        GitLab `pipeline`_. Alternatively, you can also manually publish a release. Install the dependencies with

        .. code-block:: bash

            python -m pip install -U [ --user ] setuptools wheel twine

        and publish via

        .. code-block:: bash

            flit publish


Open TODOs
----------

For the Belle II collaborators: for a list of potential features, improvements and bugfixes see the
`GitLab issues`_. Help is welcome, so feel free to pick one, e.g. with the ``good first issue`` or
``help wanted`` tags.

.. _flit: https://pypi.org/project/flit/
.. _gitlab issues: https://gitlab.desy.de/belle2/software/b2luigi/-/issues
.. _pytest: https://docs.pytest.org/
.. _b2luigi.belle2.org: https://b2luigi.belle2.org
.. _pre-commit: https://pre-commit.com
.. _ruff: https://docs.astral.sh/ruff/
.. _PEP 8: https://www.python.org/dev/peps/pep-0008/
.. _bump-my-version: https://github.com/callowayproject/bump-my-version
.. _release: https://github.com/belle2/b2luigi/releases
.. _pipeline: https://github.com/belle2/b2luigi/blob/main/.gitlab-ci.yml
.. _Keep a Changelog: https://keepachangelog.com/en/1.0.0/
