.. _development-label:

Development and TODOs
=====================

You want to help developing ``b2luigi``? Great! Have your GitHub or DESY `GitLab`_ account ready and let's go!

If you are a Belle II collaborator, you can contribute directly on the
_.


Local Development
-----------------

You want to help developing ``b2luigi``? Great! Here are some first steps to help you dive in:

1.  Make sure you uninstall ``b2luigi`` if you have installed if from PyPI

    .. code-block:: bash

        python -m pip uninstall b2luigi

2.  Clone the repository from GitHub

    .. code-block:: bash

        git clone https://github.com/belle2/b2luigi

    or, for Belle II collaborators, clone the repository from DESY GitLab

    .. code-block:: bash

        git clone git@gitlab.desy.de:belle2/software/b2luigi.git

3.  Change into the cloned directory

    .. code-block:: bash

        cd b2luigi

    We recommend to work only in Python virtual environments.
    You can create a new virtual environment with

    .. code-block:: bash

        python3 -m venv venv

    and activate it with

    .. code-block:: bash

        source venv/bin/activate

    If you are a Belle II collaborator, you can also use the `b2venv`_ command to create a virtual environment.

3.  ``b2luigi`` is not using ``setuptools`` but the newer (and better) `flit`_ as a a builder.
    Install it via

    .. code-block:: bash

        pip3 [ --user ] install flit

    You can now install ``b2luigi`` from the cloned git repository in development mode:

    .. code-block:: bash

        flit install -s -deps=develop

    Now you can start hacking and your changes will be immediately available to you.

4.  Automatically check your code with `pre-commit`_:

    .. code-block:: bash

        pre-commit install  # install the pre-commit hooks
        pre-commit run --all-files  # run pre-commit manually

    In particular, the python files are checked with `ruff`_ for syntax and
    `PEP 8`_ style errors. We recommend using an IDE or editor which
    automatically highlights errors with ruff or a similar python linter (e.g. `Pylint`_ or `flake8`_).

5.  We use the `pytest`_ package for testing some parts of the code.
    All tests reside in the ``tests/`` sub-directory. To run all tests, run the command

    .. code-block:: bash

        pytest -v b2luigi tests

    in the root of ``b2luigi`` repository. If you add some functionality, try to add some tests for it.

6.  The documentation is hosted on `b2luigi.belle2.org`_ and build automatically on every commit to main.
    You can (and should) also build the documentation locally:

    .. code-block:: bash

        sphinx-autobuild docs build

    The autobuild will rebuild the project whenever you change something. It displays a URL where to find
    the created docs now (most likely http://127.0.0.1:8000).
    Please make sure the documentation looks fine before creating a pull request.

7.  If you are a core developer and want to release a new version:

    a.  Make sure all changes are committed and merged on main

    b.  Use the `bump-my-version`_ package to update the version in `b2luigi/__init__.py`,
	`.bumpversion.cfg` as well as the git tag. ``flit`` will automatically use this.

        .. code-block:: bash

            bump-my-version bump --no-commit [patch|minor|major]

    c.  Push the new commit and the tags

        .. code-block:: bash

            git push --follow-tags

    d.  On GitLab releases are automatically generated from the merge requests that had a title and a changelog in their description.
	    For `GitHub <https://github.com/belle2/b2luigi/releases>`_ create a release and copy the content from GitLab.

    e.  Check that the new release had been published to PyPI, which should happen automatically via
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

.. _GitLab: https://gitlab.desy.de/belle2/software/b2luigi
.. _b2venv: https://software.belle2.org/development/sphinx/build/tools_doc/b2venv.html
.. _flit: https://pypi.org/project/flit/
.. _gitlab issues: https://gitlab.desy.de/belle2/software/b2luigi/-/issues
.. _pytest: https://docs.pytest.org/
.. _b2luigi.belle2.org: https://b2luigi.belle2.org
.. _pre-commit: https://pre-commit.com
.. _ruff: https://docs.astral.sh/ruff/
.. _PEP 8: https://www.python.org/dev/peps/pep-0008/
.. _Pylint: https://pylint.pycqa.org/en/latest/
.. _flake8: https://flake8.pycqa.org/en/latest/
.. _bump-my-version: https://github.com/callowayproject/bump-my-version
.. _release: https://github.com/belle2/b2luigi/releases
.. _pipeline: https://github.com/belle2/b2luigi/blob/main/.gitlab-ci.yml
.. _Keep a Changelog: https://keepachangelog.com/en/1.0.0/
