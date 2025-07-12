b2luigi
=======

``b2luigi`` — bringing batch 2 luigi!


``b2luigi`` is a helper package for ``luigi`` for scheduling large ``luigi`` workflows on a batch system.
It is as simple as

.. code-block:: python

    import b2luigi


    class MyTask(b2luigi.Task):
        def output(self):
            return self.add_to_output("output_file.txt")

        def run(self):
            with open(self.get_output_file_name("output_file.txt"), "w") as f:
                f.write("This is a test\n")


    if __name__ == "__main__":
        b2luigi.process(MyTask(), batch=True)

Jump right into it with out :ref:`quick-start-label`.

If you have never worked with ``luigi`` before, you may want to have a look into the `luigi documentation`_.
But you can learn most of the nice features also from this documentation!

Why not use the already created batch tasks?
--------------------------------------------

``luigi`` already contains a large set of tasks for scheduling and monitoring batch jobs [1]_.
But for thousands of tasks in very large projects with different task-defining libraries, you have some problems:

*   **You want to run many (many many!) batch jobs in parallel**

    In other ``luigi`` batch implementations, for every running batch job you also need a running task that monitors it.
    On most of the systems, the maximal number of processes is limited per user, so you will not be able to run more
    batch jobs than this.
    But what do you do if you have thousands of tasks to do?
*   **You have already a large set of luigi tasks in your project**

    In other implementations, you either have to override a ``work`` function (and you are not allowed to touch
    the ``run`` function) or run an external command, which you need to define.
    The first approach is problematic when mixing non-batch and batch task libraries and the second
    has problems when you need to pass complex arguments to the external command (via command line).
*   **You do not know which batch system you will run on**

    Currently, the batch tasks are mostly defined for a specific batch system. But what if you want to
    switch from AWS to Azure? From LSF to SGE?

Entering ``b2luigi`` -- it tries to solve all this (but was heavily inspired by the previous implementations):

*   You can run as many tasks as your batch system can handle in parallel! There will only be a single process running
    on your submission machine.
*   No need to rewrite your tasks! Just call them with ``b2luigi.process(.., batch=True)`` or with
    ``python file.py --batch`` and you are ready to go!
*   Switching the batch system is just a single change in a config file or one line in python.
    In the future, ``b2luigi`` will be able to automatically discover which batch system is in use!


Is this the only thing I can do with ``b2luigi``?
-------------------------------------------------

As ``b2luigi`` should help you with large ``luigi`` projects, we have also included some helper functionalities for
``luigi`` tasks and task handling.
``b2luigi`` task is a super-hero version of ``luigi`` task, with simpler handling for output and input files.
Also, we give you working examples and best-practices for better data management and how to accomplish your goals,
that we have learned with time.


Why are you still talking, let's use it!
---------------------------------------

Have a look into the :ref:`quick-start-label` and/or the :ref:`starterkit_label`.
You can also start reading the :ref:`api-documentation-label` or the code on GitHub_.
If you find any bugs or want to improve the documentation, please send us a pull request.
You can help me by working with one of the to-do items described in :ref:`development-label`.

Content
-------

.. toctree::
    :maxdepth: 2

    installation
    quickstart
    starterkit/index
    features
    examples
    api
    faq
    development


The name
--------

``b2luigi`` stands for multiple things at the same time:

*   It brings **b**\ atch to (**2**\ ) ``luigi``.
*   It helps you with the **b**\ read and **b**\ utter work in ``luigi`` (e.g. proper data management)
*   It was developed for the `Belle II`_ experiment.


The team
--------

Current developer and maintainer
    The Belle II Collaboration (`belle2`_) since November 2023

Original author
    Nils Braun (`nils-braun`_)

.. _github: https://github.com/belle2/b2luigi
.. _`luigi documentation`: https://luigi.readthedocs.io/en/stable
.. _`Belle II`: https://www.belle2.org
.. _`belle2`: https://github.com/belle2
.. _`nils-braun`: https://github.com/nils-braun

.. [1] https://github.com/spotify/luigi/blob/master/luigi/contrib/sge.py
