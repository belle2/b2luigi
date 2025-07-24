Parameters (Advanced)
=====================

While Parameters are mostly handled by ``luigi`` itself, there are some additional features in ``b2luigi`` that are worth mentioning.

Unique Task outputs
-------------------
``b2luigi`` uses a tasks parameter to create a unique output folder for each task.
This uses the pattern ``parameter_name=parameter_value`` to create a unique folder for each task.

Exclude Parameters from the output name
---------------------------------------
Sometimes, one does not want to include all parameters in the output folder name.
This can be done by setting the ``Parameter``'s ``significant`` attribute to ``False``.

.. code-block:: python

    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.Parameter(significant=False)

Do not use the parameter name in the output name
------------------------------------------------
If you do not want to use the parameter name in the output folder name, set the setting ``use_parameter_name_in_output`` to ``False``.

Do not use "=" to separate the parameter name and value
-------------------------------------------------------
If you do not want to use the "=" to separate the parameter name and value, set the setting ``parameter_separator`` to a character that fits.

Hashing List and Dict Parameters
--------------------------------
When using lists or dictionaries as parameters, the output name can get very long and unwieldy.
To prevent this, you can set the ``hashed`` attribute of the parameter to ``True``.

.. code-block:: python

    class MyTask(b2luigi.Task):
        my_parameter = b2luigi.ListParameter(hashed=True)

This will use an md5 hash of the parameters string representation to build the output name.
It is also possible, to give a custom hashing function to the parameter, by setting the ``hash_function`` attribute to a function that
takes the parameter as input and returns a string.

.. code-block:: python

    class MyTask(b2lujson.Task):
        my_parameter = b2luigi.DictParameter(hashed=True, hash_function=lambda x: x["name"])

In this example, the Dictionary is expected to have a key ``name``. This name is used to build the output name.
