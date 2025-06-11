import hashlib

import luigi
from luigi.parameter import _no_value
from inspect import signature


def wrap_parameter():
    """
    Monkey patch the parameter base class (and with it all other parameters(
    of luigi to include three additional parameters in its constructor:
    ``hashed``, ``hash_function`` and ``hidden``.

    Enabling the ``hashed`` parameter will use a hashed version of the
    parameter value when creating file paths our of the parameters of a task
    instead of the value itself. By default an md5 hash is used. A custom
    hash function can be provided via the ``hash_function`` parameter. This
    function should take one input, the value of the parameter. It is up
    to the user to ensure a unique string is created from the input.

    This is especially useful when you have list, string or dict parameters,
    where the resulting file path may include "/" or "{}".

    With the ``hidden`` parameter, you can control whether the parameter
    should be hiddened in the task's output directory structure when using
    :meth:`add_to_output <b2luigi.Task.add_to_output>`.

    .. caution::
        This will remove the parameter from the unique output of the task,
        so be sure to add it back, e.g. into the output file name:

        .. code-block:: python

            class MyTask(b2luigi.Task):
                iddened_parameter = b2luigi.Parameter(hidden=True)

                def output(self):
                    yield self.add_to_output(f"test_{self.hiddened_parameter}.txt")
    """
    import b2luigi

    parameter_class = b2luigi.Parameter

    def serialize_hashed(self, x):
        if self.hash_function is None:
            return "hashed_" + hashlib.md5(str(x).encode()).hexdigest()
        else:
            return self.hash_function(x)

    old_init = parameter_class.__init__

    def __init__(self, hashed=False, hash_function=None, hidden=None, *args, **kwargs):
        old_init(self, *args, **kwargs)

        if hash_function is not None:
            n_params = len(signature(hash_function).parameters)
            assert n_params == 1, "Custom hash function can have only" f" 1 argument, found {n_params}"

        self.hash_function = hash_function

        if hashed:
            self.serialize_hashed = lambda x: serialize_hashed(self, x)

        self.hidden = hidden if hidden is not None else not self.significant

        if not self.significant and not self.hidden:
            raise ValueError("Parameter cannot be both hidden=False and significant=False.")

    parameter_class.__init__ = __init__


class BoolParameter(luigi.BoolParameter):
    """Copied BoolParameter without default value"""

    def __init__(self, **kwargs):
        kwargs.setdefault("default", _no_value)
        luigi.Parameter.__init__(self, **kwargs)
