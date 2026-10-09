import pickle

from basf2 import conditions as b2conditions, logging
from basf2.pickle_path import serialize_path
from variables import variables as vm


def get_alias_dict_from_variable_manager():
    """
    Extracts a dictionary of alias names and their corresponding variable names from the
    internal state of the variable manager.

    This function iterates over all alias names managed by the variable manager, retrieves
    the associated variable for each alias, and constructs a dictionary where the keys
    are the alias names (as strings) and the values are the names of the corresponding
    variables.

    Returns:
        dict: A dictionary where keys are alias names (str) and values are the names of
        the corresponding variables (str).
    """
    alias_dictionary = {str(alias_name): vm.getVariable(str(alias_name)).name for alias_name in vm.getAliasNames()}
    return alias_dictionary


def write_path_and_state_to_file(basf2_path, file_path):
    """
    Serialize a ``basf2`` path, variable aliases, and global tags to a file.

    This function is a variant of ``basf2.pickle_path.write_path_to_file`` with
    additional serialization of ``basf2`` variable aliases and global tags. The aliases
    are extracted from the current state of the variable manager singleton, and the
    global tags are taken from the `b2conditions` module. Both must be set in the
    python/``basf2`` process before calling this function.

    Args:
        basf2_path: The ``basf2`` path object to serialize.
        file_path: The file path where the serialized pickle object will be written.
    """
    with open(file_path, "bw") as pickle_file:
        serialized = serialize_path(basf2_path)
        serialized["aliases"] = get_alias_dict_from_variable_manager()
        serialized["globaltags"] = b2conditions.globaltags
        serialized["log_level"] = logging.log_level
        # serialized["conditions"] = b2conditions
        pickle.dump(serialized, pickle_file)
