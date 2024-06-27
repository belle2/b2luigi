import os
import fnmatch


def search_pattern_in_folder(path: str, pattern: str):
    """
    Recursively search for a pattern in the folder structure starting from 'path'.
    Args:
      path (str): The root directory to start the search from.
      pattern (str): The pattern to search for in file and folder names. It can include wildcards.
    Returns:
      str
    """
    for root, dirs, files in os.walk(path):
        for dir_name in dirs:
            if fnmatch.fnmatch(dir_name, pattern):
                return root
        for file_name in files:
            if fnmatch.fnmatch(file_name, pattern):
                return root
    raise RuntimeError("Pattern not found in the given directory!")
