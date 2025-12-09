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
    if not os.path.isdir(path):
        raise RuntimeError(f"Search root '{path}' does not exist or is not a directory.")

    sample_entries = []
    dir_count = 0
    file_count = 0

    for root, dirs, files in os.walk(path):
        dir_count += len(dirs)
        file_count += len(files)

        for dir_name in dirs:
            if fnmatch.fnmatch(dir_name, pattern):
                return root
        for file_name in files:
            if fnmatch.fnmatch(file_name, pattern):
                return root

        if len(sample_entries) < 5:
            candidates = [os.path.join(root, name) for name in sorted(dirs + files)]
            for candidate in candidates:
                if len(sample_entries) >= 5:
                    break
                sample_entries.append(candidate)

    example_entries = ", ".join(sample_entries) if sample_entries else "no entries found"
    raise RuntimeError(
        f"Pattern '{pattern}' not found anywhere under '{path}'. "
        f"Visited {dir_count} directories and {file_count} files. "
        f"Examples encountered: {example_entries}."
    )
