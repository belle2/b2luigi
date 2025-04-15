from contextlib import contextmanager
import os
import random
from typing import Generator, Optional
import luigi
import tempfile

from b2luigi.core.settings import get_setting


class FileSystemTarget(luigi.target.FileSystemTarget):
    """A local file system target that extends :class:`luigi.LocalTarget` with temporary file handling.

    This target provides additional functionality for handling temporary files during task execution,
    including support for temporary input and output paths with proper cleanup and atomic file operations.

    Attributes:
        scratch_dir (str): Directory path where temporary files will be created

    Args:
        _scratch_dir (str): Directory path where temporary files will be created
        *args: Additional positional arguments passed to :obj:`LocalTarget`
        **kwargs: Additional keyword arguments passed to :obj:`LocalTarget`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def tmp_name(self) -> str:
        """Generate a temporary filename based on the target's path.

        Creates a temporary filename by adding a random suffix to the original filename
        while preserving the file extension.

        Returns:
            str: A temporary filename in the format::

                "{original_name}-luigi-tmp-{random_number}.{extension}"

        Warning:
            This method assumes the file has an extension. Files without extensions will raise an :obj:``IndexError``.
            This limitation should be addressed in future versions.

        Example:
            If ``self.path`` is `"data.txt"`, might return `"data-luigi-tmp-1234567890.txt"`
        """
        num = random.randrange(0, 10_000_000_000)
        filename_parts = os.path.basename(self.path).split(".")
        prefix = filename_parts[0]
        extension = "".join(filename_parts[1:]) if len(filename_parts) > 1 else ""
        _temp_path = f"{prefix}-luigi-tmp-{num:010}.{extension}{self._trailing_slash()}"
        return _temp_path

    @contextmanager
    def get_temporary_input(self, task: Optional[luigi.Task] = None, **tmp_file_kwargs) -> Generator[str, None, None]:
        """Create a temporary copy of the input file for processing.

        Creates a temporary copy of the input file in the scratch directory, allowing
        safe concurrent access to the same input file by multiple tasks.

        Args:
            **tmp_file_kwargs: Keyword arguments passed to :class:`tempfile.TemporaryDirectory`

        Yields:
            str: Absolute path to the temporary copy of the input file

        Note:
            The temporary file and its parent directory are automatically cleaned up
            when exiting the context manager, regardless of whether an exception occurred.

        Example:

            .. code-block:: python

                with target.get_temporary_input() as tmp_input:
                    process_file(tmp_input)
        """
        with tempfile.TemporaryDirectory(
            dir=get_setting("scratch_dir", task=task, default="/tmp"), **tmp_file_kwargs
        ) as tmp_path:
            tmp_path = os.path.join(tmp_path, self.tmp_name)
            self.fs.copy(self.path, tmp_path)
            yield tmp_path

    @contextmanager
    def temporary_path(self, task: Optional[luigi.Task] = None, **tmp_file_kwargs) -> Generator[str, None, None]:
        """Create a temporary file for output that will be moved to the final location.

        Implements atomic write operations by first writing to a temporary location
        and then moving the file to its final destination only after successful completion.

        Args:
            **tmp_file_kwargs: Keyword arguments passed to ``tempfile.TemporaryDirectory()``

        Yields:
            str: Absolute path to the temporary file for writing

        Note:
            - The temporary directory and its contents are automatically cleaned up
              after the file is copied to its final location
            - The final copy operation is atomic, preventing partial writes from
              corrupting the output file
            - Parent directories of the target path will be created if they don't exist

        Example:
            .. code-block:: python

                with target.temporary_path() as tmp_output:
                    with open(tmp_output, 'w') as f:
                        f.write('result data')
                # File is automatically moved to target location after context exit
        """
        # Use a temporary directory
        with tempfile.TemporaryDirectory(
            dir=get_setting("scratch_dir", task=task, default="/tmp"), **tmp_file_kwargs
        ) as tmp_dir:
            tmp_path = os.path.join(tmp_dir, self.tmp_name)
            os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
            yield tmp_path
            self.fs.rename_dont_move(tmp_path, self.path)


class LocalTarget(FileSystemTarget, luigi.LocalTarget):
    """A local file system target that extends :class:`luigi.LocalTarget` with temporary file handling.

    This target provides additional functionality for handling temporary files during task execution,
    including support for temporary input and output paths with proper cleanup and atomic file operations.

    Attributes:
        scratch_dir (str): Directory path where temporary files will be created

    Args:
        _scratch_dir (str): Directory path where temporary files will be created
        *args: Additional positional arguments passed to :class:`luigi.LocalTarget`
        **kwargs: Additional keyword arguments passed to :class:`luigi.LocalTarget`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
