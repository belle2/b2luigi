"""Parameters for multi-task fixture — only covers the root task.

:Description: ``child_param`` is intentionally absent; it is computed by
    ParentTask.requires() as ``parent_param * 2``.
"""

config = {"parent_param": 3}
