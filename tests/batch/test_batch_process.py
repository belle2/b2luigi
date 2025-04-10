import os
import subprocess

from ..helpers import B2LuigiTestCase


class BatchProcessTestCase(B2LuigiTestCase):
    def test_simple_task(self):
        self.call_file("batch/batch_task_1.py")
        print(os.listdir("some_parameter=bla_blub"))
        self.assertTrue(os.path.exists("some_parameter=bla_blub/test.txt"))
        self.assertTrue(os.path.exists("some_parameter=bla_blub/combined.txt"))

    def test_failing_task(self):
        out = self.call_file("batch/batch_task_2.py", stderr=subprocess.STDOUT)
        self.assertTrue(os.path.exists("some_parameter=bla_blub/test.txt"))
        self.assertFalse(os.path.exists("some_parameter=bla_blub/combined.txt"))

        self.assertIn(b"INFO: Batch process TestProcess running  MyTask(some_parameter=bla_blub)", out.splitlines())
        self.assertIn(
            b"Failed task MyAdditionalTask(some_parameter=bla_blub) with task_id and parameters:", out.splitlines()
        )
        self.assertIn(b"Please have a look into the log files in:", out.splitlines())
        self.assertIn(b"This progress looks :( because there were failed tasks", out.splitlines())
