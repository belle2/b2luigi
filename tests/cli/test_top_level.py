"""Integration tests for b2luigi top-level commands: version, about, and init.

:Description: Tests ``b2luigi version``, ``b2luigi about``, and ``b2luigi init``
    end-to-end, verifying exit codes, output content, and file creation.
"""

import os

from .helpers import CLITestCase


class TestVersion(CLITestCase):
    """Integration tests for the version command."""

    def test_version_exits_zero(self) -> None:
        """Verify that ``b2luigi version`` exits with code 0."""
        returncode, stdout, stderr = self._run_cli("version")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_version_output_nonempty(self) -> None:
        """Verify that ``b2luigi version`` prints a non-empty string."""
        _, stdout, _ = self._run_cli("version")
        self.assertTrue(stdout.strip(), "Expected non-empty version output")

    def test_version_matches_semver_pattern(self) -> None:
        """Verify that the version string matches a semver-like pattern."""
        _, stdout, _ = self._run_cli("version")
        self.assertRegex(stdout.strip(), r"^\d+\.\d+\.\d+")


class TestAbout(CLITestCase):
    """Integration tests for the about command."""

    def test_about_exits_zero(self) -> None:
        """Verify that ``b2luigi about`` exits with code 0."""
        returncode, stdout, stderr = self._run_cli("about")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_about_contains_b2luigi_label(self) -> None:
        """Verify that the about output contains the ``b2luigi:`` label."""
        _, stdout, _ = self._run_cli("about")
        self.assertIn("b2luigi:", stdout)

    def test_about_contains_python_label(self) -> None:
        """Verify that the about output contains the ``python:`` label."""
        _, stdout, _ = self._run_cli("about")
        self.assertIn("python:", stdout)

    def test_about_contains_cwd_label(self) -> None:
        """Verify that the about output contains ``cwd:`` and the temp directory path."""
        returncode, stdout, stderr = self._run_cli("about")
        self.assertEqual(returncode, 0)
        self.assertIn("cwd:", stdout)
        # Use realpath to handle macOS /var -> /private/var symlink
        self.assertIn(os.path.realpath(self.tmp_dir), stdout)

    def test_about_unset_env_vars_show_unset(self) -> None:
        """Verify that ``(unset)`` appears when B2LUIGI_* env vars are not set."""
        _, stdout, _ = self._run_cli(
            "about",
            exclude_env={"B2LUIGI_TASK_FILE", "B2LUIGI_PARAMS_FILE"},
        )
        self.assertIn("(unset)", stdout)


class TestInit(CLITestCase):
    """Integration tests for the init command."""

    def test_init_exits_zero(self) -> None:
        """Verify that ``b2luigi init`` exits with code 0."""
        returncode, stdout, stderr = self._run_cli("init")
        self.assertEqual(returncode, 0, f"Command failed with stderr: {stderr}")

    def test_init_creates_tasks_py(self) -> None:
        """Verify that ``b2luigi init`` creates tasks.py."""
        self._run_cli("init")
        self.assertTrue(
            os.path.exists(os.path.join(self.tmp_dir, "tasks.py")),
            "tasks.py was not created by b2luigi init",
        )

    def test_init_creates_parameters_py(self) -> None:
        """Verify that ``b2luigi init`` creates parameters.py."""
        self._run_cli("init")
        self.assertTrue(
            os.path.exists(os.path.join(self.tmp_dir, "parameters.py")),
            "parameters.py was not created by b2luigi init",
        )

    def test_init_skips_existing_without_force(self) -> None:
        """Verify that existing files are skipped without ``--force``."""
        sentinel = "# sentinel content\n"
        tasks_path = os.path.join(self.tmp_dir, "tasks.py")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write(sentinel)

        _, stdout, _ = self._run_cli("init")

        self.assertIn("Skip", stdout)
        with open(tasks_path, encoding="utf-8") as f:
            self.assertEqual(f.read(), sentinel, "File content was changed despite no --force")

    def test_init_force_overwrites_existing(self) -> None:
        """Verify that ``--force`` overwrites existing files."""
        sentinel = "# old content\n"
        tasks_path = os.path.join(self.tmp_dir, "tasks.py")
        with open(tasks_path, "w", encoding="utf-8") as f:
            f.write(sentinel)

        _, stdout, _ = self._run_cli("init", ["--force"])

        self.assertIn("Wrote", stdout)
        with open(tasks_path, encoding="utf-8") as f:
            new_content = f.read()
        self.assertNotEqual(new_content, sentinel)
        self.assertIn("b2luigi", new_content)  # template always imports b2luigi
