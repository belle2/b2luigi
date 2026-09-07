"""Unit and integration tests for ParameterGenerator and ZippedParameterGenerator."""

import os
import pathlib
import shutil
import unittest

from b2luigi.cli.errors import CliUserError

from .helpers import CLITestCase


class TestParameterGeneratorConstruction(unittest.TestCase):
    """Unit tests for ParameterGenerator construction."""

    def test_valid_construction(self) -> None:
        from b2luigi.cli.parameter_generator import ParameterGenerator

        pg = ParameterGenerator([1, 2, 3])
        self.assertEqual(pg.values, [1, 2, 3])

    def test_empty_list_raises(self) -> None:
        from b2luigi.cli.parameter_generator import ParameterGenerator

        with self.assertRaises(CliUserError):
            ParameterGenerator([])


class TestZippedParameterGeneratorConstruction(unittest.TestCase):
    """Unit tests for ZippedParameterGenerator construction."""

    def test_valid_construction(self) -> None:
        from b2luigi.cli.parameter_generator import ZippedParameterGenerator

        zpg = ZippedParameterGenerator(alpha=[1, 2], beta=["a", "b"])
        self.assertEqual(zpg.pairs, {"alpha": [1, 2], "beta": ["a", "b"]})

    def test_no_kwargs_raises(self) -> None:
        from b2luigi.cli.parameter_generator import ZippedParameterGenerator

        with self.assertRaises(CliUserError):
            ZippedParameterGenerator()

    def test_mismatched_lengths_raises(self) -> None:
        from b2luigi.cli.parameter_generator import ZippedParameterGenerator

        with self.assertRaises(CliUserError):
            ZippedParameterGenerator(alpha=[1, 2], beta=[10])

    def test_single_kwarg_valid(self) -> None:
        from b2luigi.cli.parameter_generator import ZippedParameterGenerator

        zpg = ZippedParameterGenerator(x=[1, 2, 3])
        self.assertEqual(zpg.pairs, {"x": [1, 2, 3]})

    def test_empty_value_lists_raises(self) -> None:
        from b2luigi.cli.parameter_generator import ZippedParameterGenerator

        with self.assertRaises(CliUserError):
            ZippedParameterGenerator(x=[], y=[])

    def test_single_empty_value_list_raises(self) -> None:
        from b2luigi.cli.parameter_generator import ZippedParameterGenerator

        with self.assertRaises(CliUserError):
            ZippedParameterGenerator(x=[])


class TestExpandParameters(unittest.TestCase):
    """Unit tests for expand_parameters()."""

    def _expand(self, config):
        from b2luigi.cli.utils import expand_parameters

        return expand_parameters(config)

    def test_scalar_only_returns_single_dict(self) -> None:
        result = self._expand({"alpha": 1, "beta": "x"})
        self.assertEqual(result, [{"alpha": 1, "beta": "x"}])

    def test_single_parameter_generator(self) -> None:
        from b2luigi.cli.parameter_generator import ParameterGenerator

        result = self._expand({"v": ParameterGenerator([1, 2, 3])})
        self.assertEqual(result, [{"v": 1}, {"v": 2}, {"v": 3}])

    def test_two_parameter_generators_cartesian(self) -> None:
        from b2luigi.cli.parameter_generator import ParameterGenerator

        result = self._expand(
            {
                "a": ParameterGenerator([1, 2]),
                "b": ParameterGenerator(["x", "y"]),
            }
        )
        self.assertEqual(len(result), 4)
        self.assertIn({"a": 1, "b": "x"}, result)
        self.assertIn({"a": 1, "b": "y"}, result)
        self.assertIn({"a": 2, "b": "x"}, result)
        self.assertIn({"a": 2, "b": "y"}, result)

    def test_zipped_parameter_generator(self) -> None:
        from b2luigi.cli.parameter_generator import ZippedParameterGenerator

        result = self._expand({"_z": ZippedParameterGenerator(alpha=[1, 2], beta=["a", "b"])})
        self.assertEqual(result, [{"alpha": 1, "beta": "a"}, {"alpha": 2, "beta": "b"}])

    def test_mixed_cartesian_and_zipped(self) -> None:
        from b2luigi.cli.parameter_generator import ParameterGenerator, ZippedParameterGenerator

        result = self._expand(
            {
                "v": ParameterGenerator([10, 20]),
                "_z": ZippedParameterGenerator(alpha=[1, 2], beta=["a", "b"]),
            }
        )
        self.assertEqual(len(result), 4)
        self.assertIn({"v": 10, "alpha": 1, "beta": "a"}, result)
        self.assertIn({"v": 10, "alpha": 2, "beta": "b"}, result)
        self.assertIn({"v": 20, "alpha": 1, "beta": "a"}, result)
        self.assertIn({"v": 20, "alpha": 2, "beta": "b"}, result)

    def test_scalars_propagate_into_every_combination(self) -> None:
        from b2luigi.cli.parameter_generator import ParameterGenerator

        result = self._expand({"v": ParameterGenerator([1, 2]), "fixed": 99})
        self.assertEqual(result, [{"v": 1, "fixed": 99}, {"v": 2, "fixed": 99}])


class TestPublicAPI(unittest.TestCase):
    """Smoke tests for top-level b2luigi namespace re-exports."""

    def test_parameter_generator_importable_from_b2luigi(self) -> None:
        import b2luigi

        self.assertTrue(hasattr(b2luigi, "ParameterGenerator"))

    def test_zipped_parameter_generator_importable_from_b2luigi(self) -> None:
        import b2luigi

        self.assertTrue(hasattr(b2luigi, "ZippedParameterGenerator"))

    def test_parameter_generator_direct_import(self) -> None:
        from b2luigi import ParameterGenerator

        pg = ParameterGenerator([1, 2])
        self.assertEqual(pg.values, [1, 2])

    def test_zipped_parameter_generator_direct_import(self) -> None:
        from b2luigi import ZippedParameterGenerator

        zpg = ZippedParameterGenerator(x=[1, 2])
        self.assertEqual(zpg.pairs, {"x": [1, 2]})


class TestParameterGeneratorIntegration(CLITestCase):
    """Integration tests for ParameterGenerator expansion via b2luigi run."""

    def setUp(self) -> None:
        super().setUp()
        test_dir = os.path.dirname(__file__)
        shutil.copy(
            os.path.join(test_dir, "cli_generator_tasks.py"),
            os.path.join(self.tmp_dir, "tasks.py"),
        )

    def _write_parameters(self, content: str) -> None:
        """Write a ``parameters.py`` file in the temp directory.

        :param content: Full file content to write.
        :type content: str
        """
        pathlib.Path(self.tmp_dir, "parameters.py").write_text(content)

    def test_scalar_config_runs_single_task(self) -> None:
        """Scalar config produces one task — no WrapperTask, existing behaviour."""
        self._write_parameters("config = {'value': 42}\n")
        returncode, stdout, stderr = self._run_cli("run", ["SimpleTask", "--dry"])
        self.assertIn(returncode, (0, 256), stderr)
        combined = stdout + stderr
        self.assertNotIn("SimpleTaskWrapper", combined)

    def test_parameter_generator_dry_run_lists_all_tasks(self) -> None:
        """ParameterGenerator with 3 values → dry-run mentions all 3 SimpleTask instances."""
        self._write_parameters(
            "from b2luigi import ParameterGenerator\n" "config = {'value': ParameterGenerator([1, 2, 3])}\n"
        )
        returncode, stdout, stderr = self._run_cli("run", ["SimpleTask", "--dry"])
        self.assertIn(returncode, (0, 256), stderr)
        combined = stdout + stderr
        self.assertIn("SimpleTaskWrapper", combined)
        for v in (1, 2, 3):
            self.assertIn(f"value={v}", combined)

    def test_param_override_pins_generator(self) -> None:
        """--param override replaces ParameterGenerator with a scalar → single task."""
        self._write_parameters(
            "from b2luigi import ParameterGenerator\n" "config = {'value': ParameterGenerator([1, 2, 3])}\n"
        )
        returncode, stdout, stderr = self._run_cli("run", ["SimpleTask", "--param", "value=99", "--dry"])
        self.assertIn(returncode, (0, 256), stderr)
        combined = stdout + stderr
        self.assertNotIn("SimpleTaskWrapper", combined)

    def test_zipped_parameter_generator_dry_run(self) -> None:
        """ZippedParameterGenerator with 2 pairs → dry-run succeeds (2 tasks)."""
        self._write_parameters(
            "from b2luigi import ZippedParameterGenerator\n"
            "config = {'_z': ZippedParameterGenerator(value=[10, 20])}\n"
        )
        returncode, stdout, stderr = self._run_cli("run", ["SimpleTask", "--dry"])
        self.assertIn(returncode, (0, 256), stderr)
        combined = stdout + stderr
        self.assertIn("SimpleTaskWrapper", combined)
        for v in (10, 20):
            self.assertIn(f"value={v}", combined)

    def test_show_with_parameter_generator_no_repr_in_path(self) -> None:
        """Regression: show must not render ParameterGenerator repr in output paths."""
        self._write_parameters(
            "from b2luigi import ParameterGenerator\n" "config = {'value': ParameterGenerator([1, 2, 3])}\n"
        )
        returncode, stdout, stderr = self._run_cli("show", ["SimpleTask"])
        combined = stdout + stderr
        self.assertNotIn("ParameterGenerator", combined)
        self.assertNotIn("object at 0x", combined)

    def test_show_all_with_parameter_generator(self) -> None:
        """show with no task name expands generators and lists all task combinations."""
        self._write_parameters(
            "from b2luigi import ParameterGenerator\n" "config = {'value': ParameterGenerator([1, 2, 3])}\n"
        )
        returncode, stdout, stderr = self._run_cli("show", [])
        combined = stdout + stderr
        self.assertNotIn("ParameterGenerator", combined)
        self.assertNotIn("object at 0x", combined)


if __name__ == "__main__":
    unittest.main()
