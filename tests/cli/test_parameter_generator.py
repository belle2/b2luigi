"""Unit and integration tests for ParameterGenerator and ZippedParameterGenerator."""

import unittest

from b2luigi.cli.errors import CliUserError


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


if __name__ == "__main__":
    unittest.main()
