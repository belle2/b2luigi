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


if __name__ == "__main__":
    unittest.main()
