import polars as pl

import b2luigi
from b2luigi.contrib.polars.parameters import (
    PolarsExpressionDictParameter,
    PolarsExpressionListParameter,
    PolarsExpressionParameter,
)

from ...helpers import B2LuigiTestCase


class PolarsParametersTestCase(B2LuigiTestCase):
    def test_expression_roundtrip(self):
        expr = (pl.col("value") + 1) * 2
        parameter = PolarsExpressionParameter()

        serialized = parameter.serialize(expr)
        restored = parameter.parse(serialized)

        self.assertIsInstance(restored, pl.Expr)
        self.assertTrue(expr.meta.eq(restored))

    def test_expression_hashing_is_deterministic(self):
        expr = pl.col("value") + 3
        parameter = PolarsExpressionParameter()

        first_hash = parameter.serialize_hashed(expr)
        second_hash = parameter.serialize_hashed(expr)
        different_hash = parameter.serialize_hashed(pl.col("value") + 4)

        self.assertEqual(first_hash, second_hash)
        self.assertNotEqual(first_hash, different_hash)

    def test_list_roundtrip(self):
        values = [
            pl.col("a") + 1,
            {"inner": pl.col("b") * 2, "plain": "keep"},
            ("tuple", pl.col("c") == 5),
            [1, 2, 3],
        ]
        parameter = PolarsExpressionListParameter()

        restored = parameter.parse(parameter.serialize(values))

        self.assertTrue(values[0].meta.eq(restored[0]))
        self.assertIsInstance(restored[1], dict)
        self.assertTrue(values[1]["inner"].meta.eq(restored[1]["inner"]))
        self.assertEqual(values[1]["plain"], restored[1]["plain"])
        # JSON has no tuples, luigi freezes all sequences to tuples on task creation anyway
        self.assertEqual(values[2][0], restored[2][0])
        self.assertTrue(values[2][1].meta.eq(restored[2][1]))
        self.assertEqual(values[3], list(restored[3]))

    def test_dict_roundtrip(self):
        values = {
            "first": pl.col("x") * 3,
            "nested": {
                "list": [pl.col("x") + 1, 3],
                "tuple": (pl.col("x") - 1,),
            },
        }
        parameter = PolarsExpressionDictParameter()

        restored = parameter.parse(parameter.serialize(values))
        self.assertTrue(values["first"].meta.eq(restored["first"]))
        self.assertIsInstance(restored["nested"], dict)
        self.assertIsInstance(restored["nested"]["list"], list)
        self.assertTrue(values["nested"]["list"][0].meta.eq(restored["nested"]["list"][0]))
        self.assertEqual(values["nested"]["list"][1], restored["nested"]["list"][1])
        self.assertTrue(values["nested"]["tuple"][0].meta.eq(restored["nested"]["tuple"][0]))

    def test_with_task(self):
        """Test that Polars parameters work within a task's run method."""

        class PolarsTask(b2luigi.Task):
            expr_param = PolarsExpressionParameter(hashed=True)
            list_param = PolarsExpressionListParameter()
            dict_param = PolarsExpressionDictParameter()

            def run(self):
                # Create test data
                df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

                # Apply the expression parameter
                _ = df.select(self.expr_param.alias("result"))

                # Apply expressions from list parameter
                _ = df.select(self.list_param[0].alias("result"))

                # Apply expression from dict parameter
                _ = df.select(self.dict_param["transform"].alias("result"))

            def output(self):
                yield self.add_to_output("results.json")

        # Create task with Polars expressions
        task = PolarsTask(
            expr_param=pl.col("a") * 2,
            list_param=[pl.col("b") + 10],
            dict_param={"transform": pl.col("a") + pl.col("b")},
        )

        # Run the task and make sure no exceptions occur
        task.run()

    def test_task_string_roundtrip(self):
        """Test that a task can be recreated from its string parameters (as done e.g. for batch jobs)."""

        class PolarsTask(b2luigi.Task):
            expr_param = PolarsExpressionParameter()
            list_param = PolarsExpressionListParameter(hashed=True)
            dict_param = PolarsExpressionDictParameter(hashed=True)

            def output(self):
                yield self.add_to_output("results.json")

        task = PolarsTask(
            expr_param=pl.col("a") * 2,
            list_param=[pl.col("b") + 10, 3],
            dict_param={"transform": pl.col("a") + pl.col("b"), "threshold": 0.5},
        )
        restored = PolarsTask.from_str_params(task.to_str_params())

        self.assertEqual(task.task_id, restored.task_id)
        self.assertTrue(task.expr_param.meta.eq(restored.expr_param))
        self.assertTrue(task.list_param[0].meta.eq(restored.list_param[0]))
        self.assertTrue(task.dict_param["transform"].meta.eq(restored.dict_param["transform"]))
        self.assertEqual(task.get_output_file_name("results.json"), restored.get_output_file_name("results.json"))

        # All parameters are hashed, so no serialized expression ends up in the output path
        output_path = task.get_output_file_name("results.json")
        self.assertNotIn(PolarsTask.expr_param.serialize(task.expr_param), output_path)
        self.assertIn(f"expr_param={PolarsTask.expr_param.serialize_hashed(task.expr_param)}", output_path)
