import sys
import unittest
from decimal import ROUND_DOWN, Underflow, localcontext
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from surge_cluster.finqa import check_oracle, compare_answer, execute_program, number


class FinQAOracleTests(unittest.TestCase):
    def test_replay_does_not_inherit_caller_rounding(self):
        expected = execute_program("divide(1, 6)")["value"]
        with localcontext() as caller:
            caller.rounding = ROUND_DOWN
            self.assertEqual(execute_program("divide(1, 6)")["value"], expected)

    def test_executable_check_does_not_inherit_caller_numeric_limits(self):
        with localcontext() as caller:
            caller.prec = 2
            caller.Emin = 0
            caller.traps[Underflow] = False
            report = check_oracle("1.0001", {"answer": "1.0001", "exe_ans": "1"})
        self.assertFalse(report["ok"])
        self.assertFalse(report["exe_ans_ok"])

    def test_arithmetic_underflow_does_not_become_a_zero_answer(self):
        program = ", ".join(["exp(0.00000001, 10)"] + [f"exp(#{index}, 10)" for index in range(5)])
        with self.assertRaisesRegex(ValueError, "invalid arithmetic"):
            execute_program(program)

    def test_tiny_display_tolerance_underflow_cannot_match_zero(self):
        report = compare_answer("0", "1e-1100000")
        self.assertFalse(report["ok"])
        self.assertIn("error", report)

    def test_program_results_and_float_labels_accept_scientific_notation(self):
        trace = execute_program("divide(1, 100000000)")
        self.assertTrue(check_oracle(trace["value"], {"answer": "0.00000001", "exe_ans": 1e-8})["ok"])
        self.assertTrue(compare_answer("1E-8", "0.00000001")["ok"])

    def test_scientific_numbers_still_enforce_finite_replay_bounds(self):
        for value in ("1e101", "-1e101", "1e999999999999999999999999", "NaN", "Infinity", "1e+bad"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                number(value)

    def test_replays_arithmetic_trace_without_eval(self):
        trace = execute_program("add(40, 60), divide(#0, 4)")
        self.assertEqual(trace["value"], "25")
        self.assertEqual(len(trace["trace"]), 2)

    def test_oracle_requires_executable_answer(self):
        self.assertFalse(check_oracle("100", {"answer": "100"})["ok"])
        self.assertTrue(check_oracle("100", {"answer": "100", "exe_ans": 100})["ok"])

    def test_mismatch_does_not_pass_by_display_answer_copy(self):
        self.assertFalse(check_oracle("99", {"answer": "100", "exe_ans": 100})["ok"])

    def test_percent_answer_supports_ratio_or_percentage_program_output(self):
        self.assertTrue(compare_answer("0.125", "12.5%")["ok"])
        self.assertTrue(compare_answer("24.69136", "24.69%")["ok"])

    def test_forward_reference_and_unsupported_operation_fail_closed(self):
        with self.assertRaises(ValueError):
            execute_program("add(#0, 1)")
        with self.assertRaises(ValueError):
            execute_program("table_sum(1)")


if __name__ == "__main__":
    unittest.main()
