"""Tests for the model-side verifier.

核心不变量：验证器输出不可解析时必须记为"未通过"，因此这里的负向用例与正向
用例同等重要；同时验证模型 token 会折回节点 usage，预算不会少记真实开销。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from surge_cluster import (
    AdapterFailure,
    DAGScheduler,
    NodeSpec,
    RunSpec,
    VerifyingAdapter,
    WorkerResult,
)
from surge_cluster.verification import (
    ModelVerifier,
    build_verification_prompt,
    parse_verifier_verdict,
)


def _envelope(answer: str, claims: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    payload = {
        "answer": answer,
        "claims": claims,
        "citations": [],
        "confidence": 0.5,
        "warnings": [],
        "usage": {"total_tokens": 40, "cost": 0.0004},
    }
    payload.update(extra)
    return payload


def _node(**overrides: Any) -> NodeSpec:
    options: dict[str, Any] = {"id": "n1", "prompt": "work", "max_attempts": 1, "timeout_seconds": 10.0}
    options.update(overrides)
    return NodeSpec(**options)


class _VerdictAdapter:
    """扮演验证模型：返回预设文本，并记录收到的提示。"""

    model = "fake-verifier"
    cost_per_1k_tokens = 0.01

    def __init__(self, text: str, *, usage: tuple[int, int] = (10, 5)):
        self.text = text
        self.usage = {"input_tokens": usage[0], "output_tokens": usage[1]}
        self.prompts: list[str] = []
        self.calls = 0
        self.last_response: Mapping[str, Any] | None = None

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        self.calls += 1
        self.prompts.append(node.prompt)
        self.last_response = {"text": self.text, "usage": dict(self.usage)}
        return WorkerResult(
            envelope=_envelope(self.text, [], usage={"total_tokens": 0, "cost": 0.0}),
        )


class _RaisingAdapter:
    model = "fake-verifier"

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        raise AdapterFailure("verifier transport failed", kind="transport", retryable=True)


class _WorkerAdapter:
    def __init__(self, payload: Mapping[str, Any], artifacts: Mapping[str, str]):
        self.payload = dict(payload)
        self.artifacts = dict(artifacts)

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        return WorkerResult(envelope=dict(self.payload), artifacts=dict(self.artifacts))


CLAIM = [{"id": "c1", "text": "the measured value is 42", "evidence_refs": ["notes"]}]


class ModelVerifierUnitTests(unittest.TestCase):
    def _verify(self, text: str, *, own: Mapping[str, str] | None = None, **kwargs: Any):
        adapter = _VerdictAdapter(text)
        verifier = ModelVerifier(adapter, **kwargs)
        result = verifier.verify(
            _node(),
            _envelope("42", CLAIM),
            own_artifacts=own if own is not None else {"notes": "the measured value is 42"},
        )
        return adapter, verifier, result

    def test_valid_pass_verdict(self) -> None:
        adapter, _, report = self._verify('{"pass": true, "score": 0.9, "issues": []}')
        self.assertTrue(report.passed)
        self.assertAlmostEqual(report.score, 0.9)
        self.assertEqual(report.checked_claims, 1)
        self.assertEqual(report.model, "fake-verifier")
        self.assertEqual(adapter.calls, 1)
        self.assertEqual(report.usage["total_tokens"], 15)
        self.assertAlmostEqual(report.usage["cost"], 15 / 1000.0 * 0.01)

    def test_valid_fail_verdict_carries_issues(self) -> None:
        _, _, report = self._verify('{"pass": false, "score": 0.2, "issues": ["evidence does not state 42"]}')
        self.assertFalse(report.passed)
        self.assertEqual([issue.code for issue in report.issues], ["verifier_reported_issue"])
        self.assertIn("does not state 42", report.issues[0].detail)

    def test_fenced_json_is_accepted(self) -> None:
        _, _, report = self._verify('Here you go:\n```json\n{"pass": true, "score": 1, "issues": []}\n```')
        self.assertTrue(report.passed)

    def test_invalid_output_counts_as_unverified(self) -> None:
        _, _, report = self._verify("I think it is probably fine.")
        self.assertFalse(report.passed)
        self.assertEqual([issue.code for issue in report.issues], ["verifier_output_invalid"])
        self.assertIn("not valid JSON", report.issues[0].detail)

    def test_wrong_field_types_count_as_unverified(self) -> None:
        _, _, report = self._verify('{"pass": "yes", "score": 3, "issues": "none"}')
        self.assertFalse(report.passed)
        codes = {issue.code for issue in report.issues}
        self.assertEqual(codes, {"verifier_output_invalid"})
        self.assertEqual(len(report.issues), 3)

    def test_missing_score_is_rejected(self) -> None:
        _, _, report = self._verify('{"pass": true, "issues": []}')
        self.assertFalse(report.passed)
        self.assertIn("score", report.issues[0].detail)

    def test_no_readable_evidence_never_calls_the_verifier(self) -> None:
        adapter = _VerdictAdapter('{"pass": true, "score": 1, "issues": []}')
        verifier = ModelVerifier(adapter)
        report = verifier.verify(_node(), _envelope("42", CLAIM), own_artifacts={}, dependency_refs={})
        self.assertFalse(report.passed)
        self.assertEqual([issue.code for issue in report.issues], ["no_readable_evidence"])
        self.assertEqual(adapter.calls, 0)

    def test_verifier_adapter_failure_propagates(self) -> None:
        verifier = ModelVerifier(_RaisingAdapter())
        with self.assertRaises(AdapterFailure) as caught:
            verifier.verify(_node(), _envelope("42", CLAIM), own_artifacts={"notes": "42"})
        self.assertEqual(caught.exception.kind, "transport")

    def test_prompt_contains_claims_and_evidence_but_no_gold_label(self) -> None:
        adapter, _, _ = self._verify('{"pass": true, "score": 1, "issues": []}')
        prompt = adapter.prompts[0]
        self.assertIn("c1", prompt)
        self.assertIn("the measured value is 42", prompt)
        self.assertIn("no ground-truth", prompt)
        self.assertIn('"pass"', prompt)
        self.assertNotIn("EXPECTED_ANSWER", prompt)
        self.assertNotIn("exe_ans", prompt)

    def test_duplicate_evidence_is_sent_once(self) -> None:
        marker = "MEASURED-42-MARKER"
        adapter = _VerdictAdapter('{"pass": true, "score": 1, "issues": []}')
        verifier = ModelVerifier(adapter)
        verifier.verify(
            _node(),
            _envelope("42", CLAIM),
            own_artifacts={"notes": marker},
            dependency_refs={"scout/notes": "artifact:" + "a" * 64},
            read_artifact=lambda _ref: marker,
        )
        self.assertEqual(adapter.prompts[0].count(marker), 1)

    def test_long_evidence_is_marked_as_truncated(self) -> None:
        adapter, _, report = self._verify(
            '{"pass": true, "score": 1, "issues": []}',
            own={"notes": "z" * 20000},
            max_evidence_chars=1000,
            max_total_evidence_chars=2000,
        )
        self.assertIn("[evidence truncated:", adapter.prompts[0])
        self.assertTrue(report.passed)

    def test_invalid_configuration_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ModelVerifier(object())

    def test_verifier_call_is_recorded_with_evidence_keys(self) -> None:
        _, verifier, _ = self._verify('{"pass": true, "score": 1, "issues": []}')
        self.assertEqual(verifier.calls[0]["node_id"], "n1/verify")
        self.assertEqual(verifier.calls[0]["evidence_keys"], ["notes"])


class VerifyingAdapterWithModelVerifierTests(unittest.TestCase):
    def _inner(self) -> _WorkerAdapter:
        return _WorkerAdapter(_envelope("42", CLAIM, warnings=["original"]), {"notes": "the measured value is 42"})

    def test_annotate_policy_folds_verifier_usage_without_rewriting(self) -> None:
        inner = self._inner()
        verdict = _VerdictAdapter('{"pass": false, "score": 0.1, "issues": ["weak"]}')
        adapter = VerifyingAdapter(inner, verifier=ModelVerifier(verdict), policy="annotate")
        result = adapter.run(_node(), {})
        self.assertEqual(result.artifacts, inner.artifacts)
        self.assertEqual(result.envelope["answer"], "42")
        self.assertEqual(result.envelope["claims"], inner.payload["claims"])
        # 模型验证器给出的是整体结论，不带 claim_id，因此警告不带 @claim 后缀。
        self.assertIn("verification:verifier_reported_issue", result.envelope["warnings"])
        self.assertFalse(result.envelope["verification"]["passed"])
        # worker 40 tokens + verifier 15 tokens
        self.assertEqual(result.envelope["usage"]["total_tokens"], 55)
        self.assertAlmostEqual(result.envelope["usage"]["cost"], 0.0004 + 15 / 1000.0 * 0.01)

    def test_fail_policy_raises_on_model_verdict_failure(self) -> None:
        verdict = _VerdictAdapter('{"pass": false, "score": 0.0, "issues": ["unsupported"]}')
        adapter = VerifyingAdapter(self._inner(), verifier=ModelVerifier(verdict), policy="fail")
        with self.assertRaises(AdapterFailure) as caught:
            adapter.run(_node(), {})
        self.assertEqual(caught.exception.kind, "verification")

    def test_scheduler_settles_worker_plus_verifier_cost(self) -> None:
        verdict = _VerdictAdapter('{"pass": true, "score": 1, "issues": []}')
        adapter = VerifyingAdapter(self._inner(), verifier=ModelVerifier(verdict), policy="annotate")
        with tempfile.TemporaryDirectory() as directory:
            run = RunSpec(id="mv", budget_cost=5.0, max_nodes=1, max_workers=1, deadline_seconds=30.0, cost_per_1k_tokens=0.01)
            with DAGScheduler(Path(directory) / "run.sqlite3", max_workers=1) as scheduler:
                scheduler.submit(run, [_node(max_tokens=4000)])
                result = scheduler.execute(adapter)
                budget = scheduler.budget_snapshot("mv")
                stored = scheduler.envelopes("mv")
        self.assertEqual(result.status, "succeeded", result.events)
        self.assertAlmostEqual(result.spent_cost, 55 / 1000.0 * 0.01, places=9)
        self.assertTrue(budget["invariant_ok"])
        self.assertEqual(stored["n1"]["envelope"]["usage"]["total_tokens"], 55)
        self.assertFalse(stored["n1"]["envelope"]["verification"]["passed"] is None)


class VerdictParsingTests(unittest.TestCase):
    def test_parser_accepts_minimal_and_full_verdicts(self) -> None:
        verdict, problems = parse_verifier_verdict('{"pass": true, "score": 0, "issues": []}')
        self.assertEqual(problems, [])
        self.assertEqual(verdict["pass"], True)
        full, problems = parse_verifier_verdict(
            '{"pass": false, "score": 1, "issues": ["a"], "revision_prompt": "fix it"}'
        )
        self.assertEqual(problems, [])
        self.assertEqual(full["revision_prompt"], "fix it")

    def test_parser_rejects_non_boolean_and_out_of_range(self) -> None:
        for text in ('{"pass": 1, "score": 0.5, "issues": []}', '{"pass": true, "score": 1.5, "issues": []}'):
            verdict, problems = parse_verifier_verdict(text)
            self.assertIsNone(verdict)
            self.assertTrue(problems)

    def test_prompt_marks_missing_claims(self) -> None:
        prompt = build_verification_prompt(_node(), _envelope("42", []), {"notes": "42"})
        self.assertIn("<no claims>", prompt)


if __name__ == "__main__":
    unittest.main()
