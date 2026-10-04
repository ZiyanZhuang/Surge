"""Tests for the independent evidence verifier and claim provenance."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from surge_cluster import (
    AdapterFailure,
    DAGScheduler,
    EvidenceVerifier,
    LocalEchoAdapter,
    NodeSpec,
    RunSpec,
    VerifyingAdapter,
    WorkerResult,
    build_provenance,
)
from surge_cluster.verification import json_safe


def _envelope(answer: str, claims: list[dict[str, Any]], warnings: list[str] | None = None) -> dict[str, Any]:
    return {
        "answer": answer,
        "claims": claims,
        "citations": [],
        "confidence": 0.5,
        "warnings": list(warnings or []),
        "usage": {"total_tokens": 3, "cost": 0.0},
    }


class _StaticAdapter:
    """返回固定 envelope/artifact 的 adapter，用于隔离验证逻辑。"""

    def __init__(self, envelope: Mapping[str, Any], artifacts: Mapping[str, str] | None = None):
        self.envelope = envelope
        self.artifacts = dict(artifacts or {})

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        return WorkerResult(envelope=dict(self.envelope), artifacts=dict(self.artifacts))


class _CitingAdapter:
    """引用依赖节点证据的 adapter，并实际读取其正文。"""

    def __init__(self, ref_key: str, answer: str = "42"):
        self.ref_key = ref_key
        self.answer = answer

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        refs = context.get("artifacts") or {}
        ref = refs.get(self.ref_key)
        if ref is None:
            raise AdapterFailure(f"missing dependency evidence: {self.ref_key}", kind="test")
        reader = context.get("read_artifact")
        content = reader(ref) if callable(reader) else ""
        return WorkerResult(
            envelope=_envelope(
                self.answer,
                [{"id": "c1", "text": "supported by upstream evidence", "evidence_refs": [self.ref_key]}],
            ),
            artifacts={"derived": content[:64]},
        )


def _node(**overrides: Any) -> NodeSpec:
    options: dict[str, Any] = {"id": "n1", "prompt": "work", "max_attempts": 1, "timeout_seconds": 10.0}
    options.update(overrides)
    return NodeSpec(**options)


class _SourceAdapter:
    """产生可被下游引用的证据 artifact，自身不带 claim。"""

    def __init__(self, content: str = "the measured value is 42"):
        self.content = content

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        return WorkerResult(envelope=_envelope("evidence collected", []), artifacts={"source": self.content})


class EvidenceVerifierTests(unittest.TestCase):
    def test_missing_evidence_is_reported(self) -> None:
        report = EvidenceVerifier().verify(_node(), _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": []}]))
        self.assertFalse(report.passed)
        self.assertEqual([issue.code for issue in report.issues], ["missing_evidence"])

    def test_own_evidence_is_flagged_as_self_reference(self) -> None:
        envelope = _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["notes"]}])
        report = EvidenceVerifier().verify(_node(), envelope, own_artifacts={"notes": "42 is the answer"})
        self.assertIn("self_reference", [issue.code for issue in report.issues])

    def test_dependency_evidence_is_not_self_reference(self) -> None:
        envelope = _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["scout/notes"]}])
        report = EvidenceVerifier().verify(
            _node(),
            envelope,
            own_artifacts={},
            dependency_refs={"scout/notes": "artifact:" + "a" * 64},
            read_artifact=lambda _ref: "the measured value is 42",
        )
        self.assertEqual([issue.code for issue in report.issues], [])
        self.assertTrue(report.passed)

    def test_unresolved_reference_is_an_error(self) -> None:
        envelope = _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["nowhere"]}])
        report = EvidenceVerifier().verify(_node(), envelope, own_artifacts={"notes": "42"})
        self.assertIn("unresolved_reference", [issue.code for issue in report.issues])
        self.assertFalse(report.passed)

    def test_unreadable_reference_severity_follows_configuration(self) -> None:
        envelope = _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["scout/notes"]}])
        lenient = EvidenceVerifier().verify(
            _node(), envelope, dependency_refs={"scout/notes": "artifact:" + "b" * 64}
        )
        strict = EvidenceVerifier(require_content=True).verify(
            _node(), envelope, dependency_refs={"scout/notes": "artifact:" + "b" * 64}
        )
        self.assertTrue(lenient.passed)
        self.assertFalse(strict.passed)
        self.assertEqual([issue.code for issue in lenient.issues], ["unreadable_reference"])

    def test_answer_absent_check(self) -> None:
        envelope = _envelope("99", [{"id": "c1", "text": "x", "evidence_refs": ["notes"]}])
        report = EvidenceVerifier(require_answer_in_evidence=True).verify(
            _node(), envelope, own_artifacts={"notes": "the value is 42"}
        )
        self.assertIn("answer_absent", [issue.code for issue in report.issues])

    def test_empty_claim_list_passes(self) -> None:
        report = EvidenceVerifier().verify(_node(), _envelope("42", []))
        self.assertEqual([issue.code for issue in report.issues], [])

    def test_malformed_claim_is_reported(self) -> None:
        envelope = _envelope("42", ["not-an-object"])  # type: ignore[list-item]
        report = EvidenceVerifier().verify(_node(), envelope)
        self.assertEqual([issue.code for issue in report.issues], ["malformed_claim"])


class VerifyingAdapterTests(unittest.TestCase):
    def _inner(self) -> _StaticAdapter:
        return _StaticAdapter(
            _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["notes"]}], warnings=["original"]),
            {"notes": "42"},
        )

    def test_annotate_policy_does_not_rewrite_the_result(self) -> None:
        inner = self._inner()
        adapter = VerifyingAdapter(inner, policy="annotate")
        result = adapter.run(_node(), {})
        self.assertEqual(result.artifacts, inner.artifacts)
        self.assertEqual(result.envelope["answer"], "42")
        self.assertEqual(result.envelope["claims"], inner.envelope["claims"])
        self.assertEqual(result.envelope["usage"], inner.envelope["usage"])
        self.assertTrue(result.envelope["warnings"][0] == "original")
        self.assertIn("verification:self_reference@c1", result.envelope["warnings"])
        self.assertFalse(result.envelope["verification"]["passed"])

    def test_fail_policy_raises_and_keeps_kind(self) -> None:
        adapter = VerifyingAdapter(self._inner(), policy="fail")
        with self.assertRaises(AdapterFailure) as caught:
            adapter.run(_node(), {})
        self.assertEqual(caught.exception.kind, "verification")

    def test_verification_report_summarizes_records(self) -> None:
        adapter = VerifyingAdapter(self._inner(), policy="annotate")
        adapter.run(_node(), {})
        summary = adapter.verification_report()
        self.assertEqual(summary["nodes_verified"], 1)
        self.assertEqual(summary["nodes_passed"], 0)
        self.assertEqual(summary["records"][0]["node_id"], "n1")

    def test_invalid_configuration_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            VerifyingAdapter(object())
        with self.assertRaises(ValueError):
            VerifyingAdapter(LocalEchoAdapter(), policy="ignore")


class VerifierSchedulerIntegrationTests(unittest.TestCase):
    def test_dependency_evidence_passes_through_the_scheduler(self) -> None:
        verifier = EvidenceVerifier(require_content=True)
        hybrid = _HybridAdapter(_SourceAdapter(), _CitingAdapter("scout/source"))
        nodes = [
            _node(id="scout"),
            _node(id="verify", depends_on=("scout",), wave=1),
        ]
        with tempfile.TemporaryDirectory() as directory:
            run = RunSpec(id="verify-run", budget_cost=1.0, max_nodes=2, max_workers=1, deadline_seconds=30.0)
            with DAGScheduler(Path(directory) / "run.sqlite3", max_workers=1) as scheduler:
                scheduler.submit(run, nodes)
                result = scheduler.execute(VerifyingAdapter(hybrid, verifier=verifier, policy="fail"))
                artifacts = scheduler.artifacts("verify-run")
        self.assertEqual(result.status, "succeeded", result.events)
        self.assertEqual(len(artifacts), 2)

    def test_verifier_failure_blocks_downstream_in_fail_policy(self) -> None:
        weak = _StaticAdapter(_envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["notes"]}]), {"notes": "42"})
        nodes = [_node(id="first"), _node(id="second", depends_on=("first",), wave=1)]
        adapter = VerifyingAdapter(_HybridAdapter(weak, weak), policy="fail")
        with tempfile.TemporaryDirectory() as directory:
            run = RunSpec(id="fail-run", budget_cost=1.0, max_nodes=2, max_workers=1, deadline_seconds=30.0)
            with DAGScheduler(Path(directory) / "run.sqlite3", max_workers=1) as scheduler:
                scheduler.submit(run, nodes)
                result = scheduler.execute(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(list(result.failed), ["first"])
        self.assertEqual(list(result.blocked), ["second"])


class _HybridAdapter:
    """按节点 id 分派到不同内层 adapter，便于在一条 DAG 上混合角色。"""

    def __init__(self, first: Any, second: Any, first_id: str = "scout"):
        self.first = first
        self.second = second
        self.first_id = first_id

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        if node.id == self.first_id:
            return self.first.run(node, context)
        # 把 first_id 作为依赖短名来源
        patched = dict(context)
        refs = dict(patched.get("artifacts") or {})
        patched["artifacts"] = refs
        return self.second.run(node, patched)


class _FakeScheduler:
    """只实现 provenance 需要的三个只读方法，且读取方不做 digest 校验。"""

    def __init__(self, digest: str, content: str = "tampered content"):
        self.digest = digest
        self.content = content

    def snapshot(self, run_id: str) -> list[dict[str, Any]]:
        return [{"id": "n1", "status": "succeeded", "attempt_count": 1}]

    def artifacts(self, run_id: str) -> list[dict[str, Any]]:
        return [{"node_id": "n1", "name": "a", "ref": f"artifact:{self.digest}", "sha256": self.digest}]

    def read_artifact(self, run_id: str, ref: str) -> str:
        return self.content


class ProvenanceTests(unittest.TestCase):
    def _run_once(self, directory: str) -> tuple[Path, Any, dict[str, Any]]:
        db = Path(directory) / "prov.sqlite3"
        adapter = _StaticAdapter(
            _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["source"]}]),
            {"source": "the measured value is 42"},
        )
        run = RunSpec(id="prov", budget_cost=1.0, max_nodes=1, max_workers=1, deadline_seconds=30.0)
        with DAGScheduler(db, max_workers=1) as scheduler:
            scheduler.submit(run, [_node(id="only")])
            result = scheduler.execute(adapter)
            report = build_provenance(
                scheduler,
                "prov",
                claims_by_node={"only": [{"id": "c1", "text": "x", "evidence_refs": ["source"]}]},
            )
        return db, result, report.as_dict()

    def test_provenance_rebuilds_nodes_artifacts_and_claims(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, result, report = self._run_once(directory)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(report["node_count"], 1)
        self.assertEqual(report["artifact_count"], 1)
        self.assertTrue(report["claims_available"])
        self.assertEqual(report["claim_count"], 1)
        kinds = {edge["kind"] for edge in report["edges"]}
        self.assertEqual(kinds, {"produced", "cites"})
        self.assertEqual(report["finding_count"], 0)

    def test_dangling_reference_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "prov.sqlite3"
            adapter = _StaticAdapter(_envelope("42", []), {"source": "42"})
            run = RunSpec(id="prov", budget_cost=1.0, max_nodes=1, max_workers=1, deadline_seconds=30.0)
            with DAGScheduler(db, max_workers=1) as scheduler:
                scheduler.submit(run, [_node(id="only")])
                scheduler.execute(adapter)
                report = build_provenance(
                    scheduler, "prov", claims_by_node={"only": [{"id": "c1", "evidence_refs": ["ghost"]}]}
                ).as_dict()
        codes = [finding["code"] for finding in report["findings"]]
        self.assertIn("dangling_reference", codes)
        self.assertIn("uncited_artifact", codes)

    def test_tampered_artifact_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db, _, _ = self._run_once(directory)
            root = Path(directory) / "prov-artifacts" / hashlib.sha256(b"prov").hexdigest()
            target = next(path for path in root.iterdir() if path.is_file())
            target.write_text("tampered", encoding="utf-8")
            with DAGScheduler(db, max_workers=1) as scheduler:
                report = build_provenance(scheduler, "prov").as_dict()
        codes = [finding["code"] for finding in report["findings"]]
        # ArtifactStore 自身就会拒绝 digest 不匹配的文件，因此表现为不可读。
        self.assertIn("unreadable_artifact", codes)

    def test_digest_mismatch_is_detected_for_non_validating_readers(self) -> None:
        """对不做校验的读取方，provenance 仍要独立发现内容与 digest 不一致。"""
        content = "the measured value is 42"
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        report = build_provenance(_FakeScheduler(digest), "fake").as_dict()
        codes = [finding["code"] for finding in report["findings"]]
        self.assertIn("digest_mismatch", codes)

    def test_claims_are_recovered_from_persisted_envelopes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db, _, _ = self._run_once(directory)
            with DAGScheduler(db, max_workers=1) as scheduler:
                report = build_provenance(scheduler, "prov").as_dict()
        self.assertTrue(report["claims_available"])
        self.assertEqual(report["envelope_source"], "persisted")
        self.assertEqual(report["claim_count"], 1)

    def test_missing_envelope_is_declared(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db, _, _ = self._run_once(directory)
            with DAGScheduler(db, max_workers=1) as scheduler:
                scheduler._conn.execute("UPDATE attempts SET envelope=NULL")
                scheduler._conn.commit()
                report = build_provenance(scheduler, "prov").as_dict()
        self.assertFalse(report["claims_available"])
        self.assertIn("no_persisted_envelopes", [item["code"] for item in report["findings"]])

    def test_json_safe_handles_unserializable_values(self) -> None:
        self.assertEqual(json_safe({"a": 1}), {"a": 1})
        self.assertIn("error", json_safe({"a": float("nan")}))
        self.assertEqual(json_safe({"a": 1}), json.loads(json.dumps({"a": 1})))


if __name__ == "__main__":
    unittest.main()
