"""Tests for envelope persistence.

落库的目标是让 claim 级 provenance 在运行结束后仍可重建，因此这里同时验证
成功、验证失败、adapter 异常三条路径，以及超限时的"投影而非截断"行为。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from surge_cluster import DAGScheduler, NodeSpec, RunSpec, WorkerResult, build_provenance
from surge_cluster.core import encode_envelope_record


def _envelope(answer: str, claims: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    payload = {
        "answer": answer,
        "claims": claims if claims is not None else [],
        "citations": [],
        "confidence": 0.5,
        "warnings": [],
        "usage": {"total_tokens": 4, "cost": 0.0},
    }
    payload.update(extra)
    return payload


class _StaticAdapter:
    def __init__(self, payload: Mapping[str, Any], artifacts: Mapping[str, str] | None = None):
        self.payload = dict(payload)
        self.artifacts = dict(artifacts or {})

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        return WorkerResult(envelope=dict(self.payload), artifacts=dict(self.artifacts))


class _RaisingAdapter:
    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        raise RuntimeError("adapter exploded")


def _node(**overrides: Any) -> NodeSpec:
    options: dict[str, Any] = {"id": "n1", "prompt": "work", "max_attempts": 1, "timeout_seconds": 10.0}
    options.update(overrides)
    return NodeSpec(**options)


class EnvelopePersistenceTests(unittest.TestCase):
    def _execute(self, directory: str, adapter: Any, nodes: list[NodeSpec], run_id: str = "env"):
        db = Path(directory) / "env.sqlite3"
        run = RunSpec(id=run_id, budget_cost=5.0, max_nodes=max(1, len(nodes)), max_workers=1, deadline_seconds=30.0)
        with DAGScheduler(db, max_workers=1) as scheduler:
            scheduler.submit(run, nodes)
            result = scheduler.execute(adapter)
            records = scheduler.envelopes(run_id)
        return db, result, records

    def test_success_is_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, result, records = self._execute(
                directory,
                _StaticAdapter(_envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["notes"]}]), {"notes": "42"}),
                [_node()],
            )
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(records["n1"]["status"], "succeeded")
        self.assertEqual(records["n1"]["envelope"]["answer"], "42")
        self.assertEqual(records["n1"]["envelope"]["claims"][0]["id"], "c1")

    def test_validation_failure_is_persisted(self) -> None:
        broken = _envelope("42")
        broken["citations"] = "not-a-list"  # strict 校验失败
        with tempfile.TemporaryDirectory() as directory:
            _, result, records = self._execute(directory, _StaticAdapter(broken), [_node()])
        self.assertEqual(result.status, "failed")
        self.assertEqual(records["n1"]["status"], "failed")
        self.assertEqual(records["n1"]["envelope"]["answer"], "42")

    def test_adapter_exception_leaves_no_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, result, records = self._execute(directory, _RaisingAdapter(), [_node()])
        self.assertEqual(result.status, "failed")
        self.assertEqual(records, {})

    def test_oversized_envelope_is_stored_as_projection_not_truncated(self) -> None:
        answer = "x" * 400_000
        payload = _envelope(answer, [{"id": "c1", "text": "t", "evidence_refs": ["notes"]}])
        node = _node(validation_policy="lenient")
        with tempfile.TemporaryDirectory() as directory:
            _, result, records = self._execute(directory, _StaticAdapter(payload, {"notes": "x"}), [node])
        self.assertEqual(result.status, "succeeded")
        stored = records["n1"]["envelope"]
        self.assertEqual(records["n1"]["persisted"], "projection")
        self.assertIsNone(stored["answer"])
        self.assertEqual(stored["answer_sha256"], hashlib.sha256(answer.encode("utf-8")).hexdigest())
        self.assertEqual(stored["claims"][0]["id"], "c1")
        self.assertTrue(any("projection" in item for item in stored["warnings"]))

    def test_malformed_stored_envelope_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db, _, _ = self._execute(directory, _StaticAdapter(_envelope("42")), [_node()])
            with DAGScheduler(db, max_workers=1) as scheduler:
                scheduler._conn.execute("UPDATE attempts SET envelope='{not json'")
                scheduler._conn.commit()
                records = scheduler.envelopes("env")
        self.assertIsNone(records["n1"]["envelope"])
        self.assertIn("not valid JSON", records["n1"]["error"])
        self.assertEqual(records["n1"]["attempt_number"], 1)

    def test_provenance_rebuilds_claims_from_persisted_envelopes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db, _, _ = self._execute(
                directory,
                _StaticAdapter(
                    _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["notes"]}]),
                    {"notes": "42"},
                ),
                [_node(id="only")],
            )
            with DAGScheduler(db, max_workers=1) as scheduler:
                report = build_provenance(scheduler, "env").as_dict()
        self.assertTrue(report["claims_available"])
        self.assertEqual(report["envelope_source"], "persisted")
        self.assertEqual(report["claim_count"], 1)
        self.assertIn("cites", {edge["kind"] for edge in report["edges"]})

    def test_provenance_declares_when_no_envelope_was_stored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db, _, _ = self._execute(directory, _RaisingAdapter(), [_node()])
            with DAGScheduler(db, max_workers=1) as scheduler:
                report = build_provenance(scheduler, "env").as_dict()
        self.assertFalse(report["claims_available"])
        self.assertIn("no_persisted_envelopes", [item["code"] for item in report["findings"]])


class EnvelopeEncodingTests(unittest.TestCase):
    def test_within_limit_is_stored_verbatim(self) -> None:
        payload = _envelope("42", [{"id": "c1", "text": "x", "evidence_refs": ["notes"]}])
        stored = encode_envelope_record(payload, max_bytes=10_000)
        self.assertEqual(json.loads(stored), payload)

    def test_projection_keeps_claims_and_hashes_the_answer(self) -> None:
        answer = "y" * 5_000
        stored = json.loads(
            encode_envelope_record(_envelope(answer, [{"id": "c1", "text": "t", "evidence_refs": []}]), max_bytes=1_000)
        )
        self.assertEqual(stored["persisted"], "projection")
        self.assertEqual(stored["claims"][0]["id"], "c1")
        self.assertEqual(stored["answer_sha256"], hashlib.sha256(answer.encode("utf-8")).hexdigest())
        self.assertEqual(stored["answer_chars"], 5_000)

    def test_unserializable_envelope_is_reported(self) -> None:
        stored = json.loads(encode_envelope_record({"answer": float("nan")}, max_bytes=1_000))
        self.assertEqual(stored["persisted"], "unavailable")
        self.assertIn("not JSON serializable", stored["reason"])

    def test_projection_that_still_exceeds_the_limit_is_declared_unavailable(self) -> None:
        claims = [{"id": f"c{i}", "text": "t" * 100, "evidence_refs": ["notes"]} for i in range(200)]
        stored = json.loads(encode_envelope_record(_envelope("z" * 5_000, claims), max_bytes=1_000))
        self.assertEqual(stored["persisted"], "unavailable")
        self.assertIn("projection still exceeds", stored["reason"])


class EnvelopeMigrationTests(unittest.TestCase):
    def test_previous_schema_gains_envelope_column(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "legacy.sqlite3"
            connection = sqlite3.connect(db)
            connection.executescript(
                """
                CREATE TABLE attempts (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL,
                    owner_id TEXT, number INTEGER NOT NULL, status TEXT NOT NULL,
                    lease_until REAL NOT NULL, started_at REAL NOT NULL, finished_at REAL,
                    error TEXT, cost REAL NOT NULL DEFAULT 0
                );
                """
            )
            connection.close()
            with DAGScheduler(db, max_workers=1) as scheduler:
                columns = {row[1] for row in scheduler._conn.execute("PRAGMA table_info(attempts)")}
                version = scheduler._conn.execute("PRAGMA user_version").fetchone()[0]
        self.assertIn("envelope", columns)
        self.assertEqual(version, 3)


if __name__ == "__main__":
    unittest.main()
