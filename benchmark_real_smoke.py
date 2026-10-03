"""Run the first real-benchmark smoke gate with a deterministic replay adapter.

The fixture contains real FinQA records, but this script deliberately does not
call a model. It validates the local DAG/evidence/budget contract before a
separate Host-side DSH adapter is introduced.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from surge_cluster import DAGScheduler, NodeSpec, RunSpec, WorkerResult
from surge_cluster.finqa import check_oracle, execute_program


NODE_IDS = ("scout-text", "scout-table", "deepen", "verify-calc", "verify-evidence", "synthesize")
WAVE_BY_NODE = {
    "scout-text": 0,
    "scout-table": 0,
    "deepen": 1,
    "verify-calc": 2,
    "verify-evidence": 2,
    "synthesize": 3,
}


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_manifest(path: Path, fixture: Path, records: list[dict[str, Any]]) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest must be a JSON object")
    required = ("source_url", "revision", "split", "fixture_sha256", "record_ids")
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"manifest missing required fields: {', '.join(missing)}")
    if not isinstance(payload["source_url"], str) or not payload["source_url"].startswith(("http://", "https://")):
        raise ValueError("manifest source_url must be an HTTP(S) URL")
    if not isinstance(payload["record_ids"], list):
        raise ValueError("manifest record_ids must be a list")
    if payload["fixture_sha256"] != file_digest(fixture):
        raise ValueError("manifest fixture_sha256 does not match fixture bytes")
    actual_ids = [str(record["id"]) for record in records]
    declared_ids = [str(value) for value in payload["record_ids"]]
    if actual_ids != declared_ids:
        raise ValueError("manifest record_ids must match fixture order and ids")
    return payload


def load_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(
            f"FinQA fixture not found: {path}. Provide tests/fixtures/finqa/smoke.jsonl first."
        )
    if path.suffix.lower() == ".jsonl":
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
            records = payload["data"]
        elif isinstance(payload, dict) and isinstance(payload.get("qa"), Mapping):
            records = [payload]
        else:
            raise ValueError("fixture JSON must be a record, a list, or an object with a data list")
    if not records:
        raise ValueError("FinQA fixture is empty")
    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"fixture record {index} must be an object")
        if not isinstance(record.get("id"), (str, int)):
            raise ValueError(f"fixture record {index} requires id")
        qa = record.get("qa")
        if not isinstance(qa, Mapping):
            raise ValueError(f"fixture record {record['id']} requires qa object")
        for field in ("question", "answer", "program", "exe_ans"):
            if field not in qa:
                raise ValueError(f"fixture record {record['id']} requires qa.{field}")
        if not isinstance(record.get("table"), list):
            raise ValueError(f"fixture record {record['id']} requires table list")
    return records


def dependency_refs(context: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    for ref in context.get("artifacts", {}).values():
        if isinstance(ref, str) and re.fullmatch(r"artifact:[0-9a-f]{64}", ref) and ref not in refs:
            refs.append(ref)
    return refs


def artifact_ref(text: str) -> str:
    return f"artifact:{digest(text)}"


def envelope(answer: str, claims: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "answer": answer,
        "claims": claims,
        "citations": [],
        "confidence": 1.0,
        "warnings": ["FinQA replay adapter; no model call"],
        "usage": {"total_tokens": 1, "cost": 0.0},
    }


class FinQAReplayAdapter:
    """Replay a real FinQA record with a bounded, independent arithmetic oracle."""

    def __init__(self, record: Mapping[str, Any]):
        self.record = record
        self.final_answer: str | None = None
        self.calls: list[str] = []
        self.oracle: dict[str, Any] | None = None
        self.oracle_error: str | None = None

    def _oracle(self) -> dict[str, Any]:
        if self.oracle is not None:
            return self.oracle
        try:
            qa = self._qa()
            trace = execute_program(str(qa["program"]))
            check = check_oracle(trace["value"], qa)
            self.oracle = {"trace": trace, "check": check}
            if not check["ok"]:
                raise ValueError(f"program oracle mismatch: {stable_json(check)}")
            return self.oracle
        except (KeyError, TypeError, ValueError) as exc:
            self.oracle_error = str(exc)
            raise RuntimeError(f"FinQA program oracle failed: {exc}") from exc

    def _qa(self) -> Mapping[str, Any]:
        qa = self.record["qa"]
        assert isinstance(qa, Mapping)
        return qa

    def _claim(self, claim_id: str, text: str, refs: list[str]) -> dict[str, Any]:
        if not refs:
            raise RuntimeError(f"replay node {claim_id} has no evidence refs")
        return {"id": claim_id, "text": text, "evidence_refs": refs}

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        self.calls.append(node.id)
        qa = self._qa()
        if node.id == "scout-text":
            content = stable_json({"pre_text": self.record.get("pre_text", []), "post_text": self.record.get("post_text", [])})
            ref = artifact_ref(content)
            return WorkerResult(
                envelope("text evidence extracted", [self._claim("text-evidence", "Relevant financial context was extracted.", [ref])]),
                {"text-evidence": content},
            )
        if node.id == "scout-table":
            content = stable_json({"table": self.record["table"], "question": qa["question"]})
            ref = artifact_ref(content)
            return WorkerResult(
                envelope("table evidence extracted", [self._claim("table-evidence", "Relevant table fields were extracted.", [ref])]),
                {"table-evidence": content},
            )
        refs = dependency_refs(context)
        if node.id == "deepen":
            oracle = self._oracle()
            content = stable_json({"program": qa["program"], "trace": oracle["trace"], "oracle": oracle["check"]})
            own_ref = artifact_ref(content)
            all_refs = refs + [own_ref]
            return WorkerResult(
                envelope("gold program replayed by bounded oracle", [self._claim("program-replay", "The benchmark program was executed without a model call.", all_refs)]),
                {"program": content},
            )
        if node.id == "verify-calc":
            oracle = self._oracle()
            if not oracle["check"]["ok"]:
                raise RuntimeError("calculation oracle did not pass")
            content = stable_json({"program": qa["program"], "computed": oracle["trace"]["value"], "oracle": oracle["check"]})
            return WorkerResult(
                envelope("calculation verified by oracle", [self._claim("calculation-check", "The bounded arithmetic oracle matched FinQA exe_ans and answer.", refs)]),
                {"calculation-check": content},
            )
        if node.id == "verify-evidence":
            content = stable_json({"gold_inds": qa.get("gold_inds", []), "evidence_refs": refs})
            return WorkerResult(
                envelope("evidence verified", [self._claim("evidence-check", "The fixture evidence provenance is retained.", refs)]),
                {"evidence-check": content},
            )
        if node.id == "synthesize":
            oracle = self._oracle()
            if not oracle["check"]["ok"]:
                raise RuntimeError("cannot synthesize before calculation oracle passes")
            # FinQA's display answer is preserved only after the independent numeric
            # oracle has matched it; this is replay, not a claim of model generation.
            answer = str(qa["answer"])
            self.final_answer = answer
            content = stable_json({"answer": answer, "computed": oracle["trace"]["value"], "id": self.record["id"]})
            return WorkerResult(
                envelope(answer, [self._claim("final-answer", "The gold display answer is emitted after oracle verification.", refs)]),
                {"final-answer": content},
            )
        raise ValueError(f"unknown smoke node: {node.id}")


def make_nodes() -> list[NodeSpec]:
    return [
        NodeSpec("scout-text", "extract relevant narrative evidence", wave=0, max_attempts=1, timeout_seconds=10, max_tokens=256),
        NodeSpec("scout-table", "extract relevant table evidence", wave=0, max_attempts=1, timeout_seconds=10, max_tokens=256),
        NodeSpec("deepen", "replay the benchmark calculation program", depends_on=("scout-text", "scout-table"), wave=1, max_attempts=1, timeout_seconds=10, max_tokens=256),
        NodeSpec("verify-calc", "verify the numerical calculation", depends_on=("deepen",), wave=2, max_attempts=1, timeout_seconds=10, max_tokens=256),
        NodeSpec("verify-evidence", "verify evidence provenance", depends_on=("deepen",), wave=2, max_attempts=1, timeout_seconds=10, max_tokens=256),
        NodeSpec("synthesize", "synthesize the verified benchmark answer", depends_on=("verify-calc", "verify-evidence"), wave=3, max_attempts=1, timeout_seconds=10, max_tokens=256),
    ]


def check_gate(events: list[dict[str, Any]], snapshot: list[dict[str, Any]], result_status: str, final_answer: str | None, expected_answer: str) -> dict[str, Any]:
    started = {event["node_id"]: index for index, event in enumerate(events) if event["event"] == "task.started"}
    completed = {event["node_id"]: index for index, event in enumerate(events) if event["event"] == "task.completed"}
    gate_order_ok = True
    for node_id, wave in WAVE_BY_NODE.items():
        if wave == 0:
            continue
        prior_completed = [completed[other] for other, other_wave in WAVE_BY_NODE.items() if other_wave < wave and other in completed]
        if node_id not in started or not prior_completed or started[node_id] <= max(prior_completed):
            gate_order_ok = False
    statuses_ok = result_status == "succeeded" and all(row["status"] == "succeeded" for row in snapshot)
    return {
        "passed": statuses_ok and gate_order_ok and final_answer == expected_answer,
        "status_ok": statuses_ok,
        "wave_gate_order_ok": gate_order_ok,
        "answer_match": final_answer == expected_answer,
        "expected_answer": expected_answer,
        "replayed_answer": final_answer,
        "snapshot": snapshot,
        "events": events,
    }


def run_case(record: Mapping[str, Any], index: int, workdir: Path) -> dict[str, Any]:
    run_id = f"finqa-{index:03d}"
    case_dir = workdir / run_id
    case_dir.mkdir(parents=True, exist_ok=False)
    db_path = case_dir / "run.sqlite3"
    adapter = FinQAReplayAdapter(record)
    nodes = make_nodes()
    with DAGScheduler(db_path, max_workers=2) as scheduler:
        scheduler.submit(RunSpec(run_id, budget_cost=1.0, max_nodes=6, max_workers=2, deadline_seconds=60), nodes)
        result = scheduler.execute(adapter)
        snapshot = scheduler.snapshot(run_id)
        run_row = scheduler._conn.execute("SELECT status,budget_cost,reserved_cost,spent_cost FROM runs WHERE id=?", (run_id,)).fetchone()
        artifacts = scheduler._conn.execute("SELECT node_id,name,ref,sha256 FROM artifacts WHERE run_id=? ORDER BY node_id,name", (run_id,)).fetchall()
        events = [dict(row) for row in scheduler._conn.execute("SELECT id,node_id,event,detail FROM events WHERE run_id=? ORDER BY id", (run_id,))]
    gate = check_gate(events, snapshot, result.status, adapter.final_answer, str(record["qa"]["answer"]))
    gate.update(
        {
            "id": record["id"],
            "run_id": run_id,
            "result_status": result.status,
            "succeeded": list(result.succeeded),
            "failed": list(result.failed),
            "blocked": list(result.blocked),
            "artifacts": [dict(row) for row in artifacts],
            "artifact_count": len(artifacts),
            "budget": dict(run_row),
            "calls": adapter.calls,
            "oracle": adapter.oracle,
            "oracle_error": adapter.oracle_error,
            "db": str(db_path),
        }
    )
    gate["oracle_ok"] = bool(adapter.oracle and adapter.oracle.get("check", {}).get("ok"))
    gate["budget_invariant_ok"] = float(run_row["spent_cost"]) + float(run_row["reserved_cost"]) <= float(run_row["budget_cost"]) + 1e-12
    gate["passed"] = gate["passed"] and gate["artifact_count"] == len(NODE_IDS) and gate["budget_invariant_ok"] and gate["oracle_ok"]
    return gate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, help="provenance manifest for the fixture")
    parser.add_argument("--workdir", type=Path, default=Path("smoke-runs"))
    parser.add_argument("--output", type=Path, default=Path("smoke-results") / "finqa-replay.json")
    parser.add_argument("--limit", type=int, default=3)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    try:
        all_records = load_records(args.fixture)
        manifest = load_manifest(args.manifest, args.fixture, all_records) if args.manifest else None
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    records = all_records[: args.limit]
    args.workdir.mkdir(parents=True, exist_ok=True)
    reports = [run_case(record, index, args.workdir) for index, record in enumerate(records, start=1)]
    output = {
        "benchmark": "FinQA local replay smoke",
        "mode": "offline replay; no model call",
        "generator": {
            "script": "benchmark_real_smoke.py",
            "oracle": "surge_cluster.finqa.execute_program",
            "node_ids": list(NODE_IDS),
            "schema_version": 2,
        },
        "fixture": str(args.fixture),
        "fixture_sha256": file_digest(args.fixture),
        "manifest": manifest,
        "summary": {
            "case_count": len(reports),
            "passed_count": sum(report["passed"] for report in reports),
            "all_passed": all(report["passed"] for report in reports),
        },
        "cases": reports,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if all(report["passed"] for report in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
