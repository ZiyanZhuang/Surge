"""Authorized one-case real model-adapter smoke.

The network call is deliberately routed through :class:`DAGScheduler` so the
smoke exercises the public WorkerAdapter boundary, envelope validation, budget
reservation, artifact storage, and failure persistence.  It is still only a
connectivity/quality smoke, not a capacity benchmark or direct Host ``llm``
Service integration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Mapping

from benchmark_real_smoke import file_digest, load_manifest, load_records
from surge_cluster import DAGScheduler, NodeSpec, RunSpec, WorkerResult
from surge_cluster.finqa import check_oracle, compare_answer, execute_program

DEFAULT_URL = "http://127.0.0.1:17800/v1/messages"
DEFAULT_MODEL = "gpt-6.1-sol"
MAX_OUTPUT_TOKENS = 2000
MAX_RESPONSE_BYTES = 4_000_000
_REQUIRED_EVENTS = ("message_start", "content_block_delta", "message_delta", "message_stop")
_NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?%?")


def _prompt(record: dict[str, Any]) -> str:
    qa = record["qa"]
    # Do not include answer/exe_ans: the adapter must calculate from the
    # question and executable program rather than copy a gold field.
    return (
        "Solve this FinQA arithmetic question. Return only a JSON object "
        'with one string field named "answer"; do not include explanation.\n'
        f"question: {qa['question']}\n"
        f"program: {qa['program']}\n"
        f"table: {json.dumps(record.get('table', []), ensure_ascii=False)}"
    )


def _sse_text(payload: bytes) -> tuple[str, dict[str, Any] | None, list[str]]:
    text = payload.decode("utf-8", errors="replace")
    events: list[str] = []
    output: list[str] = []
    usage: dict[str, Any] | None = None
    for frame in text.split("\n\n"):
        event_name = None
        data = None
        for line in frame.splitlines():
            if line.startswith("event:"):
                event_name = line[6:].strip()
            elif line.startswith("data:"):
                raw = line[5:].strip()
                if raw:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        data = None
        if event_name:
            events.append(event_name)
        if not isinstance(data, dict):
            continue
        if event_name == "content_block_delta":
            delta = data.get("delta") or {}
            if delta.get("type") == "text_delta":
                output.append(str(delta.get("text", "")))
        if event_name == "message_delta":
            candidate = data.get("usage")
            if isinstance(candidate, dict):
                usage = candidate
    return "".join(output), usage, events


def _extract_answer(text: str) -> str | None:
    try:
        payload = json.loads(text.strip())
        if isinstance(payload, dict) and isinstance(payload.get("answer"), str):
            return payload["answer"].strip()
    except json.JSONDecodeError:
        pass
    matches = _NUMBER_RE.findall(text)
    return matches[-1] if matches else None


def _require_usage(usage: Mapping[str, Any] | None) -> dict[str, int]:
    if not isinstance(usage, Mapping):
        raise RuntimeError("adapter SSE did not contain message_delta.usage")
    result: dict[str, int] = {}
    for key in ("input_tokens", "output_tokens"):
        value = usage.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeError(f"adapter usage.{key} must be a non-negative integer")
        result[key] = value
    if result["output_tokens"] > MAX_OUTPUT_TOKENS:
        raise RuntimeError(
            f"adapter reported output_tokens={result['output_tokens']} above requested limit "
            f"{MAX_OUTPUT_TOKENS}; failing closed"
        )
    return result


def call_adapter(url: str, model: str, prompt: str, timeout: float) -> dict[str, Any]:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    body = {
        "model": model,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "system": "You are a careful arithmetic benchmark solver.",
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "stream": True,
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "content-type": "application/json",
            "x-api-key": "local",
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read(MAX_RESPONSE_BYTES + 1)
            status = response.status
            content_type = response.headers.get("content-type")
    except urllib.error.HTTPError as exc:
        detail = exc.read(2048).decode("utf-8", errors="replace")
        raise RuntimeError(f"adapter HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"adapter transport failure: {exc.reason}") from exc
    if len(payload) > MAX_RESPONSE_BYTES:
        raise RuntimeError(f"adapter response exceeds {MAX_RESPONSE_BYTES} bytes")
    if status < 200 or status >= 300:
        raise RuntimeError(f"adapter HTTP {status}")
    if not content_type or "text/event-stream" not in content_type.lower():
        raise RuntimeError(f"adapter content-type is not SSE: {content_type!r}")
    text, usage, events = _sse_text(payload)
    missing = [event for event in _REQUIRED_EVENTS if event not in events]
    if missing:
        raise RuntimeError(f"adapter SSE missing events: {', '.join(missing)}")
    positions = [events.index(event) for event in _REQUIRED_EVENTS]
    if positions != sorted(positions):
        raise RuntimeError("adapter SSE event order is incomplete or invalid")
    normalized_usage = _require_usage(usage)
    if not text.strip():
        raise RuntimeError("adapter SSE contained no text output")
    return {
        "http_status": status,
        "content_type": content_type,
        "events": events,
        "text": text,
        "usage": normalized_usage,
        "request_max_tokens": MAX_OUTPUT_TOKENS,
        "output_budget_policy": "fail-closed if provider usage is missing or above request",
    }


class HttpWorkerAdapter:
    """Translate one authorized SSE response into the scheduler contract."""

    def __init__(self, url: str, model: str, timeout: float):
        self.url = url
        self.model = model
        self.timeout = timeout
        self.last_response: dict[str, Any] | None = None
        self.last_error: str | None = None

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        try:
            response = call_adapter(self.url, self.model, node.prompt, self.timeout)
            self.last_response = response
            self.last_error = None
        except Exception as exc:
            self.last_error = str(exc)
            raise
        answer = _extract_answer(response["text"])
        if not answer:
            raise RuntimeError("adapter output did not contain an answer")
        usage = response["usage"]
        total_tokens = usage["input_tokens"] + usage["output_tokens"]
        return WorkerResult(
            envelope={
                "answer": answer,
                "claims": [{
                    "id": "model-answer",
                    "text": answer,
                    "evidence_refs": ["model-response"],
                }],
                "citations": [],
                "confidence": 0.5,
                "warnings": ["single-case model-adapter smoke"],
                "usage": {"total_tokens": total_tokens, "cost": total_tokens / 1000.0 * 0.01},
            },
            artifacts={"model-response": json.dumps(response, ensure_ascii=False)},
        )


def _result_dict(result: Any) -> dict[str, Any]:
    return {
        "run_id": result.run_id,
        "status": result.status,
        "succeeded": list(result.succeeded),
        "failed": list(result.failed),
        "blocked": list(result.blocked),
        "spent_cost": result.spent_cost,
        "events": list(result.events),
    }


def _execute_scheduler(
    *, prompt: str, url: str, model: str, timeout: float, workdir: Path
) -> tuple[dict[str, Any], HttpWorkerAdapter]:
    workdir.mkdir(parents=True, exist_ok=True)
    adapter = HttpWorkerAdapter(url, model, timeout)
    run_spec = RunSpec(
        id="finqa-real-adapter",
        budget_cost=1.0,
        max_nodes=1,
        max_workers=1,
        deadline_seconds=max(timeout + 10.0, 30.0),
        cost_per_1k_tokens=0.01,
    )
    node = NodeSpec(
        id="model-answer",
        prompt=prompt,
        max_attempts=1,
        timeout_seconds=timeout,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    with DAGScheduler(workdir / "run.sqlite3", max_workers=1) as scheduler:
        scheduler.submit(run_spec, [node])
        result = scheduler.execute(adapter)
    return _result_dict(result), adapter


def run(args: argparse.Namespace) -> dict[str, Any]:
    records = load_records(args.fixture)
    manifest = load_manifest(args.manifest, args.fixture, records)
    record = next((item for item in records if item["id"] == args.record_id), None)
    if record is None:
        raise ValueError(f"record id not found in fixture: {args.record_id}")
    qa = record["qa"]
    oracle_trace = execute_program(qa["program"])
    oracle = check_oracle(oracle_trace["value"], qa)
    if not oracle["ok"]:
        raise ValueError(f"fixture oracle rejected selected record: {oracle}")
    prompt = _prompt(record)
    cleanup = tempfile.TemporaryDirectory(prefix="dsh-real-adapter-") if args.workdir is None else None
    workdir = Path(args.workdir) if args.workdir is not None else Path(cleanup.name)
    try:
        scheduler_result, adapter = _execute_scheduler(
            prompt=prompt, url=args.url, model=args.model, timeout=args.timeout, workdir=workdir
        )
    finally:
        if cleanup is not None:
            cleanup.cleanup()
    response = adapter.last_response or {"error": adapter.last_error or "no adapter response"}
    model_answer = _extract_answer(response.get("text", "")) if isinstance(response, dict) else None
    answer_check = compare_answer(model_answer, qa["answer"]) if model_answer else {"ok": False, "error": "no numeric answer"}
    output_usage = (response.get("usage") or {}).get("output_tokens") if isinstance(response, dict) else None
    output_budget_ok = isinstance(output_usage, int) and not isinstance(output_usage, bool) and output_usage <= MAX_OUTPUT_TOKENS
    return {
        "benchmark": "FinQA real model-adapter smoke",
        "mode": "authorized real adapter through DAGScheduler; one case; not a capacity benchmark",
        "adapter": {
            "url": args.url,
            "model": args.model,
            "transport": "Anthropic Messages SSE -> local Codex relay",
            "request_max_tokens": MAX_OUTPUT_TOKENS,
            "timeout_seconds": args.timeout,
            "output_budget_policy": "request plus fail-closed provider usage validation; upstream may ignore request",
        },
        "fixture": str(args.fixture),
        "fixture_sha256": file_digest(args.fixture),
        "manifest": manifest,
        "case": {
            "id": record["id"],
            "question": qa["question"],
            "program": qa["program"],
            "oracle": oracle,
            "oracle_trace": oracle_trace["trace"],
            "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "scheduler": scheduler_result,
            "response": response,
            "model_answer": model_answer,
            "answer_check": answer_check,
            "output_budget_ok": output_budget_ok,
            "passed": scheduler_result["status"] == "succeeded" and bool(answer_check.get("ok")) and output_budget_ok,
        },
        "limitations": [
            "This calls the authorized local reverse-proxy adapter, not the Host llm business Service directly.",
            "One case only; no throughput or model-capability claim.",
            "The output limit is fail-closed after provider usage; the reverse proxy may not enforce upstream max_tokens.",
            "The offline scheduler Gate A remains the authoritative multi-node orchestration evidence.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--record-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--workdir", type=Path, default=None, help="persist scheduler SQLite/artifacts here")
    args = parser.parse_args()
    try:
        report = run(args)
    except Exception as exc:
        report = {"benchmark": "FinQA real model-adapter smoke", "case": {"passed": False}, "error": str(exc)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("case", {}).get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
