"""Authorized one-case real model-adapter smoke.

网络调用刻意经过 :class:`DAGScheduler`，因此这次烟测走的是公开的
``WorkerAdapter`` 边界、envelope 校验、预算预留、artifact 存储和失败持久化。
它仍然只是连通性与单案例质量烟测，不是容量基准，也不是直接的 DSH Host
``llm`` Service 集成。

适配器实现已提升到 :mod:`surge_cluster.http_adapter`；本脚本保留同名的薄包装，
以便历史证据和既有 harness 测试继续指向同一份实现。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmark_real_smoke import file_digest, load_manifest, load_records
from surge_cluster import (
    DAGScheduler,
    HttpWorkerAdapter,
    NodeSpec,
    RunSpec,
    call_messages_endpoint,
    extract_numeric_answer,
)
from surge_cluster.finqa import check_oracle, compare_answer, execute_program
from surge_cluster.http_adapter import (
    DEFAULT_MAX_OUTPUT_TOKENS,
    DEFAULT_MAX_RESPONSE_BYTES,
    parse_sse,
    require_usage,
)

DEFAULT_URL = "http://127.0.0.1:17800/v1/messages"
DEFAULT_MODEL = "gpt-6.1-sol"
MAX_OUTPUT_TOKENS = DEFAULT_MAX_OUTPUT_TOKENS
MAX_RESPONSE_BYTES = DEFAULT_MAX_RESPONSE_BYTES

# 兼容旧引用：解析与 usage 校验现在由库实现。
_sse_text = parse_sse
_extract_answer = extract_numeric_answer


def _require_usage(usage: Any) -> dict[str, int]:
    return require_usage(usage, MAX_OUTPUT_TOKENS)


def call_adapter(url: str, model: str, prompt: str, timeout: float) -> dict[str, Any]:
    """薄包装：固定本烟测的输出上限，调用库中的 Messages 端点实现。"""
    return call_messages_endpoint(
        url,
        model,
        prompt,
        timeout,
        max_output_tokens=MAX_OUTPUT_TOKENS,
        max_response_bytes=MAX_RESPONSE_BYTES,
    )


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
    adapter = HttpWorkerAdapter(
        url,
        model,
        timeout=timeout,
        max_output_tokens=MAX_OUTPUT_TOKENS,
        answer_extractor=extract_numeric_answer,
        warning="single-case model-adapter smoke",
    )
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
        # 时间戳与环境快照使这一次运行事后可核对；早期记录缺少这些字段。
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": {"python": sys.version.split()[0], "platform": platform.platform()},
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
        "adapter_calls": list(adapter.calls),
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
    # stdout 使用 ASCII 转义，避免在 cp936/GBK 控制台或管道中出现乱码字节。
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 0 if report.get("case", {}).get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
