"""Gate C：三案例真实并发烟测（``max_workers=2``）。

与 Gate B 的区别
----------------
Gate B 是单案例、单节点的连通性烟测；Gate C 在**同一次 run** 内提交 3 个案例的
DAG，用 ``max_workers=2`` 限制在飞调用数，因此它检验的是：

* 真实 provider 下的**有界并发**与路由容量（第三个 wave-0 节点必须等待槽位）；
* 预算 reservation/settlement 在并发下仍满足 ``spent + reserved <= budget``；
* lease heartbeat 与失败分类在真实调用路径上被记录；
* 确定性 check 节点能否对**模型自己产出的 artifact** 做独立核对。

每个案例两个节点：``caseN/solve`` 真实调用模型并落盘响应，``caseN/check`` 只读该
artifact、用冻结的 FinQA oracle 独立复核。模型节点自己的 claim 只有一个自引用证据，
check 节点提供的是外部证据；报告会把这两种情况分开显示。

边界：一次 Gate C 只说明这三个案例在当前 endpoint 上完成，不构成 provider 容量、
模型质量或多案例稳定性结论。反代必须由使用者显式启动与授权。
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import shutil
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from benchmark_real_smoke import file_digest, load_manifest, load_records
from surge_cluster import (
    DAGScheduler,
    EvidenceVerifier,
    HttpWorkerAdapter,
    IsolatedAdapter,
    NodeSpec,
    RunSpec,
    VerifyingAdapter,
    WorkerResult,
    build_provenance,
    extract_numeric_answer,
)
from surge_cluster.finqa import check_oracle, compare_answer, execute_program, question_prompt
from surge_cluster.http_adapter import ARTIFACT_NAME
from surge_cluster.verification import content_digest

DEFAULT_URL = "http://127.0.0.1:17800/v1/messages"
DEFAULT_MODEL = "gpt-6.1-sol"
DEFAULT_MAX_OUTPUT_TOKENS = 2000
DEFAULT_NODE_MAX_TOKENS = 8000
DEFAULT_COST_PER_1K_TOKENS = 0.01
DEFAULT_BUDGET_COST = 5.0
DEFAULT_WORKERS = 2
DEFAULT_CASES = 3
# 保守估算：按 3 字符/token 高估输入长度，宁可预留更多也不要在中途撞上硬预算。
CHARS_PER_TOKEN = 3
CHECK_NODE_MAX_TOKENS = 1000

LIMITATIONS = [
    "Three cases on one endpoint: no provider capacity, quota or stability claim.",
    "The reverse proxy must be started and authorized by the operator; this script never starts it.",
    "FinQA is a financial question-answering fixture; scientific-domain quality is unverified.",
    "Python thread isolation is cooperative; use --isolate for hard per-call timeouts.",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def estimate_tokens(text: str) -> int:
    """保守 token 估算：向上取整的字符数/3。"""
    return max(1, math.ceil(len(text) / CHARS_PER_TOKEN))


def build_nodes(
    records: list[Mapping[str, Any]], *, node_max_tokens: int, timeout: float
) -> list[NodeSpec]:
    """每个案例两个节点：真实 solve + 确定性 check。"""
    nodes: list[NodeSpec] = []
    for index, record in enumerate(records, start=1):
        prefix = f"case{index}"
        nodes.append(
            NodeSpec(
                id=f"{prefix}/solve",
                prompt=question_prompt(record),
                wave=0,
                max_attempts=1,
                timeout_seconds=timeout,
                max_tokens=node_max_tokens,
                stage="scout",
                route_class="default",
            )
        )
        nodes.append(
            NodeSpec(
                id=f"{prefix}/check",
                prompt="deterministic oracle check over the persisted model response",
                depends_on=(f"{prefix}/solve",),
                wave=1,
                max_attempts=1,
                timeout_seconds=30.0,
                max_tokens=CHECK_NODE_MAX_TOKENS,
                stage="verify",
            )
        )
    return nodes


def preflight_prompts(
    labels_and_prompts: Iterable[tuple[str, str]],
    *,
    node_max_tokens: int,
    max_output_tokens: int,
) -> list[str]:
    """在花钱之前检查预算口径是否覆盖输入+输出，避免中途硬预算失败。

    Gate C 与 Gate D 共用同一判定：``估算输入 + 输出上限 <= node max_tokens``。
    """
    problems: list[str] = []
    for label, prompt in labels_and_prompts:
        estimated_input = estimate_tokens(prompt)
        needed = estimated_input + max_output_tokens
        if needed > node_max_tokens:
            problems.append(
                f"{label}: estimated input {estimated_input} + output ceiling {max_output_tokens} "
                f"exceeds node max_tokens {node_max_tokens}; raise --node-max-tokens to at least {needed}"
            )
    return problems


def preflight(
    records: list[Mapping[str, Any]], *, node_max_tokens: int, max_output_tokens: int
) -> list[str]:
    return preflight_prompts(
        ((f"case{index}", question_prompt(record)) for index, record in enumerate(records, start=1)),
        node_max_tokens=node_max_tokens,
        max_output_tokens=max_output_tokens,
    )


class GateCAdapter:
    """按节点 id 分派：solve 走真实模型，check 走确定性 oracle 复核。"""

    def __init__(
        self,
        model_adapter: Any,
        *,
        records: list[Mapping[str, Any]],
        max_output_tokens: int,
    ):
        self.model = model_adapter
        self.records = list(records)
        self.max_output_tokens = max_output_tokens
        self._lock = threading.Lock()
        self._active = 0
        self.peak_concurrency = 0
        self.model_calls = 0

    def _enter(self) -> None:
        with self._lock:
            self._active += 1
            self.peak_concurrency = max(self.peak_concurrency, self._active)

    def _exit(self) -> None:
        with self._lock:
            self._active -= 1

    def run(self, node: NodeSpec, context: Mapping[str, Any]):
        self._enter()
        try:
            if node.id.endswith("/solve"):
                with self._lock:
                    self.model_calls += 1
                return self.model.run(node, context)
            return self._check(node, context)
        finally:
            self._exit()

    def case_index(self, node_id: str) -> int:
        return int(node_id.split("/", 1)[0].removeprefix("case"))

    def _check(self, node: NodeSpec, context: Mapping[str, Any]):
        index = self.case_index(node.id)
        record = self.records[index - 1]
        qa = record["qa"]
        refs = context.get("artifacts") or {}
        ref = refs.get(f"case{index}/solve/{ARTIFACT_NAME}")
        if not isinstance(ref, str):
            raise RuntimeError(f"missing persisted model response for case{index}")
        reader = context.get("read_artifact")
        if not callable(reader):
            raise RuntimeError("deterministic check requires context['read_artifact']")
        payload = json.loads(reader(ref))
        text = str(payload.get("text", ""))
        model_answer = extract_numeric_answer(text)
        oracle_trace = execute_program(str(qa["program"]))
        oracle = check_oracle(oracle_trace["value"], qa)
        answer_check = compare_answer(model_answer, qa["answer"]) if model_answer else {"ok": False}
        passed = bool(oracle["ok"] and answer_check.get("ok"))
        detail = {
            "case": f"case{index}",
            "model_answer": model_answer,
            "oracle_value": oracle_trace["value"],
            "oracle_ok": oracle["ok"],
            "answer_check": answer_check,
            "passed": passed,
            "prompt_sha256": content_digest(node.prompt),
            "response_ref": ref,
        }
        if not passed:
            raise RuntimeError(f"case{index} failed independent check: {json.dumps(detail, ensure_ascii=False)}")
        return WorkerResult(
            envelope={
                "answer": str(model_answer),
                "claims": [
                    {
                        "id": f"case{index}-verified",
                        "text": "the persisted model response matches the frozen FinQA oracle",
                        "evidence_refs": [ref],
                    }
                ],
                "citations": [],
                "confidence": 1.0,
                "warnings": ["deterministic oracle check; not an LLM verifier"],
                "usage": {"total_tokens": 0, "cost": 0.0},
            },
            artifacts={"check": json.dumps(detail, ensure_ascii=False, sort_keys=True)},
        )


def _case_verdicts(
    scheduler: DAGScheduler, run_id: str, records: list[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """从已持久化的 artifact 重建每案例结论，避免依赖内存状态。"""
    verdicts: list[dict[str, Any]] = []
    for index, record in enumerate(records, start=1):
        qa = record["qa"]
        row = next(
            (
                item
                for item in scheduler.artifacts(run_id, f"case{index}/solve")
                if item["name"] == ARTIFACT_NAME
            ),
            None,
        )
        entry: dict[str, Any] = {
            "case": f"case{index}",
            "id": record.get("id"),
            "question": qa.get("question"),
            "program": qa.get("program"),
        }
        if row is None:
            entry.update({"passed": False, "reason": "model response artifact missing"})
            verdicts.append(entry)
            continue
        try:
            payload = json.loads(scheduler.read_artifact(run_id, row["ref"]))
        except Exception as exc:
            entry.update({"passed": False, "reason": f"model response unreadable: {exc}"})
            verdicts.append(entry)
            continue
        text = str(payload.get("text", ""))
        model_answer = extract_numeric_answer(text)
        oracle_trace = execute_program(str(qa["program"]))
        oracle = check_oracle(oracle_trace["value"], qa)
        answer_check = compare_answer(model_answer, qa["answer"]) if model_answer else {"ok": False}
        entry.update(
            {
                "model_answer": model_answer,
                "oracle_value": oracle_trace["value"],
                "oracle_ok": oracle["ok"],
                "exe_ans_ok": oracle.get("exe_ans_ok"),
                "answer_check": answer_check,
                "usage": payload.get("usage"),
                "request_max_tokens": payload.get("request_max_tokens"),
                "passed": bool(oracle["ok"] and answer_check.get("ok")),
            }
        )
        verdicts.append(entry)
    return verdicts


def run(args: argparse.Namespace) -> dict[str, Any]:
    records = load_records(args.fixture)
    manifest = load_manifest(args.manifest, args.fixture, records)
    if len(records) < args.cases:
        raise ValueError(f"fixture has {len(records)} records but {args.cases} cases were requested")
    selected = list(records[: args.cases])
    problems = preflight(
        selected, node_max_tokens=args.node_max_tokens, max_output_tokens=args.max_output_tokens
    )
    if problems:
        raise ValueError("preflight rejected the run: " + "; ".join(problems))

    http_adapter = HttpWorkerAdapter(
        args.endpoint,
        args.model,
        timeout=args.timeout,
        max_output_tokens=args.max_output_tokens,
        cost_per_1k_tokens=args.cost_per_1k_tokens,
        answer_extractor=extract_numeric_answer,
        warning="Gate C: one authorized call inside a bounded-concurrency run",
    )
    model: Any = http_adapter
    if args.isolate:
        model = IsolatedAdapter(http_adapter, max_processes=args.max_workers)
    dispatcher = GateCAdapter(
        model, records=selected, max_output_tokens=args.max_output_tokens
    )
    adapter = VerifyingAdapter(
        dispatcher,
        verifier=EvidenceVerifier(require_content=True, require_answer_in_evidence=True),
        policy="annotate",
    )

    nodes = build_nodes(selected, node_max_tokens=args.node_max_tokens, timeout=args.timeout)
    temporary = args.workdir is None
    workdir = Path(tempfile.mkdtemp(prefix="dsh-gate-c-")) if temporary else Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    run_spec = RunSpec(
        id="gate-c",
        budget_cost=args.budget_cost,
        max_nodes=len(nodes),
        max_workers=args.max_workers,
        deadline_seconds=args.deadline,
        cost_per_1k_tokens=args.cost_per_1k_tokens,
    )
    started = time.perf_counter()
    try:
        with DAGScheduler(workdir / "run.sqlite3", max_workers=args.max_workers) as scheduler:
            scheduler.submit(run_spec, nodes)
            result = scheduler.execute(adapter)
            elapsed = time.perf_counter() - started
            verdicts = _case_verdicts(scheduler, run_spec.id, selected)
            budget = scheduler.budget_snapshot(run_spec.id)
            events = scheduler.events(run_spec.id)
            nodes_snapshot = scheduler.snapshot(run_spec.id)
            provenance = build_provenance(
                scheduler,
                run_spec.id,
                claims_by_node={
                    f"case{index}/check": [
                        {
                            "id": f"case{index}-verified",
                            "text": "matches the frozen FinQA oracle",
                            "evidence_refs": [
                                item["ref"]
                                for item in scheduler.artifacts(run_spec.id, f"case{index}/solve")
                                if item["name"] == ARTIFACT_NAME
                            ],
                        }
                    ]
                    for index in range(1, len(selected) + 1)
                },
            ).as_dict()
    finally:
        if temporary:
            shutil.rmtree(workdir, ignore_errors=True)

    event_counts: dict[str, int] = {}
    for event in events:
        event_counts[event["event"]] = event_counts.get(event["event"], 0) + 1
    passed_cases = sum(1 for verdict in verdicts if verdict["passed"])
    ok = (
        result.status == "succeeded"
        and passed_cases == len(verdicts)
        and budget["invariant_ok"]
    )
    return {
        "benchmark": "Gate C: three-case bounded-concurrency real adapter run",
        "schema_version": 1,
        "generated_at_utc": _utc_now(),
        "environment": {"python": sys.version.split()[0], "platform": platform.platform()},
        "adapter": {
            "endpoint": args.endpoint,
            "model": args.model,
            "transport": "Anthropic Messages SSE; authorized local reverse proxy",
            "isolated": bool(args.isolate),
            "request_max_tokens_ceiling": args.max_output_tokens,
            "node_max_tokens": args.node_max_tokens,
            "cost_per_1k_tokens": args.cost_per_1k_tokens,
            "budget_model": "node max_tokens 是单次调用预算预留上限（输入+输出）",
        },
        "concurrency": {
            "configured_max_workers": args.max_workers,
            "observed_peak_adapter_calls": dispatcher.peak_concurrency,
            "model_calls": dispatcher.model_calls,
            "node_count": len(nodes),
        },
        "fixture": {
            "path": str(args.fixture),
            "sha256": file_digest(args.fixture),
            "manifest": manifest,
            "cases_requested": args.cases,
        },
        "result": {
            "status": result.status,
            "succeeded": list(result.succeeded),
            "failed": list(result.failed),
            "blocked": list(result.blocked),
            "spent_cost": result.spent_cost,
            "wall_seconds": round(elapsed, 3),
        },
        "budget": budget,
        "budget_invariant_ok": budget["invariant_ok"],
        "event_counts": event_counts,
        "heartbeat_events": event_counts.get("task.heartbeat", 0),
        "nodes": nodes_snapshot,
        "adapter_calls": list(getattr(model, "calls", [])),
        "verification": adapter.verification_report(),
        "provenance": provenance,
        "cases": verdicts,
        "cases_passed": passed_cases,
        "passed": ok,
        "limitations": LIMITATIONS,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Gate C bounded-concurrency real adapter run")
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--endpoint", required=True, help="authorized Anthropic-compatible Messages endpoint")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--cases", type=int, default=DEFAULT_CASES)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--deadline", type=float, default=1800.0)
    parser.add_argument("--max-output-tokens", type=int, default=DEFAULT_MAX_OUTPUT_TOKENS)
    parser.add_argument("--node-max-tokens", type=int, default=DEFAULT_NODE_MAX_TOKENS)
    parser.add_argument("--budget-cost", type=float, default=DEFAULT_BUDGET_COST)
    parser.add_argument("--cost-per-1k-tokens", type=float, default=DEFAULT_COST_PER_1K_TOKENS)
    parser.add_argument("--isolate", action="store_true", help="run model calls in worker processes")
    parser.add_argument("--workdir", type=Path, default=None)
    args = parser.parse_args()
    if args.cases < 1 or args.max_workers < 1 or args.max_output_tokens < 1 or args.node_max_tokens < 1:
        parser.error("--cases, --max-workers, --max-output-tokens and --node-max-tokens must be positive")
    try:
        report = run(args)
    except Exception as exc:
        report = {
            "benchmark": "Gate C: three-case bounded-concurrency real adapter run",
            "generated_at_utc": _utc_now(),
            "passed": False,
            "error": str(exc),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    # stdout 使用 ASCII 转义，避免在 cp936/GBK 控制台或管道中出现乱码字节。
    print(json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
