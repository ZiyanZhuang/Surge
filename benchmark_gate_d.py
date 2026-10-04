"""Gate D：真实 provider 的并发容量曲线。

与 Gate C 的区别
----------------
Gate C 用 3 个案例、`max_workers=2` 验证"有界并发下的调度与预算语义"；Gate D 关心
**同一个 endpoint 在不同并发档位下能返回多少合法结果**，因此：

* 每个档位提交 N 个 wave-0 节点，`max_workers=N`，全部是真实模型调用；
* 任务固定且极小（返回一个固定 JSON 对象），把任务复杂度从容量测量中剔除；
* `max_attempts=1`，不做重试掩盖 —— 失败就是失败，按 adapter 给出的 `kind` 归类；
* 逐档位给出 submitted / returned / valid / 失败分类 / 峰值并发 / wall / 延迟分位 / 成本。

能得出什么、不能得出什么
-----------------------
能得到：这条 endpoint 在这台机器、这个时间窗、这个模型与输出上限下的**实测返回率曲线**。
不能得到：稳定配额、SLA、跨时段结论，也不能把某一档的成功率外推成"支持 N 个 Agent"。
provider 限流、账号配额和网络状况都会让曲线移动，因此报告必须带时间戳与环境快照。

预算口径与 Gate C 相同：`node.max_tokens` 是单次调用的预留上限（输入+输出），
启动前用 `preflight_prompts` 拒绝口径不足的配置，避免付费跑到一半撞硬预算。
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import statistics
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmark_gate_c import estimate_tokens, preflight_prompts
from surge_cluster import DAGScheduler, HttpWorkerAdapter, NodeSpec, RunSpec, extract_json_answer

DEFAULT_URL = "http://127.0.0.1:17800/v1/messages"
DEFAULT_MODEL = "gpt-6.1-sol"
DEFAULT_LEVELS = "16,32,64"
DEFAULT_MAX_OUTPUT_TOKENS = 2000
DEFAULT_NODE_MAX_TOKENS = 8000
DEFAULT_COST_PER_1K_TOKENS = 0.01
DEFAULT_BUDGET_COST = 50.0
DEFAULT_TIMEOUT = 180.0
DEFAULT_DEADLINE = 3600.0

CAPACITY_TASK = (
    "Extract the single fact requested below and answer with JSON only, no prose.\n"
    'schema: {"answer": "<string>"}\n'
    'fact: the capital city of France\n'
)

LIMITATIONS = [
    "One endpoint, one machine, one time window: this is not a quota or SLA statement.",
    "The task is deliberately trivial, so a failure here reflects transport or provider pressure, not task difficulty.",
    "max_attempts=1: no retry masks a failure, and no retry inflates the success count.",
    "A success rate at one level does not extrapolate to 'supports N agents'.",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_levels(raw: str) -> list[int]:
    levels: list[int] = []
    for piece in str(raw).split(","):
        piece = piece.strip()
        if piece == "":
            continue
        value = int(piece)
        if value < 1:
            raise ValueError(f"concurrency levels must be positive: {piece}")
        levels.append(value)
    if not levels:
        raise ValueError("at least one concurrency level is required")
    if len(set(levels)) != len(levels):
        raise ValueError("concurrency levels must be unique")
    return sorted(levels)


class _ConcurrencyProbe:
    """记录在飞调用数的峰值；同时在飞调用数就是物理并发证据。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def enter(self) -> None:
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)

    def leave(self) -> None:
        with self._lock:
            self.active -= 1


class _ProbedAdapter:
    def __init__(self, inner: Any, probe: _ConcurrencyProbe):
        self.inner = inner
        self.probe = probe
        self.calls = inner.calls

    def run(self, node: NodeSpec, context: Any):
        self.probe.enter()
        try:
            return self.inner.run(node, context)
        finally:
            self.probe.leave()


def build_nodes(level: int, *, node_max_tokens: int, timeout: float) -> list[NodeSpec]:
    return [
        NodeSpec(
            id=f"agent-{index:02d}",
            prompt=CAPACITY_TASK,
            wave=0,
            max_attempts=1,
            timeout_seconds=timeout,
            max_tokens=node_max_tokens,
        )
        for index in range(level)
    ]


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return round(ordered[index], 6)


def run_level(
    level: int,
    *,
    endpoint: str,
    model: str,
    node_max_tokens: int,
    max_output_tokens: int,
    cost_per_1k_tokens: float,
    budget_cost: float,
    timeout: float,
    deadline: float,
    workdir_root: Path,
) -> dict[str, Any]:
    probe = _ConcurrencyProbe()
    http_adapter = HttpWorkerAdapter(
        endpoint,
        model,
        timeout=timeout,
        max_output_tokens=max_output_tokens,
        cost_per_1k_tokens=cost_per_1k_tokens,
        answer_extractor=extract_json_answer,
        warning=f"Gate D concurrency level {level}; not a capacity guarantee",
    )
    adapter = _ProbedAdapter(http_adapter, probe)
    nodes = build_nodes(level, node_max_tokens=node_max_tokens, timeout=timeout)
    workdir = workdir_root / f"level-{level}"
    workdir.mkdir(parents=True, exist_ok=True)
    run_spec = RunSpec(
        id=f"gate-d-{level}",
        budget_cost=budget_cost,
        max_nodes=level,
        max_workers=level,
        deadline_seconds=deadline,
        cost_per_1k_tokens=cost_per_1k_tokens,
    )
    started = time.perf_counter()
    with DAGScheduler(workdir / "run.sqlite3", max_workers=level) as scheduler:
        scheduler.submit(run_spec, nodes)
        result = scheduler.execute(adapter)
        budget = scheduler.budget_snapshot(run_spec.id)
        snapshot = scheduler.snapshot(run_spec.id)
    elapsed = time.perf_counter() - started

    calls = list(http_adapter.calls)
    failure_kinds: dict[str, int] = {}
    latencies: list[float] = []
    for call in calls:
        if call.get("ok"):
            latencies.append(float(call.get("elapsed_seconds") or 0.0))
        else:
            kind = str(call.get("kind") or "unknown")
            failure_kinds[kind] = failure_kinds.get(kind, 0) + 1
    returned = sum(1 for call in calls if call.get("ok"))
    return {
        "level": level,
        "submitted": level,
        "returned_valid": len(result.succeeded),
        "adapter_ok_calls": returned,
        "failed_nodes": len(result.failed),
        "blocked_nodes": len(result.blocked),
        "failure_kinds": failure_kinds,
        "peak_concurrency": probe.peak,
        "wall_seconds": round(elapsed, 3),
        "valid_rate": round(len(result.succeeded) / level, 4),
        "latency_seconds": {
            "median": round(statistics.median(latencies), 6) if latencies else 0.0,
            "p95": _percentile(latencies, 0.95),
            "max": round(max(latencies), 6) if latencies else 0.0,
        },
        "spent_cost": result.spent_cost,
        "budget_invariant_ok": budget["invariant_ok"],
        "status": result.status,
        "nodes": snapshot,
        "adapter_calls": calls,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    levels = parse_levels(args.levels)
    problems = preflight_prompts(
        ((f"level-{level}", CAPACITY_TASK) for level in levels),
        node_max_tokens=args.node_max_tokens,
        max_output_tokens=args.max_output_tokens,
    )
    if problems:
        raise ValueError("preflight rejected the run: " + "; ".join(problems))
    temporary = args.workdir is None
    workdir_root = Path(tempfile.mkdtemp(prefix="dsh-gate-d-")) if temporary else Path(args.workdir)
    workdir_root.mkdir(parents=True, exist_ok=True)
    try:
        measured = [
            run_level(
                level,
                endpoint=args.endpoint,
                model=args.model,
                node_max_tokens=args.node_max_tokens,
                max_output_tokens=args.max_output_tokens,
                cost_per_1k_tokens=args.cost_per_1k_tokens,
                budget_cost=args.budget_cost,
                timeout=args.timeout,
                deadline=args.deadline,
                workdir_root=workdir_root,
            )
            for level in levels
        ]
    finally:
        if temporary:
            shutil.rmtree(workdir_root, ignore_errors=True)

    return {
        "benchmark": "Gate D: real-provider concurrency capacity curve",
        "schema_version": 1,
        "generated_at_utc": _utc_now(),
        "environment": {"python": sys.version.split()[0], "platform": platform.platform()},
        "adapter": {
            "endpoint": args.endpoint,
            "model": args.model,
            "transport": "Anthropic Messages SSE; authorized local reverse proxy",
            "request_max_tokens_ceiling": args.max_output_tokens,
            "node_max_tokens": args.node_max_tokens,
            "cost_per_1k_tokens": args.cost_per_1k_tokens,
            "max_attempts": 1,
        },
        "task": CAPACITY_TASK,
        "levels_requested": levels,
        "levels": measured,
        "capacity_summary": [
            {
                "level": entry["level"],
                "valid_rate": entry["valid_rate"],
                "peak_concurrency": entry["peak_concurrency"],
                "failure_kinds": entry["failure_kinds"],
            }
            for entry in measured
        ],
        "total_spent_cost": round(sum(entry["spent_cost"] for entry in measured), 6),
        "limitations": LIMITATIONS,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Gate D real-provider concurrency capacity curve")
    parser.add_argument("--endpoint", required=True, help="authorized Anthropic-compatible Messages endpoint")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--levels", default=DEFAULT_LEVELS, help="comma-separated concurrency levels")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--deadline", type=float, default=DEFAULT_DEADLINE)
    parser.add_argument("--max-output-tokens", type=int, default=DEFAULT_MAX_OUTPUT_TOKENS)
    parser.add_argument("--node-max-tokens", type=int, default=DEFAULT_NODE_MAX_TOKENS)
    parser.add_argument("--budget-cost", type=float, default=DEFAULT_BUDGET_COST)
    parser.add_argument("--cost-per-1k-tokens", type=float, default=DEFAULT_COST_PER_1K_TOKENS)
    parser.add_argument("--workdir", type=Path, default=None)
    args = parser.parse_args()
    try:
        report = run(args)
    except Exception as exc:
        report = {
            "benchmark": "Gate D: real-provider concurrency capacity curve",
            "generated_at_utc": _utc_now(),
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
    return 0 if "error" not in report else 1


if __name__ == "__main__":
    raise SystemExit(main())
