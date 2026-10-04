"""``dsh-surge-run``：用一条命令把一个 JSON 任务计划跑成真实的 DAG。

两种模式：

* ``--dry-run``：只用 ``LocalEchoAdapter`` 离线执行，验证计划结构、DAG 依赖、
  波次顺序和调度路径，不产生任何网络调用；
* 真实模式：显式给出 ``--endpoint`` 后，通过 ``DAGScheduler`` 与
  :class:`surge_cluster.HttpWorkerAdapter` 调用经授权的 Anthropic-compatible
  Messages endpoint，并沿用同一套 fail-closed 校验。

报告是机器可读的 JSON，包含 UTC 时间戳、运行环境、计划摘要、逐节点状态、
预算结算和（真实模式下的）逐次调用证据。报告不包含 prompt 正文，也不写入凭据；
stdout 使用 ASCII 转义输出，避免在 GBK 控制台上出现乱码。
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from surge_cluster import (
    DAGScheduler,
    HttpWorkerAdapter,
    LocalEchoAdapter,
    NodeSpec,
    PlanError,
    RunPlan,
    RunSpec,
    extract_numeric_answer,
    load_plan,
)

TOOL_NAME = "dsh-surge-run"
SCHEMA_VERSION = 1
BUDGET_MODEL_NOTE = (
    "节点 max_tokens 是单次调用的预算预留上限（输入+输出）；"
    "真实请求的输出上限是 min(--max-output-tokens, node.max_tokens)。"
)
LIMITATIONS = [
    "一次运行只说明这条 DAG 在当前 endpoint 上完成，不构成 provider 容量或模型质量证明。",
    "Python 线程超时是协作式的；需要硬超时请改用进程级 worker。",
    "调度语义是 at-least-once；适配器必须使用幂等键，不能假设 exactly-once。",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _environment() -> dict[str, str]:
    return {"python": sys.version.split()[0], "platform": platform.platform()}


def _error_report(kind: str, message: str) -> dict[str, Any]:
    return {
        "tool": TOOL_NAME,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "environment": _environment(),
        "error_kind": kind,
        "error": message,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=TOOL_NAME,
        description="Run a JSON task plan as a DAG, offline or against an authorized Messages endpoint.",
    )
    parser.add_argument("--plan", type=Path, required=True, help="task plan JSON")
    parser.add_argument("--output", type=Path, required=True, help="report output path")
    parser.add_argument("--dry-run", action="store_true", help="offline LocalEchoAdapter validation only")
    parser.add_argument("--endpoint", default=None, help="authorized Messages SSE endpoint")
    parser.add_argument("--model", default="gpt-6.1-sol")
    parser.add_argument("--timeout", type=float, default=120.0, help="per-request timeout in seconds")
    parser.add_argument("--max-output-tokens", type=int, default=2000)
    parser.add_argument("--cost-per-1k-tokens", type=float, default=None)
    parser.add_argument("--workdir", type=Path, default=None, help="persist SQLite state and artifacts here")
    return parser


def _runtime_specs(plan: RunPlan, args: argparse.Namespace) -> tuple[RunSpec, list[NodeSpec]]:
    """套用 CLI 级覆盖；计划和节点内容本身不被改写。"""
    run_spec = plan.run
    if args.cost_per_1k_tokens is not None:
        run_spec = RunSpec(
            id=run_spec.id,
            budget_cost=run_spec.budget_cost,
            max_nodes=run_spec.max_nodes,
            max_workers=run_spec.max_workers,
            wave_success_threshold=run_spec.wave_success_threshold,
            cost_per_1k_tokens=args.cost_per_1k_tokens,
            deadline_seconds=run_spec.deadline_seconds,
            stage_policies=run_spec.stage_policies,
            route_profiles=run_spec.route_profiles,
        )
    return run_spec, list(plan.nodes)


def _build_adapter(args: argparse.Namespace, run_spec: RunSpec) -> tuple[Any, dict[str, Any] | None]:
    if args.dry_run:
        return LocalEchoAdapter(), None
    adapter = HttpWorkerAdapter(
        args.endpoint,
        args.model,
        timeout=args.timeout,
        max_output_tokens=args.max_output_tokens,
        cost_per_1k_tokens=(
            run_spec.cost_per_1k_tokens if args.cost_per_1k_tokens is None else args.cost_per_1k_tokens
        ),
        answer_extractor=extract_numeric_answer,
        warning="one authorized model call through the CLI; not a capacity or quality claim",
    )
    meta = {
        "endpoint": args.endpoint,
        "model": args.model,
        "timeout_seconds": args.timeout,
        "request_max_tokens_ceiling": args.max_output_tokens,
        "cost_per_1k_tokens": adapter.cost_per_1k_tokens,
        "transport": "Anthropic Messages SSE; authorized local reverse proxy or compatible endpoint",
        "api_key_placeholder": adapter.api_key,
    }
    return adapter, meta


def run(argv: list[str] | None = None) -> tuple[int, dict[str, Any], Path]:
    args = _build_parser().parse_args(argv)
    if not args.dry_run and not args.endpoint:
        return 2, _error_report(
            "usage", "real mode requires --endpoint; use --dry-run for an offline plan validation"
        ), args.output
    if args.max_output_tokens < 1:
        return 2, _error_report("usage", "--max-output-tokens must be positive"), args.output
    if args.timeout <= 0:
        return 2, _error_report("usage", "--timeout must be positive"), args.output
    try:
        plan = load_plan(args.plan)
    except PlanError as exc:
        return 2, _error_report("plan", str(exc)), args.output
    try:
        run_spec, nodes = _runtime_specs(plan, args)
        adapter, adapter_meta = _build_adapter(args, run_spec)
    except (ValueError, TypeError) as exc:
        return 2, _error_report("usage", str(exc)), args.output

    temporary = args.workdir is None
    workdir = Path(tempfile.mkdtemp(prefix="dsh-surge-run-")) if temporary else Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        with DAGScheduler(workdir / "run.sqlite3", max_workers=run_spec.max_workers) as scheduler:
            scheduler.submit(run_spec, nodes)
            result = scheduler.execute(adapter)
            nodes_snapshot = scheduler.snapshot(run_spec.id)
    finally:
        if temporary:
            shutil.rmtree(workdir, ignore_errors=True)

    report: dict[str, Any] = {
        "tool": TOOL_NAME,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "environment": _environment(),
        "mode": "dry-run" if args.dry_run else "http-adapter",
        "plan": {
            "path": str(args.plan),
            "id": run_spec.id,
            "sha256": plan.digest,
            "node_count": len(plan.nodes),
        },
        "adapter": adapter_meta,
        "budget_model": BUDGET_MODEL_NOTE,
        "runtime_state": {
            "workdir": str(workdir),
            "persisted": not temporary,
            "note": "临时目录已在运行结束后删除" if temporary else "运行时状态已保留，可按需归档或脱敏",
        },
        "result": {
            "status": result.status,
            "succeeded": list(result.succeeded),
            "failed": list(result.failed),
            "blocked": list(result.blocked),
            "deferred": list(result.deferred),
            "spent_cost": result.spent_cost,
        },
        "nodes": nodes_snapshot,
        "limitations": LIMITATIONS,
    }
    calls = getattr(adapter, "calls", None)
    if calls:
        report["adapter_calls"] = calls
    return (0 if result.status == "succeeded" else 1), report, args.output


def main() -> int:
    code, report, output = run()
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        # newline="\n" 让报告在 Windows 上也保持 LF，便于跨平台 diff。
        output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n"
        )
    except OSError as exc:
        code, report = 2, _error_report("output", f"cannot write report: {exc}")
    # stdout 使用 ASCII 转义，避免在 cp936/GBK 控制台或管道里被写成乱码字节。
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
