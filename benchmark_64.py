"""Run a reproducible 64-agent synthetic concurrency benchmark for 浪潮模式.

This measures the local scheduler, not model quality or network inference. The
workload follows common agentic benchmark reporting dimensions: concurrency,
wall latency, throughput, task latency, failures, and cost.
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from surge_cluster import DAGScheduler, NodeSpec, RunSpec, WorkerResult


class SyntheticAgentAdapter:
    """Deterministic I/O-shaped worker; replace with a real DSH adapter later."""

    def __init__(self, delay_seconds: float):
        self.delay_seconds = delay_seconds
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.latencies: list[float] = []

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        started = time.perf_counter()
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            # Exercise the lease path as a real adapter should do for long calls.
            context["heartbeat"](max(1.0, node.timeout_seconds))
            time.sleep(self.delay_seconds)
            return WorkerResult(
                envelope={
                    "answer": f"synthetic completion for {node.id}",
                    "claims": [],
                    "citations": [],
                    "confidence": 0.5,
                    "warnings": ["synthetic benchmark; no model call"],
                    "usage": {"total_tokens": 16, "cost": 0.00016},
                },
                artifacts={"report": f"synthetic artifact from {node.id}"},
            )
        finally:
            elapsed = time.perf_counter() - started
            with self._lock:
                self.latencies.append(elapsed)
                self.active -= 1


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def make_nodes(scenario: str) -> list[NodeSpec]:
    if scenario == "fanout64":
        return [NodeSpec(f"agent-{i:02d}", "parallel synthetic research", wave=0, timeout_seconds=10, max_tokens=64) for i in range(64)]
    if scenario == "waves64":
        scouts = [NodeSpec(f"scout-{i:02d}", "scout evidence", wave=0, timeout_seconds=10, max_tokens=64) for i in range(32)]
        scout_ids = tuple(node.id for node in scouts)
        deepen = [NodeSpec(f"deepen-{i:02d}", "deepen evidence gaps", depends_on=scout_ids, wave=1, timeout_seconds=10, max_tokens=64) for i in range(16)]
        deepen_ids = tuple(node.id for node in deepen)
        verify = [NodeSpec(f"verify-{i:02d}", "verify claims", depends_on=deepen_ids, wave=2, timeout_seconds=10, max_tokens=64) for i in range(8)]
        verify_ids = tuple(node.id for node in verify)
        synth = [NodeSpec(f"synthesize-{i:02d}", "synthesize verified evidence", depends_on=verify_ids, wave=3, timeout_seconds=10, max_tokens=64) for i in range(8)]
        return scouts + deepen + verify + synth
    raise ValueError(f"unknown scenario: {scenario}")


def run_scenario(scenario: str, workers: int, delay_seconds: float) -> dict[str, Any]:
    temp_root = Path(tempfile.mkdtemp(prefix="surge-bench-"))
    run_id = scenario
    nodes = make_nodes(scenario)
    adapter = SyntheticAgentAdapter(delay_seconds)
    started = time.perf_counter()
    try:
        with DAGScheduler(temp_root / "run.sqlite3", max_workers=workers) as scheduler:
            scheduler.submit(
                RunSpec(
                    id=run_id,
                    budget_cost=100.0,
                    max_nodes=64,
                    max_workers=workers,
                    deadline_seconds=120,
                    wave_success_threshold=1.0,
                ),
                nodes,
            )
            result = scheduler.execute(adapter, run_id)
        elapsed = time.perf_counter() - started
        latencies = adapter.latencies
        return {
            "scenario": scenario,
            "agents": len(nodes),
            "configured_workers": workers,
            "synthetic_delay_seconds": delay_seconds,
            "status": result.status,
            "succeeded": len(result.succeeded),
            "failed": len(result.failed),
            "blocked": len(result.blocked),
            "max_active_observed": adapter.max_active,
            "wall_seconds": round(elapsed, 6),
            "throughput_agents_per_second": round(len(nodes) / elapsed, 3) if elapsed else 0.0,
            "latency_seconds": {
                "median": round(median(latencies), 6) if latencies else 0.0,
                "p95": round(percentile(latencies, 0.95), 6),
            },
            "spent_cost": result.spent_cost,
            "note": "Synthetic local scheduler benchmark; not an LLM quality or remote inference benchmark.",
        }
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--delay", type=float, default=0.02)
    parser.add_argument("--output", type=Path, default=Path("benchmark-results") / "benchmark-64.json")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.delay < 0 or args.delay != args.delay or args.delay == float("inf"):
        parser.error("--delay must be finite and non-negative")
    runs = [run_scenario("fanout64", args.workers, args.delay), run_scenario("waves64", args.workers, args.delay)]
    results = {
        "benchmark": "浪潮模式 64-agent local concurrency benchmark",
        "methodology": "Single-run synthetic local scheduler measurement; not a statistical sample and not an LLM quality or remote inference benchmark.",
        "environment": {"python": sys.version.split()[0], "platform": platform.platform()},
        "runs": runs,
    }
    if any(item["status"] != "succeeded" or item["succeeded"] != item["agents"] for item in runs):
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(results, ensure_ascii=False, indent=2))
        raise SystemExit(1)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
