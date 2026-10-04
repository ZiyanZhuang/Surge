"""任务计划的解析与结构校验。

计划文件是一个 JSON 对象，把一次 DAG 运行的 run 参数和节点清单写成可复核、
可归档、可 diff 的文本。这里只做**结构**校验（字段类型、未知字段、重复 id、
悬空依赖），语义校验（环、波次顺序、数值上限）仍由调度器在 ``submit`` 时
完成，避免两处规则漂移。
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .core import NodeSpec, RunSpec

PLAN_SCHEMA_VERSION = 1

_PLAN_KEYS = {"schema_version", "description", "run", "nodes"}
_RUN_KEYS = {
    "id",
    "budget_cost",
    "max_nodes",
    "max_workers",
    "deadline_seconds",
    "wave_success_threshold",
    "cost_per_1k_tokens",
}
_NODE_KEYS = {
    "id",
    "prompt",
    "depends_on",
    "wave",
    "priority",
    "max_attempts",
    "timeout_seconds",
    "max_tokens",
    "validation_policy",
    "stage",
    "incremental_value",
    "urgency",
    "route_class",
}
# v1 计划只覆盖基础字段；stage_policies/route_profiles 需要 Python API 表达。
_PYTHON_ONLY_KEYS = {"stage_policies", "route_profiles"}


class PlanError(ValueError):
    """计划文件不合法；调用方应把它报告为结构错误而不是运行失败。"""


@dataclass(frozen=True)
class RunPlan:
    """归一化后的计划；``source`` 用于计算摘要和写入报告。"""

    run: RunSpec
    nodes: tuple[NodeSpec, ...]
    source: dict[str, Any]

    @property
    def digest(self) -> str:
        return plan_digest(self.source)


def _require_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PlanError(f"{where} must be a JSON object")
    return value


def _reject_unknown(mapping: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if not unknown:
        return
    hint = ""
    if set(unknown) & _PYTHON_ONLY_KEYS:
        hint = "; stage_policies/route_profiles 目前只能通过 Python API 传入"
    raise PlanError(f"{where} has unknown fields: {', '.join(unknown)}{hint}")


def _string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlanError(f"{where} must be a non-empty string")
    return value


def _number(value: Any, where: str, *, positive: bool = False, allow_zero: bool = True) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise PlanError(f"{where} must be a finite number")
    if positive and value <= 0:
        raise PlanError(f"{where} must be positive")
    if not allow_zero and value == 0:
        raise PlanError(f"{where} must not be zero")
    if value < 0:
        raise PlanError(f"{where} must not be negative")
    return float(value)


def _integer(value: Any, where: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PlanError(f"{where} must be an integer >= {minimum}")
    return value


def _string_tuple(value: Any, where: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise PlanError(f"{where} must be a list of node ids")
    return tuple(_string(item, f"{where}[]") for item in value)


def plan_from_mapping(mapping: Mapping[str, Any]) -> RunPlan:
    """把已解析的 JSON 对象转换成 ``RunSpec``/``NodeSpec``，并做结构校验。"""
    plan = _require_mapping(mapping, "plan")
    _reject_unknown(plan, _PLAN_KEYS, "plan")
    version = plan.get("schema_version")
    if version != PLAN_SCHEMA_VERSION:
        raise PlanError(
            f"plan.schema_version must be {PLAN_SCHEMA_VERSION}; got {version!r}"
        )
    if "run" not in plan:
        raise PlanError("plan.run is required")
    if "nodes" not in plan:
        raise PlanError("plan.nodes is required")
    run_mapping = _require_mapping(plan["run"], "plan.run")
    _reject_unknown(run_mapping, _RUN_KEYS, "plan.run")
    run_id = _string(run_mapping.get("id"), "plan.run.id")
    raw_nodes = plan["nodes"]
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise PlanError("plan.nodes must be a non-empty list")

    nodes: list[NodeSpec] = []
    seen: set[str] = set()
    for index, raw_node in enumerate(raw_nodes):
        where = f"plan.nodes[{index}]"
        node_mapping = _require_mapping(raw_node, where)
        _reject_unknown(node_mapping, _NODE_KEYS, where)
        node_id = _string(node_mapping.get("id"), f"{where}.id")
        if node_id in seen:
            raise PlanError(f"{where}.id is duplicated: {node_id}")
        seen.add(node_id)
        prompt = _string(node_mapping.get("prompt"), f"{where}.prompt")
        policy = node_mapping.get("validation_policy", "strict")
        if policy not in {"strict", "lenient"}:
            raise PlanError(f"{where}.validation_policy must be 'strict' or 'lenient'")
        stage = node_mapping.get("stage", "")
        if not isinstance(stage, str):
            raise PlanError(f"{where}.stage must be a string")
        nodes.append(
            NodeSpec(
                id=node_id,
                prompt=prompt,
                depends_on=_string_tuple(node_mapping.get("depends_on"), f"{where}.depends_on"),
                wave=_integer(node_mapping.get("wave", 0), f"{where}.wave"),
                priority=_integer(node_mapping.get("priority", 0), f"{where}.priority"),
                max_attempts=_integer(node_mapping.get("max_attempts", 2), f"{where}.max_attempts", minimum=1),
                timeout_seconds=_number(
                    node_mapping.get("timeout_seconds", 300.0), f"{where}.timeout_seconds", positive=True
                ),
                max_tokens=_integer(node_mapping.get("max_tokens", 4096), f"{where}.max_tokens", minimum=1),
                validation_policy=policy,
                stage=stage,
                incremental_value=_number(
                    node_mapping.get("incremental_value", 1.0), f"{where}.incremental_value"
                ),
                urgency=_number(node_mapping.get("urgency", 0.0), f"{where}.urgency"),
                route_class=_string(
                    node_mapping.get("route_class", "default"), f"{where}.route_class"
                ),
            )
        )

    known = {node.id for node in nodes}
    for node in nodes:
        for dependency in node.depends_on:
            if dependency == node.id:
                raise PlanError(f"plan node {node.id} depends on itself")
            if dependency not in known:
                raise PlanError(f"plan node {node.id} depends on unknown node {dependency}")

    run = RunSpec(
        id=run_id,
        budget_cost=_number(run_mapping.get("budget_cost", 100.0), "plan.run.budget_cost", positive=True),
        max_nodes=_integer(run_mapping.get("max_nodes", len(nodes)), "plan.run.max_nodes", minimum=1),
        max_workers=_integer(run_mapping.get("max_workers", 4), "plan.run.max_workers", minimum=1),
        wave_success_threshold=_number(
            run_mapping.get("wave_success_threshold", 1.0), "plan.run.wave_success_threshold"
        ),
        cost_per_1k_tokens=_number(
            run_mapping.get("cost_per_1k_tokens", 0.01), "plan.run.cost_per_1k_tokens"
        ),
        deadline_seconds=_number(
            run_mapping.get("deadline_seconds", 3600.0), "plan.run.deadline_seconds", positive=True
        ),
    )
    if run.wave_success_threshold > 1:
        raise PlanError("plan.run.wave_success_threshold must be between 0 and 1")
    if run.max_nodes < len(nodes):
        raise PlanError(
            f"plan.run.max_nodes={run.max_nodes} is smaller than the {len(nodes)} declared nodes"
        )
    source = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "run": {
            "id": run.id,
            "budget_cost": run.budget_cost,
            "max_nodes": run.max_nodes,
            "max_workers": run.max_workers,
            "wave_success_threshold": run.wave_success_threshold,
            "cost_per_1k_tokens": run.cost_per_1k_tokens,
            "deadline_seconds": run.deadline_seconds,
        },
        "nodes": [
            {
                "id": node.id,
                "prompt": node.prompt,
                "depends_on": list(node.depends_on),
                "wave": node.wave,
                "priority": node.priority,
                "max_attempts": node.max_attempts,
                "timeout_seconds": node.timeout_seconds,
                "max_tokens": node.max_tokens,
                "validation_policy": node.validation_policy,
                "stage": node.stage,
                "incremental_value": node.incremental_value,
                "urgency": node.urgency,
                "route_class": node.route_class,
            }
            for node in nodes
        ],
    }
    return RunPlan(run=run, nodes=tuple(nodes), source=source)


def load_plan(path: str | Path) -> RunPlan:
    """读取并校验计划文件；任何结构问题都抛出 :class:`PlanError`。"""
    plan_path = Path(path)
    try:
        text = plan_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise PlanError(f"plan file not found: {plan_path}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PlanError(f"plan file is not valid JSON: {exc}") from exc
    return plan_from_mapping(payload)


def plan_digest(source: Mapping[str, Any]) -> str:
    """计划内容的规范化 SHA-256，用于报告与复现比对。"""
    canonical = json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
