"""浪潮模式的单机有界并发 DAG 调度器。

DSH 只通过 ``WorkerAdapter`` 接入一个实际 worker 调用；并发控制、持久化、
预算账本、证据校验、波次 gate 和恢复语义全部留在 Host 本地，保证调度结果
可审计、可恢复且边界清晰。本模块不直接依赖任何 Provider/模型 SDK。
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import math
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol


TERMINAL = {"succeeded", "failed", "blocked", "cancelled", "deferred"}
RUN_TERMINAL = {"succeeded", "failed", "cancelled"}


@dataclass(frozen=True)
class StagePolicy:
    """单个逻辑执行阶段的需求策略。

    ``target_progress`` 表示本阶段需要覆盖的增量价值比例；开启
    ``defer_excess`` 后，达到目标的剩余 pending 节点会显式记为 deferred，
    而不是继续启动并消耗模型额度。
    """

    name: str
    max_in_flight: int = 1
    dispatch_batch: int = 1
    target_progress: float = 1.0
    quality_floor: float = 0.0
    preferred_routes: tuple[str, ...] = ()
    defer_excess: bool = True


@dataclass(frozen=True)
class RouteProfile:
    """一条可被需求感知路由器选择的 Provider/模型执行通道。"""

    id: str
    provider: str
    model: str
    reasoning_effort: str = "medium"
    quality: float = 0.5
    latency: float = 1.0
    cost_per_1k_tokens: float = 0.01
    max_concurrency: int = 64
    route_classes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RouteDecision:
    profile_id: str
    provider: str
    model: str
    reasoning_effort: str
    score: float
    estimated_cost: float
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "provider": self.provider,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "score": self.score,
            "estimated_cost": self.estimated_cost,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class RunSpec:
    id: str
    budget_cost: float = 100.0
    max_nodes: int = 100
    max_workers: int = 4
    wave_success_threshold: float = 1.0
    cost_per_1k_tokens: float = 0.01
    deadline_seconds: float = 3600.0
    stage_policies: tuple[StagePolicy, ...] = ()
    route_profiles: tuple[RouteProfile, ...] = ()


@dataclass(frozen=True)
class NodeSpec:
    id: str
    prompt: str
    depends_on: tuple[str, ...] = ()
    wave: int = 0
    priority: int = 0
    max_attempts: int = 2
    timeout_seconds: float = 300.0
    max_tokens: int = 4096
    validation_policy: str = "strict"
    stage: str = ""
    incremental_value: float = 1.0
    urgency: float = 0.0
    route_class: str = "default"


@dataclass(frozen=True)
class WorkerResult:
    envelope: Mapping[str, Any]
    artifacts: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationReport:
    ok: bool
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    score: float = 0.0


@dataclass(frozen=True)
class RunResult:
    """运行摘要；blocked 为兼容性聚合，包含节点 blocked 与 cancelled 两种状态。"""

    run_id: str
    status: str
    succeeded: tuple[str, ...]
    failed: tuple[str, ...]
    blocked: tuple[str, ...]
    spent_cost: float
    events: tuple[str, ...]
    deferred: tuple[str, ...] = ()


class AdapterFailure(RuntimeError):
    """适配器 fail-closed 失败；调度器按失败 attempt 记录并计入重试。

    ``kind`` 区分失败来源，``retryable`` 表示该来源在语义上是否值得重试。
    调度器当前对任何异常都按 ``max_attempts`` 重试，尚未按 kind 分流。
    把它放在 core 是为了让隔离层、HTTP 层和未来的 Host adapter 共用同一失败类型，
    避免任何一层反向依赖具体传输实现。
    """

    def __init__(self, message: str, *, kind: str = "adapter", retryable: bool = False):
        super().__init__(message)
        self.kind = kind
        self.retryable = retryable


class WorkerAdapter(Protocol):
    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        """Execute one node. Implement this in a DSH Host adapter."""


class ArtifactStore:
    """带完整性校验和原子发布的内容寻址文本 artifact 存储。"""

    def __init__(self, root: str | os.PathLike[str], *, max_bytes: int = 2_000_000):
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes

    def put_text(self, content: str) -> tuple[str, str]:
        """写入 UTF-8 文本；同一 digest 已存在时复用，不覆盖已有内容。"""
        if not isinstance(content, str):
            raise TypeError("artifact content must be text")
        data = content.encode("utf-8")
        if len(data) > self.max_bytes:
            raise ValueError(f"artifact exceeds max_bytes={self.max_bytes}")
        digest = hashlib.sha256(data).hexdigest()
        target = self.root / digest
        if target.is_symlink():
            raise IOError("artifact target must not be a symlink")
        if target.exists():
            # 已存在的文件也必须通过校验，避免静默复用损坏内容。
            existing = target.read_bytes()
            if len(existing) > self.max_bytes:
                raise IOError(f"artifact exceeds max_bytes={self.max_bytes}")
            if hashlib.sha256(existing).hexdigest() != digest:
                raise IOError(f"artifact integrity mismatch: {digest}")
            return f"artifact:{digest}", digest
        fd, tmp_name = tempfile.mkstemp(prefix=f".{digest}.", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            # 原子 rename 保证读者不会看到半个文件。
            os.replace(tmp_name, target)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        return f"artifact:{digest}", digest

    def read_text(self, ref: str) -> str:
        """读取并校验 artifact；拒绝路径穿越和 digest 不匹配。"""
        prefix = "artifact:"
        digest = ref.removeprefix(prefix) if isinstance(ref, str) else ""
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise KeyError(ref)
        target = self.root / digest
        if not target.exists():
            raise KeyError(ref)
        root_resolved = self.root.resolve()
        if target.is_symlink() or not target.resolve().is_relative_to(root_resolved):
            raise IOError("artifact path escapes store root")
        data = target.read_bytes()
        if len(data) > self.max_bytes:
            raise IOError(f"artifact exceeds max_bytes={self.max_bytes}")
        if hashlib.sha256(data).hexdigest() != digest:
            raise IOError(f"artifact integrity mismatch: {digest}")
        return data.decode("utf-8")


def validate_envelope(
    envelope: Mapping[str, Any],
    artifact_refs: Iterable[str] = (),
    *,
    max_bytes: int = 2_000_000,
) -> ValidationReport:
    """Perform deterministic checks before an optional independent verifier."""

    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")
    errors: list[str] = []
    refs = set(artifact_refs)
    if not isinstance(envelope, Mapping):
        return ValidationReport(False, ("envelope must be an object",), score=0.0)
    try:
        encoded_size = len(json.dumps(envelope, ensure_ascii=False, allow_nan=False).encode("utf-8"))
        if encoded_size > max_bytes:
            errors.append("envelope exceeds max_bytes")
    except (TypeError, ValueError):
        errors.append("envelope is not JSON serializable")
    if not isinstance(envelope.get("answer"), str):
        errors.append("answer must be a string")
    if not isinstance(envelope.get("claims"), list):
        errors.append("claims must be a list")
    citations = envelope.get("citations")
    if not isinstance(citations, list):
        errors.append("citations must be a list")
    else:
        for index, citation in enumerate(citations):
            if not isinstance(citation, str) or not citation.strip():
                errors.append(f"citations[{index}] must be a non-empty string")
    confidence = envelope.get("confidence")
    if (
        not isinstance(confidence, (int, float))
        or isinstance(confidence, bool)
        or not math.isfinite(float(confidence))
        or not 0 <= confidence <= 1
    ):
        errors.append("confidence must be between 0 and 1")
    warnings = envelope.get("warnings")
    if not isinstance(warnings, list):
        errors.append("warnings must be a list")
    else:
        for index, warning in enumerate(warnings):
            if not isinstance(warning, str) or not warning.strip():
                errors.append(f"warnings[{index}] must be a non-empty string")
    usage = envelope.get("usage")
    if not isinstance(usage, Mapping):
        errors.append("usage must be an object")
    else:
        total_tokens = usage.get("total_tokens")
        cost = usage.get("cost")
        if (
            not isinstance(total_tokens, (int, float))
            or isinstance(total_tokens, bool)
            or not math.isfinite(float(total_tokens))
            or total_tokens < 0
        ):
            errors.append("usage.total_tokens must be a finite non-negative number")
        if cost is None:
            errors.append("usage.cost is required")
        elif (
            not isinstance(cost, (int, float))
            or isinstance(cost, bool)
            or not math.isfinite(float(cost))
            or cost < 0
        ):
            errors.append("usage.cost must be a finite non-negative number")

    claims = envelope.get("claims", [])
    if isinstance(claims, list):
        seen: set[str] = set()
        for index, claim in enumerate(claims):
            if not isinstance(claim, Mapping):
                errors.append(f"claims[{index}] must be an object")
                continue
            claim_id = claim.get("id")
            if not isinstance(claim_id, str) or not claim_id:
                errors.append(f"claims[{index}].id is required")
            elif claim_id in seen:
                errors.append(f"duplicate claim id: {claim_id}")
            else:
                seen.add(claim_id)
            if not isinstance(claim.get("text"), str) or not claim.get("text"):
                errors.append(f"claims[{index}].text is required")
            evidence = claim.get("evidence_refs")
            if not isinstance(evidence, list) or not evidence:
                errors.append(f"claims[{index}].evidence_refs is required")
            elif any(not isinstance(ref, str) or ref not in refs for ref in evidence):
                errors.append(f"claims[{index}] contains an unknown evidence ref")

    score = 1.0 if not errors else max(0.0, 1.0 - min(1.0, len(errors) / 10))
    return ValidationReport(not errors, tuple(errors), score=score)


class LocalEchoAdapter:
    """Offline adapter used for smoke tests; never represents a real LLM call."""

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        return WorkerResult(
            envelope={
                "answer": f"completed: {node.prompt}",
                "claims": [],
                "citations": [],
                "confidence": 0.0,
                "warnings": ["local echo adapter; replace with a DSH Host adapter"],
                "usage": {"total_tokens": 1, "cost": 0.0},
            }
        )


class DemandAwareRouter:
    """根据任务需求、实时进度、预算和通道容量选择路由。

    先执行硬约束过滤（阶段质量、任务类别、并发槽位、预算），再进行软评分。
    这样路由结果可解释、可复现，同时不会把 Provider SDK 耦合进调度器。
    """

    def __init__(self, profiles: Iterable[RouteProfile]):
        self.profiles = tuple(profiles)
        ids = [profile.id for profile in self.profiles]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate route profile id")

    def availability(
        self,
        node: NodeSpec,
        *,
        stage: StagePolicy | None,
        active_by_route: Mapping[str, int],
        remaining_budget: float,
    ) -> str:
        """Explain why a task has no route: capacity, budget, or incompatibility."""
        compatible = []
        for profile in self.profiles:
            if profile.route_classes and node.route_class not in profile.route_classes:
                continue
            if stage and stage.preferred_routes and profile.id not in stage.preferred_routes:
                continue
            if stage and profile.quality + 1e-12 < stage.quality_floor:
                continue
            compatible.append(profile)
        if not compatible:
            return "incompatible"
        if not any(active_by_route.get(profile.id, 0) < profile.max_concurrency for profile in compatible):
            return "capacity"
        if not any(node.max_tokens / 1000.0 * profile.cost_per_1k_tokens <= remaining_budget + 1e-12 for profile in compatible):
            return "budget"
        return "available"

    def choose(
        self,
        node: NodeSpec,
        *,
        stage: StagePolicy | None,
        progress: float,
        active_by_route: Mapping[str, int],
        remaining_budget: float,
    ) -> RouteDecision | None:
        # 第一步只保留满足硬约束的通道，避免“高分但不可用”的假选择。
        eligible: list[RouteProfile] = []
        for profile in self.profiles:
            if profile.max_concurrency < 1 or active_by_route.get(profile.id, 0) >= profile.max_concurrency:
                continue
            if profile.route_classes and node.route_class not in profile.route_classes:
                continue
            if stage and stage.preferred_routes and profile.id not in stage.preferred_routes:
                continue
            estimated = node.max_tokens / 1000.0 * profile.cost_per_1k_tokens
            if estimated > remaining_budget + 1e-12:
                continue
            if stage and profile.quality + 1e-12 < stage.quality_floor:
                continue
            eligible.append(profile)
        if not eligible:
            return None

        # 第二步将质量需求、响应速度、成本和阶段偏好合成为可解释分数。
        max_speed = max(1.0 / max(profile.latency, 0.001) for profile in eligible)
        max_cost = max(profile.cost_per_1k_tokens for profile in eligible)
        quality_need = min(
            1.0,
            max(
                stage.quality_floor if stage else 0.0,
                0.6 * max(0.0, min(1.0, node.incremental_value))
                + 0.4 * max(0.0, min(1.0, node.urgency)),
            ),
        )
        scored: list[tuple[float, RouteProfile]] = []
        for profile in eligible:
            speed = (1.0 / max(profile.latency, 0.001)) / max_speed
            cost_penalty = profile.cost_per_1k_tokens / max(max_cost, 0.000001)
            preferred_bonus = 0.05 if stage and profile.id in stage.preferred_routes else 0.0
            score = (
                quality_need * profile.quality
                + (1.0 - quality_need) * speed
                - 0.15 * (1.0 - quality_need) * cost_penalty
                + preferred_bonus
                + 0.05 * (1.0 - min(1.0, progress)) * profile.quality
            )
            scored.append((score, profile))
        score, selected = max(scored, key=lambda item: (item[0], item[1].quality, item[1].id))
        return RouteDecision(
            profile_id=selected.id,
            provider=selected.provider,
            model=selected.model,
            reasoning_effort=selected.reasoning_effort,
            score=round(score, 6),
            estimated_cost=node.max_tokens / 1000.0 * selected.cost_per_1k_tokens,
            reason=(
                f"quality_need={quality_need:.2f}; progress={progress:.2f}; "
                f"route_capacity={active_by_route.get(selected.id, 0)}/{selected.max_concurrency}"
            ),
        )


class DAGScheduler:
    """基于 SQLite 的有界调度器，提供波次 gate、有限重试和预算结算。"""

    def __init__(self, db_path: str | os.PathLike[str], *, max_workers: int | None = None):
        if max_workers is not None and (isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1):
            raise ValueError("max_workers must be a positive integer")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=5.0)
        self._conn.row_factory = sqlite3.Row
        # WAL + busy_timeout 让多进程读写至少能有序等待；run owner 仍负责防止重复执行。
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        self._owner_id = uuid.uuid4().hex
        self._active_runs: set[str] = set()
        self._closed = False
        self._init_db()
        self._default_workers = max_workers

    def __enter__(self) -> "DAGScheduler":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if self._active_runs:
                raise RuntimeError("cannot close scheduler while execute is active")
            if not self._closed:
                self._closed = True
                self._conn.close()

    def _init_db(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    budget_cost REAL NOT NULL,
                    reserved_cost REAL NOT NULL DEFAULT 0,
                    spent_cost REAL NOT NULL DEFAULT 0,
                    max_nodes INTEGER NOT NULL,
                    max_workers INTEGER NOT NULL,
                    wave_success_threshold REAL NOT NULL,
                    cost_per_1k_tokens REAL NOT NULL,
                    deadline_at REAL NOT NULL,
                     stage_policies TEXT NOT NULL DEFAULT '[]',
                     route_profiles TEXT NOT NULL DEFAULT '[]',
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS nodes (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    depends_on TEXT NOT NULL,
                    wave INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    timeout_seconds REAL NOT NULL,
                    max_tokens INTEGER NOT NULL,
                    validation_policy TEXT NOT NULL,
                     stage TEXT NOT NULL DEFAULT '',
                     incremental_value REAL NOT NULL DEFAULT 1.0,
                     urgency REAL NOT NULL DEFAULT 0.0,
                     route_class TEXT NOT NULL DEFAULT 'default',
                     progress REAL NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                     current_attempt_id TEXT,
                    next_ready_at REAL NOT NULL DEFAULT 0,
                    error TEXT,
                    PRIMARY KEY (run_id, id)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    owner_id TEXT,
                     number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    lease_until REAL NOT NULL,
                    started_at REAL NOT NULL,
                    finished_at REAL,
                    error TEXT,
                    cost REAL NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS artifacts (
                    run_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    ref TEXT NOT NULL,
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    PRIMARY KEY (run_id, node_id, name)
                );
                CREATE TABLE IF NOT EXISTS budget_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    node_id TEXT,
                    attempt_id TEXT,
                    operation TEXT NOT NULL,
                    amount REAL NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    node_id TEXT,
                    event TEXT NOT NULL,
                    detail TEXT,
                    created_at REAL NOT NULL
                );
                """
            )

        # 迁移必须串行且处于同一 SQLite 事务；BEGIN IMMEDIATE 也避免两个 Host
        # 同时重命名 artifacts 时互相看到半成品。SQLite DDL 在事务回滚时可恢复。
        self._conn.execute("BEGIN IMMEDIATE")
        # 旧版本把 ref 设为全局主键，会导致相同内容覆盖其他 run 的 artifact 绑定。
        # 这里迁移为“每个 run/node/name 一条绑定”，文件内容仍由 digest 寻址。
        artifact_info = list(self._conn.execute("PRAGMA table_info(artifacts)"))
        artifact_columns = {row[1] for row in artifact_info}
        artifact_pk = [row[1] for row in artifact_info if row[5]]
        expected_artifact_pk = ["run_id", "node_id", "name"]
        if artifact_pk != expected_artifact_pk or not {"run_id", "node_id", "name", "ref", "path", "sha256"}.issubset(artifact_columns):
            self._conn.execute("DROP INDEX IF EXISTS idx_artifacts_node")
            self._conn.execute("ALTER TABLE artifacts RENAME TO artifacts_legacy")
            self._conn.execute(
                """
                CREATE TABLE artifacts (
                    run_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    ref TEXT NOT NULL,
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    PRIMARY KEY (run_id, node_id, name)
                )
                """
            )
            # 旧 MVP 表通常已经包含这些列；缺列时用空值保留可迁移性，随后由
            # 完整性检查阻止引用损坏记录，而不会在启动时 no such column。
            legacy = {row[1] for row in self._conn.execute("PRAGMA table_info(artifacts_legacy)")}
            expression = lambda name, fallback: name if name in legacy else fallback
            self._conn.execute(
                f"""
                INSERT OR IGNORE INTO artifacts(run_id,node_id,name,ref,path,sha256)
                SELECT {expression('run_id', "''")},{expression('node_id', "''")},
                       {expression('name', "'legacy'")},{expression('ref', "''")},
                       {expression('path', "''")},{expression('sha256', "''")}
                FROM artifacts_legacy
                """
            )
            self._conn.execute("DROP TABLE artifacts_legacy")

        # 逐列补齐早期 MVP 可能缺失的字段；默认值使已有行可安全升级。
        columns_to_add = (
            ("runs", "status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("runs", "budget_cost", "REAL NOT NULL DEFAULT 0"),
            ("runs", "reserved_cost", "REAL NOT NULL DEFAULT 0"),
            ("runs", "spent_cost", "REAL NOT NULL DEFAULT 0"),
            ("runs", "max_nodes", "INTEGER NOT NULL DEFAULT 1"),
            ("runs", "max_workers", "INTEGER NOT NULL DEFAULT 1"),
            ("runs", "wave_success_threshold", "REAL NOT NULL DEFAULT 1"),
            ("runs", "cost_per_1k_tokens", "REAL NOT NULL DEFAULT 0"),
            ("runs", "deadline_at", "REAL NOT NULL DEFAULT 0"),
            ("runs", "stage_policies", "TEXT NOT NULL DEFAULT '[]'"),
            ("runs", "route_profiles", "TEXT NOT NULL DEFAULT '[]'"),
            ("runs", "cancel_requested", "INTEGER NOT NULL DEFAULT 0"),
            ("runs", "created_at", "REAL NOT NULL DEFAULT 0"),
            ("runs", "owner_id", "TEXT"),
            ("runs", "owner_lease_until", "REAL NOT NULL DEFAULT 0"),
            ("nodes", "prompt", "TEXT NOT NULL DEFAULT ''"),
            ("nodes", "depends_on", "TEXT NOT NULL DEFAULT '[]'"),
            ("nodes", "wave", "INTEGER NOT NULL DEFAULT 0"),
            ("nodes", "priority", "INTEGER NOT NULL DEFAULT 0"),
            ("nodes", "max_attempts", "INTEGER NOT NULL DEFAULT 1"),
            ("nodes", "timeout_seconds", "REAL NOT NULL DEFAULT 60"),
            ("nodes", "max_tokens", "INTEGER NOT NULL DEFAULT 1"),
            ("nodes", "validation_policy", "TEXT NOT NULL DEFAULT 'strict'"),
            ("nodes", "stage", "TEXT NOT NULL DEFAULT ''"),
            ("nodes", "incremental_value", "REAL NOT NULL DEFAULT 1"),
            ("nodes", "urgency", "REAL NOT NULL DEFAULT 0"),
            ("nodes", "route_class", "TEXT NOT NULL DEFAULT 'default'"),
            ("nodes", "progress", "REAL NOT NULL DEFAULT 0"),
            ("nodes", "status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("nodes", "attempt_count", "INTEGER NOT NULL DEFAULT 0"),
            ("nodes", "current_attempt_id", "TEXT"),
            ("nodes", "next_ready_at", "REAL NOT NULL DEFAULT 0"),
            ("nodes", "error", "TEXT"),
            ("attempts", "run_id", "TEXT NOT NULL DEFAULT ''"),
            ("attempts", "node_id", "TEXT NOT NULL DEFAULT ''"),
            ("attempts", "owner_id", "TEXT"),
            ("attempts", "number", "INTEGER NOT NULL DEFAULT 1"),
            ("attempts", "status", "TEXT NOT NULL DEFAULT 'failed'"),
            ("attempts", "lease_until", "REAL NOT NULL DEFAULT 0"),
            ("attempts", "started_at", "REAL NOT NULL DEFAULT 0"),
            ("attempts", "finished_at", "REAL"),
            ("attempts", "error", "TEXT"),
            ("attempts", "cost", "REAL NOT NULL DEFAULT 0"),
            ("budget_ledger", "id", "INTEGER"),
            ("budget_ledger", "run_id", "TEXT NOT NULL DEFAULT ''"),
            ("budget_ledger", "node_id", "TEXT"),
            ("budget_ledger", "attempt_id", "TEXT"),
            ("budget_ledger", "operation", "TEXT NOT NULL DEFAULT 'legacy'"),
            ("budget_ledger", "amount", "REAL NOT NULL DEFAULT 0"),
            ("budget_ledger", "created_at", "REAL NOT NULL DEFAULT 0"),
            ("events", "id", "INTEGER"),
            ("events", "run_id", "TEXT NOT NULL DEFAULT ''"),
            ("events", "node_id", "TEXT"),
            ("events", "event", "TEXT NOT NULL DEFAULT 'legacy'"),
            ("events", "detail", "TEXT"),
            ("events", "created_at", "REAL NOT NULL DEFAULT 0"),
        )
        added_columns: set[tuple[str, str]] = set()
        for table, column, definition in columns_to_add:
            columns = {row[1] for row in self._conn.execute(f"PRAGMA table_info({table})")}
            if column not in columns:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                added_columns.add((table, column))
        migration_now = time.time()
        if ("runs", "deadline_at") in added_columns:
            self._conn.execute("UPDATE runs SET deadline_at=? WHERE deadline_at<=0", (migration_now + 3600.0,))
        if ("runs", "created_at") in added_columns:
            self._conn.execute("UPDATE runs SET created_at=? WHERE created_at<=0", (migration_now,))
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_ready ON nodes(run_id, status, next_ready_at)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_attempts_active ON attempts(run_id, status, lease_until)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_node ON artifacts(run_id, node_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, id)")
        self._conn.execute("PRAGMA user_version=2")
        self._conn.commit()

    @staticmethod
    def _validate_graph(run: RunSpec, nodes: list[NodeSpec]) -> None:
        """在写入 SQLite 前一次性校验输入，避免运行中才暴露配置错误。"""
        if not isinstance(run.id, str) or not run.id.strip():
            raise ValueError("run id is required")
        reserved_device = run.id.split(".", 1)[0].upper()
        if (
            run.id in {".", ".."}
            or run.id != run.id.strip()
            or any(char in '/\\:*?"<>|' or ord(char) < 32 for char in run.id)
            or run.id.endswith(".")
            or len(run.id.encode("utf-8")) > 100
            or reserved_device in {"CON", "PRN", "AUX", "NUL"}
            or (len(reserved_device) == 4 and reserved_device[:3] in {"COM", "LPT"} and reserved_device[3].isdigit() and reserved_device[3] != "0")
        ):
            raise ValueError("run id must be a portable identifier (<=100 UTF-8 bytes)")
        is_int = lambda value: isinstance(value, int) and not isinstance(value, bool)
        is_finite = lambda value: isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
        if not is_int(run.max_nodes) or run.max_nodes < 1 or len(nodes) > run.max_nodes:
            raise ValueError(f"DAG exceeds max_nodes={run.max_nodes}")
        if (
            not is_finite(run.budget_cost)
            or run.budget_cost < 0
            or not is_int(run.max_workers)
            or run.max_workers < 1
            or not is_finite(run.deadline_seconds)
            or run.deadline_seconds <= 0
            or not is_finite(run.cost_per_1k_tokens)
            or run.cost_per_1k_tokens < 0
        ):
            raise ValueError("invalid run budget, deadline, worker limit, or cost rate")

        ids = [node.id for node in nodes]
        if any(not isinstance(node_id, str) or not node_id.strip() for node_id in ids):
            raise ValueError("node id is required")
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate node id")
        known = set(ids)
        for node in nodes:
            if not isinstance(node.prompt, str):
                raise ValueError(f"node prompt must be text: {node.id}")
            if not is_int(node.wave) or node.wave < 0 or not is_int(node.priority):
                raise ValueError(f"wave and priority must be integers: {node.id}")
            if not is_int(node.max_attempts) or node.max_attempts < 1 or not is_finite(node.timeout_seconds) or node.timeout_seconds <= 0 or not is_int(node.max_tokens) or node.max_tokens < 1:
                raise ValueError(f"invalid limits for node: {node.id}")
            if not is_finite(node.incremental_value) or not is_finite(node.urgency):
                raise ValueError(f"non-finite node demand: {node.id}")
            if node.incremental_value < 0 or not 0 <= node.urgency <= 1:
                raise ValueError(f"invalid demand values: {node.id}")
            if not isinstance(node.stage, str) or not isinstance(node.route_class, str) or not isinstance(node.validation_policy, str):
                raise ValueError(f"node stage, route_class, and validation_policy must be text: {node.id}")
            if node.validation_policy not in {"strict", "lenient"}:
                raise ValueError(f"unsupported validation policy: {node.validation_policy}")
            if not isinstance(node.depends_on, (tuple, list)) or any(not isinstance(dep, str) or not dep for dep in node.depends_on):
                raise ValueError(f"dependencies must be a sequence of text ids: {node.id}")
            if len(node.depends_on) != len(set(node.depends_on)):
                raise ValueError(f"duplicate dependencies: {node.id}")
            missing = set(node.depends_on) - known
            if missing:
                raise ValueError(f"node {node.id} has missing dependencies: {sorted(missing)}")

        # 用 DFS 检查环，保证后续 ready 队列不会永久等待。
        graph = {node.id: set(node.depends_on) for node in nodes}
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node_id: str) -> None:
            if node_id in visiting:
                raise ValueError("DAG contains a cycle")
            if node_id in visited:
                return
            visiting.add(node_id)
            for dependency in graph[node_id]:
                visit(dependency)
            visiting.remove(node_id)
            visited.add(node_id)

        for node_id in graph:
            visit(node_id)
        wave_by_id = {node.id: node.wave for node in nodes}
        for node in nodes:
            # 反向 wave 边会同时被依赖状态和 wave gate 卡住，必须在提交时拒绝。
            if any(wave_by_id[dependency] > node.wave for dependency in node.depends_on):
                raise ValueError(f"node {node.id} depends on a later wave")

        if not is_finite(run.wave_success_threshold) or not 0 <= run.wave_success_threshold <= 1:
            raise ValueError("wave_success_threshold must be between 0 and 1")
        stage_names = [policy.name for policy in run.stage_policies]
        if any(not isinstance(name, str) or not name.strip() for name in stage_names):
            raise ValueError("stage policy name is required")
        if len(stage_names) != len(set(stage_names)):
            raise ValueError("duplicate stage policy name")
        for policy in run.stage_policies:
            if not is_int(policy.max_in_flight) or policy.max_in_flight < 1 or not is_int(policy.dispatch_batch) or policy.dispatch_batch < 1:
                raise ValueError(f"invalid stage limits: {policy.name}")
            if not is_finite(policy.target_progress) or not is_finite(policy.quality_floor) or not 0 <= policy.target_progress <= 1 or not 0 <= policy.quality_floor <= 1:
                raise ValueError(f"invalid stage demand bounds: {policy.name}")
            if not isinstance(policy.defer_excess, bool):
                raise ValueError(f"defer_excess must be bool: {policy.name}")
            if not isinstance(policy.preferred_routes, (tuple, list)) or any(not isinstance(route_id, str) or not route_id for route_id in policy.preferred_routes):
                raise ValueError(f"invalid preferred routes: {policy.name}")

        route_ids = [profile.id for profile in run.route_profiles]
        if any(
            not isinstance(profile.id, str)
            or not profile.id.strip()
            or not isinstance(profile.provider, str)
            or not profile.provider.strip()
            or not isinstance(profile.model, str)
            or not profile.model.strip()
            or not isinstance(profile.reasoning_effort, str)
            or not profile.reasoning_effort.strip()
            for profile in run.route_profiles
        ):
            raise ValueError("route profile id, provider, and model are required")
        if len(route_ids) != len(set(route_ids)):
            raise ValueError("duplicate route profile id")
        known_route_ids = set(route_ids)
        for policy in run.stage_policies:
            if any(route_id not in known_route_ids for route_id in policy.preferred_routes):
                raise ValueError(f"unknown preferred route: {policy.name}")
        for profile in run.route_profiles:
            if (
                not is_int(profile.max_concurrency)
                or profile.max_concurrency < 1
                or not is_finite(profile.latency)
                or profile.latency <= 0
                or not is_finite(profile.cost_per_1k_tokens)
                or profile.cost_per_1k_tokens < 0
                or not is_finite(profile.quality)
                or not isinstance(profile.route_classes, (tuple, list))
                or any(not isinstance(item, str) or not item for item in profile.route_classes)
            ):
                raise ValueError(f"invalid route profile: {profile.id}")
            if not 0 <= profile.quality <= 1:
                raise ValueError(f"route quality must be between 0 and 1: {profile.id}")

    def submit(self, run: RunSpec, nodes: Iterable[NodeSpec]) -> None:
        node_list = list(nodes)
        self._validate_graph(run, node_list)
        now = time.time()
        with self._lock, self._conn:
            # Run、Node 和预算初始值必须在同一事务中写入，避免半提交 DAG。
            existing = self._conn.execute("SELECT id FROM runs").fetchall()
            if any(row["id"].casefold() == run.id.casefold() for row in existing):
                raise ValueError(f"run id collides case-insensitively with an existing artifact directory: {run.id}")
            self._conn.execute(
                "INSERT INTO runs(id,status,budget_cost,max_nodes,max_workers,wave_success_threshold,cost_per_1k_tokens,deadline_at,stage_policies,route_profiles,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    run.id,
                    "pending",
                    run.budget_cost,
                    run.max_nodes,
                    run.max_workers,
                    run.wave_success_threshold,
                    run.cost_per_1k_tokens,
                    now + run.deadline_seconds,
                    json.dumps([asdict(policy) for policy in run.stage_policies]),
                    json.dumps([asdict(profile) for profile in run.route_profiles]),
                    now,
                ),
            )
            for node in node_list:
                self._conn.execute(
                    "INSERT INTO nodes(run_id,id,prompt,depends_on,wave,priority,max_attempts,timeout_seconds,max_tokens,validation_policy,stage,incremental_value,urgency,route_class,status,current_attempt_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        run.id,
                        node.id,
                        node.prompt,
                        json.dumps(list(node.depends_on)),
                        node.wave,
                        node.priority,
                        node.max_attempts,
                        node.timeout_seconds,
                        node.max_tokens,
                        node.validation_policy,
                        node.stage or f"wave-{node.wave}",
                        node.incremental_value,
                        node.urgency,
                        node.route_class,
                        "pending",
                        None,
                    ),
                )
            self._event(run.id, None, "run.created", f"nodes={len(node_list)}")

    def cancel(self, run_id: str) -> None:
        with self._lock, self._conn:
            changed = self._conn.execute(
                "UPDATE runs SET cancel_requested=1 WHERE id=? AND status NOT IN ('succeeded','failed','cancelled')",
                (run_id,),
            ).rowcount
            if changed:
                self._event_once(run_id, None, "run.cancel_requested", None)
            # 取消由另一个 scheduler 发起时，也要立即结束数据库中的 running
            # attempt 并释放 reservation；本地 execute 后续只需丢弃其 future。
            for attempt in self._conn.execute(
                "SELECT id,node_id FROM attempts WHERE run_id=? AND status='running'",
                (run_id,),
            ).fetchall():
                claimed = self._conn.execute(
                    "UPDATE attempts SET status='failed',finished_at=?,error=? WHERE id=? AND status='running'",
                    (time.time(), "run cancelled", attempt["id"]),
                ).rowcount
                if not claimed:
                    continue
                reserve = self._conn.execute(
                    "SELECT COALESCE(SUM(amount),0) FROM budget_ledger WHERE attempt_id=? AND operation='reserve'",
                    (attempt["id"],),
                ).fetchone()[0]
                released = self._conn.execute(
                    "SELECT COALESCE(SUM(-amount),0) FROM budget_ledger WHERE attempt_id=? AND operation='release'",
                    (attempt["id"],),
                ).fetchone()[0]
                remaining = max(0.0, float(reserve) - float(released))
                if remaining:
                    self._release(run_id, attempt["node_id"], attempt["id"], remaining)
                self._conn.execute(
                    "UPDATE nodes SET status='cancelled',progress=0,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?",
                    ("run cancelled", run_id, attempt["node_id"], attempt["id"]),
                )

    def _event(self, run_id: str, node_id: str | None, event: str, detail: str | None) -> None:
        self._conn.execute(
            "INSERT INTO events(run_id,node_id,event,detail,created_at) VALUES(?,?,?,?,?)",
            (run_id, node_id, event, detail, time.time()),
        )

    def _event_once(self, run_id: str, node_id: str | None, event: str, detail: str | None) -> None:
        """以单条 INSERT..SELECT 实现跨连接幂等，避免 SELECT/INSERT 竞态。"""
        self._conn.execute(
            """
            INSERT INTO events(run_id,node_id,event,detail,created_at)
            SELECT ?,?,?,?,?
            WHERE NOT EXISTS (
                SELECT 1 FROM events
                WHERE run_id=? AND node_id IS ? AND event=?
            )
            """,
            (run_id, node_id, event, detail, time.time(), run_id, node_id, event),
        )

    def _get_run(self, run_id: str) -> sqlite3.Row:
        row = self._conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        return row

    def _get_nodes(self, run_id: str) -> dict[str, sqlite3.Row]:
        return {row["id"]: row for row in self._conn.execute("SELECT * FROM nodes WHERE run_id=?", (run_id,))}

    def _artifact_root(self, run_id: str) -> Path:
        """返回与平台路径语义无关的 run artifact 目录。

        run_id 仍保留在 SQLite 中作为业务键，但不直接作为目录名；这样即使
        文件系统存在大小写或 Unicode 规范化差异，不同 run 也不会共享 artifact 根。
        """
        key = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
        return self.db_path.with_name(self.db_path.stem + "-artifacts") / key

    @staticmethod
    def _stage_policies(run: sqlite3.Row) -> dict[str, StagePolicy]:
        return {
            item["name"]: StagePolicy(
                name=item["name"],
                max_in_flight=item.get("max_in_flight", 1),
                dispatch_batch=item.get("dispatch_batch", 1),
                target_progress=item.get("target_progress", 1.0),
                quality_floor=item.get("quality_floor", 0.0),
                preferred_routes=tuple(item.get("preferred_routes", ())),
                defer_excess=item.get("defer_excess", True),
            )
            for item in json.loads(run["stage_policies"] or "[]")
        }

    @staticmethod
    def _router(run: sqlite3.Row) -> DemandAwareRouter | None:
        profiles = [
            RouteProfile(
                id=item["id"],
                provider=item["provider"],
                model=item["model"],
                reasoning_effort=item.get("reasoning_effort", "medium"),
                quality=item.get("quality", 0.5),
                latency=item.get("latency", 1.0),
                cost_per_1k_tokens=item.get("cost_per_1k_tokens", 0.01),
                max_concurrency=item.get("max_concurrency", 64),
                route_classes=tuple(item.get("route_classes", ())),
            )
            for item in json.loads(run["route_profiles"] or "[]")
        ]
        return DemandAwareRouter(profiles) if profiles else None

    @staticmethod
    def _node_from_row(row: sqlite3.Row) -> NodeSpec:
        return NodeSpec(
            id=row["id"],
            prompt=row["prompt"],
            depends_on=tuple(json.loads(row["depends_on"])),
            wave=row["wave"],
            priority=row["priority"],
            max_attempts=row["max_attempts"],
            timeout_seconds=row["timeout_seconds"],
            max_tokens=row["max_tokens"],
            validation_policy=row["validation_policy"],
            stage=row["stage"] or f"wave-{row['wave']}",
            incremental_value=row["incremental_value"],
            urgency=row["urgency"],
            route_class=row["route_class"],
        )

    @staticmethod
    def _stage_progress(rows: dict[str, sqlite3.Row], stage: str) -> tuple[float, float, float, int]:
        members = [row for row in rows.values() if (row["stage"] or f"wave-{row['wave']}") == stage]
        total = sum(max(0.0, row["incremental_value"]) for row in members)
        completed = sum(max(0.0, row["incremental_value"]) for row in members if row["status"] == "succeeded")
        active = sum(
            max(0.0, row["incremental_value"]) * max(0.0, min(1.0, row["progress"]))
            for row in members
            if row["status"] == "running"
        )
        pending = sum(1 for row in members if row["status"] == "pending")
        return total, completed, active, pending

    @staticmethod
    def _stage_inflight_value(rows: dict[str, sqlite3.Row], stage: str) -> float:
        return sum(
            max(0.0, row["incremental_value"])
            for row in rows.values()
            if (row["stage"] or f"wave-{row['wave']}") == stage and row["status"] == "running"
        )

    def _defer_excess(self, run_id: str, rows: dict[str, sqlite3.Row], policies: Mapping[str, StagePolicy]) -> None:
        for stage, policy in policies.items():
            if not policy.defer_excess or policy.target_progress >= 1.0:
                continue
            total, completed, active, _ = self._stage_progress(rows, stage)
            target = total * policy.target_progress
            if active > 0 or completed + 1e-9 < target:
                continue
            for row in rows.values():
                row_stage = row["stage"] or f"wave-{row['wave']}"
                if row_stage == stage and row["status"] == "pending":
                    self._conn.execute(
                        "UPDATE nodes SET status='deferred',error=? WHERE run_id=? AND id=?",
                        ("incremental demand target satisfied", run_id, row["id"]),
                    )
                    self._event(run_id, row["id"], "task.deferred", f"stage={stage};target_progress={policy.target_progress}")

    def _wave_gate(self, rows: dict[str, sqlite3.Row], wave: int, threshold: float) -> tuple[bool, str | None]:
        """检查前置波次是否达到成功阈值。

        ``deferred`` 是策略明确产生的终态：它不等价于节点成功，也不能满足
        直接依赖；但它代表该节点已被需求截断，因此不能让波次 gate 永久失败。
        """
        prior_waves = sorted({row["wave"] for row in rows.values() if row["wave"] < wave})
        for prior in prior_waves:
            members = [row for row in rows.values() if row["wave"] == prior]
            if not all(row["status"] in TERMINAL for row in members):
                return False, "waiting for previous wave"
            gate_passed = sum(row["status"] in {"succeeded", "deferred"} for row in members)
            ratio = gate_passed / len(members) if members else 1.0
            if ratio < threshold:
                return False, f"wave {prior} gate failed ({ratio:.2f} < {threshold:.2f})"
        return True, None

    def _dependency_state(self, row: sqlite3.Row, rows: dict[str, sqlite3.Row]) -> str:
        dependencies = json.loads(row["depends_on"])
        if any(rows[dependency]["status"] in {"failed", "blocked", "cancelled", "deferred"} for dependency in dependencies):
            return "blocked"
        if all(rows[dependency]["status"] == "succeeded" for dependency in dependencies):
            return "ready"
        return "waiting"

    def _artifact_refs(self, run_id: str, node: sqlite3.Row, rows: dict[str, sqlite3.Row]) -> dict[str, str]:
        refs: dict[str, str] = {}
        ambiguous_names: set[str] = set()
        for dependency in json.loads(node["depends_on"]):
            for artifact in self._conn.execute("SELECT * FROM artifacts WHERE run_id=? AND node_id=?", (run_id, dependency)):
                name, ref = artifact["name"], artifact["ref"]
                # 同名 artifact 不再静默覆盖：保留 dependency/name 命名空间，
                # 只有唯一来源才提供短名称兼容旧 adapter。
                refs[f"{dependency}/{name}"] = ref
                if name not in ambiguous_names:
                    previous = refs.get(name)
                    if previous is None:
                        refs[name] = ref
                    elif previous != ref:
                        refs.pop(name, None)
                        ambiguous_names.add(name)
                refs[ref] = ref
        return refs

    def _reserve(
        self,
        run: sqlite3.Row,
        node: sqlite3.Row,
        attempt_id: str,
        cost_per_1k_tokens: float | None = None,
    ) -> float | None:
        rate = run["cost_per_1k_tokens"] if cost_per_1k_tokens is None else cost_per_1k_tokens
        estimate = node["max_tokens"] / 1000.0 * rate
        # 单条条件 UPDATE 代替 read-then-update，避免多 Host 连接在 WAL 下发生
        # SQLITE_BUSY_SNAPSHOT 或同时超额预留。
        changed = self._conn.execute(
            """
            UPDATE runs SET reserved_cost=reserved_cost+?
            WHERE id=? AND spent_cost + reserved_cost + ? <= budget_cost + 1e-12
            """,
            (estimate, run["id"], estimate),
        ).rowcount
        if not changed:
            return None
        self._conn.execute(
            "INSERT INTO budget_ledger(run_id,node_id,attempt_id,operation,amount,created_at) VALUES(?,?,?,?,?,?)",
            (run["id"], node["id"], attempt_id, "reserve", estimate, time.time()),
        )
        return estimate

    def _release(self, run_id: str, node_id: str, attempt_id: str, amount: float, actual: float = 0.0) -> None:
        self._conn.execute("UPDATE runs SET reserved_cost=MAX(0,reserved_cost-?) WHERE id=?", (amount, run_id))
        if amount:
            self._conn.execute(
                "INSERT INTO budget_ledger(run_id,node_id,attempt_id,operation,amount,created_at) VALUES(?,?,?,?,?,?)",
                (run_id, node_id, attempt_id, "release", -amount, time.time()),
            )
        if actual:
            # 释放当前 reservation 后，其他并发 attempt 的 reservation 仍然占用预算；
            # charge 只能使用「预算 - 已花费 - 其余 reservation」，维护硬上限不变量。
            current = self._conn.execute("SELECT budget_cost,spent_cost,reserved_cost FROM runs WHERE id=?", (run_id,)).fetchone()
            available = max(0.0, float(current["budget_cost"] - current["spent_cost"] - current["reserved_cost"])) if current else 0.0
            charge = min(float(actual), available)
            overrun = float(actual) - charge
            if charge:
                self._conn.execute("UPDATE runs SET spent_cost=spent_cost+? WHERE id=?", (charge, run_id))
                self._conn.execute(
                    "INSERT INTO budget_ledger(run_id,node_id,attempt_id,operation,amount,created_at) VALUES(?,?,?,?,?,?)",
                    (run_id, node_id, attempt_id, "charge", charge, time.time()),
                )
            if overrun > 1e-12:
                # 记录 provider 报告的超预算部分，但不把 spent_cost 推过硬预算上限。
                self._conn.execute(
                    "INSERT INTO budget_ledger(run_id,node_id,attempt_id,operation,amount,created_at) VALUES(?,?,?,?,?,?)",
                    (run_id, node_id, attempt_id, "overrun", overrun, time.time()),
                )

    @staticmethod
    def _actual_cost(
        envelope: Mapping[str, Any],
        run: sqlite3.Row,
        cost_per_1k_tokens: float | None = None,
    ) -> float:
        usage = envelope.get("usage", {})
        if not isinstance(usage, Mapping):
            return 0.0
        cost = usage.get("cost")
        if isinstance(cost, (int, float)) and not isinstance(cost, bool) and math.isfinite(float(cost)) and cost >= 0:
            return float(cost)
        tokens = usage.get("total_tokens", 0)
        if isinstance(tokens, (int, float)) and not isinstance(tokens, bool) and math.isfinite(float(tokens)) and tokens >= 0:
            rate = run["cost_per_1k_tokens"] if cost_per_1k_tokens is None else cost_per_1k_tokens
            return float(tokens) / 1000.0 * rate
        return float("inf")

    def heartbeat(self, run_id: str, node_id: str, attempt_id: str, lease_seconds: float) -> bool:
        """Extend one active lease; adapters may call this during long work."""
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be a finite positive number")
        with self._lock, self._conn:
            run = self._conn.execute("SELECT deadline_at,cancel_requested,owner_id FROM runs WHERE id=?", (run_id,)).fetchone()
            if not run or run["owner_id"] != self._owner_id or run["cancel_requested"] or time.time() >= run["deadline_at"]:
                return False
            lease_until = min(time.time() + lease_seconds, run["deadline_at"])
            changed = self._conn.execute(
                """
                UPDATE attempts SET lease_until=?
                WHERE id=? AND run_id=? AND node_id=? AND status='running'
                  AND EXISTS (
                      SELECT 1 FROM nodes
                      WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?
                  )
                """,
                (lease_until, attempt_id, run_id, node_id, run_id, node_id, attempt_id),
            ).rowcount
            if changed:
                self._event(run_id, node_id, "task.heartbeat", f"lease_seconds={lease_seconds:g}")
            return bool(changed)

    def report_progress(
        self,
        run_id: str,
        node_id: str,
        attempt_id: str,
        progress: float,
        detail: str | None = None,
    ) -> bool:
        """Record monotonic worker progress without blocking the worker on callbacks."""
        if (
            isinstance(progress, bool)
            or not isinstance(progress, (int, float))
            or not math.isfinite(float(progress))
            or not 0.0 <= progress <= 1.0
        ):
            raise ValueError("progress must be a finite number between 0 and 1")
        if detail is not None and not isinstance(detail, str):
            raise TypeError("progress detail must be text")
        with self._lock, self._conn:
            attempt = self._conn.execute(
                "SELECT status FROM attempts WHERE id=? AND run_id=? AND node_id=?",
                (attempt_id, run_id, node_id),
            ).fetchone()
            owner = self._conn.execute("SELECT owner_id FROM runs WHERE id=?", (run_id,)).fetchone()
            node = self._conn.execute(
                "SELECT status,progress,current_attempt_id FROM nodes WHERE run_id=? AND id=?",
                (run_id, node_id),
            ).fetchone()
            if not owner or owner["owner_id"] != self._owner_id or not attempt or not node or attempt["status"] != "running" or node["status"] != "running" or node["current_attempt_id"] != attempt_id:
                return False
            if progress + 1e-12 < node["progress"]:
                return False
            self._conn.execute("UPDATE nodes SET progress=? WHERE run_id=? AND id=?", (progress, run_id, node_id))
            payload = {"progress": progress, "attempt_id": attempt_id}
            if detail:
                payload["detail"] = detail
            self._event(run_id, node_id, "task.progress", json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return True

    def recover_stale(
        self,
        run_id: str,
        *,
        now: float | None = None,
        owner_id: str | None = None,
        takeover: bool = False,
    ) -> tuple[str, ...]:
        """Requeue expired or prior-owner attempts after restart/takeover.

        只有调用者已经持有 run owner，或 run owner 的租约已过期，才会按
        ``owner_id`` 回收未过期的旧 attempt；活跃的其他 owner 不会被 watchdog
        误伤。无 ``owner_id`` 时仍只回收自然过期的 attempt。``takeover`` 仅供
        刚接管 run 的执行器启用，用于立即回收极旧 schema 中没有 owner_id 的 attempt。
        """
        now = time.time() if now is None else now
        if not isinstance(takeover, bool):
            raise TypeError("takeover must be a boolean")
        if owner_id is not None and (not isinstance(owner_id, str) or not owner_id):
            raise ValueError("owner_id must be a non-empty string")
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(float(now)):
            raise ValueError("now must be a finite timestamp")
        recovered: list[str] = []
        with self._lock, self._conn:
            run = self._get_run(run_id)
            stale_sql = (
                "SELECT a.*, n.max_attempts, n.attempt_count FROM attempts a "
                "JOIN nodes n ON n.run_id=a.run_id AND n.id=a.node_id "
                "WHERE a.run_id=? AND a.status='running' AND (a.lease_until<?"
            )
            stale_params: list[Any] = [run_id, now]
            can_recover_prior_owner = owner_id is not None and (
                run["owner_id"] == owner_id
                or run["owner_id"] is None
                or run["owner_lease_until"] < now
            )
            if can_recover_prior_owner:
                if takeover:
                    stale_sql += " OR (a.owner_id IS NULL OR a.owner_id!=?)"
                else:
                    stale_sql += " OR (a.owner_id IS NOT NULL AND a.owner_id!=?)"
                stale_params.append(owner_id)
            stale_sql += ")"
            stale = self._conn.execute(stale_sql, tuple(stale_params)).fetchall()
            for attempt in stale:
                # 先 CAS 认领 attempt，再释放 reservation，避免 watchdog/owner 重复结算。
                error = "worker lease expired; recovered for at-least-once retry"
                claimed = self._conn.execute(
                    "UPDATE attempts SET status='failed',finished_at=?,error=? WHERE id=? AND status='running'",
                    (now, error, attempt["id"]),
                ).rowcount
                if not claimed:
                    continue
                reserve = self._conn.execute(
                    "SELECT COALESCE(SUM(amount),0) FROM budget_ledger WHERE attempt_id=? AND operation='reserve'",
                    (attempt["id"],),
                ).fetchone()[0]
                released = self._conn.execute(
                    "SELECT COALESCE(SUM(-amount),0) FROM budget_ledger WHERE attempt_id=? AND operation='release'",
                    (attempt["id"],),
                ).fetchone()[0]
                remaining = max(0.0, float(reserve) - float(released))
                if remaining:
                    self._release(run_id, attempt["node_id"], attempt["id"], remaining)
                node = self._conn.execute(
                    "SELECT status,current_attempt_id FROM nodes WHERE run_id=? AND id=?",
                    (run_id, attempt["node_id"]),
                ).fetchone()
                # 只有当前 attempt 能推进 node；旧 schema 的 NULL 只允许在 node
                # 仍为 running 时被第一个 recovery 处理，后续 stale rows 不会互相覆盖。
                current = node and (node["current_attempt_id"] in {None, attempt["id"]}) and node["status"] == "running"
                if current and (run["cancel_requested"] or now >= run["deadline_at"]):
                    self._conn.execute("UPDATE nodes SET status='cancelled',progress=0,error=? WHERE run_id=? AND id=? AND status='running'", (error, run_id, attempt["node_id"]))
                elif current and attempt["attempt_count"] < attempt["max_attempts"]:
                    self._conn.execute("UPDATE nodes SET status='pending',progress=0,next_ready_at=?,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running'", (now, error, run_id, attempt["node_id"]))
                elif current:
                    self._conn.execute("UPDATE nodes SET status='failed',progress=0,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running'", (error, run_id, attempt["node_id"]))
                self._event(run_id, attempt["node_id"], "task.recovered", error)
                recovered.append(attempt["node_id"])
        return tuple(recovered)

    def _store_artifacts(self, run_id: str, node_id: str, artifacts: Mapping[str, str], store: ArtifactStore) -> dict[str, str]:
        refs: dict[str, str] = {}
        for name, content in artifacts.items():
            if not isinstance(name, str) or not isinstance(content, str):
                raise ValueError("artifact names and contents must be strings")
            ref, digest = store.put_text(content)
            self._conn.execute(
                """
                INSERT INTO artifacts(run_id,node_id,name,ref,path,sha256)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(run_id,node_id,name) DO UPDATE SET
                    ref=excluded.ref, path=excluded.path, sha256=excluded.sha256
                """,
                (run_id, node_id, name, ref, str(store.root / digest), digest),
            )
            refs[name] = ref
            refs[ref] = ref
        return refs

    def execute(self, adapter: WorkerAdapter, run_id: str | None = None) -> RunResult:
        with self._lock:
            if self._closed:
                raise RuntimeError("scheduler is closed")
            if run_id is None:
                row = self._conn.execute("SELECT id FROM runs WHERE status IN ('pending','running') ORDER BY created_at LIMIT 1").fetchone()
                if row is None:
                    raise KeyError("no pending run")
                run_id = row["id"]
            if run_id in self._active_runs:
                # 同一 scheduler 不允许同一 run 重入；SQL owner 条件允许同 owner，
                # 因此必须在内存 active 集合中先行拦截第二个执行循环。
                raise RuntimeError(f"run is already being executed: {run_id}")
            run = self._get_run(run_id)
            if run["status"] in RUN_TERMINAL:
                # 终态 run 不可重新 claim；重复 execute 只读返回，避免重复
                # run.completed 事件或意外重启已结束节点。
                return self.result(run_id)
            now = time.time()
            # 记录 claim 前是否接管了另一个/无 owner 的 running run；这决定
            # legacy NULL owner_id attempt 是否可以在 takeover 时立即回收。
            takeover_recovery = run["status"] == "running" and run["owner_id"] != self._owner_id
            with self._conn:
                claimed = self._conn.execute(
                    """
                    UPDATE runs
                    SET status='running', owner_id=?, owner_lease_until=?
                    WHERE id=? AND (owner_id IS NULL OR owner_id=? OR owner_lease_until<?)
                    """,
                    (self._owner_id, now + 30.0, run_id, self._owner_id, now),
                ).rowcount
                if not claimed:
                    raise RuntimeError(f"run is already being executed: {run_id}")
            run = self._get_run(run_id)
            self._active_runs.add(run_id)

            def release_claim() -> None:
                """无论初始化或执行哪里失败，都释放 owner 与 active 标记。"""
                with self._lock, self._conn:
                    self._conn.execute(
                        "UPDATE runs SET owner_id=NULL,owner_lease_until=0 WHERE id=? AND owner_id=?",
                        (run_id, self._owner_id),
                    )
                    self._active_runs.discard(run_id)

            # 接管重启/过期 owner 后自动回收不可见的 running attempts，避免执行循环
            # 永远等待；current_attempt_id 防止多个 stale attempt 互相覆盖节点。
            try:
                self.recover_stale(run_id, owner_id=self._owner_id, takeover=takeover_recovery)
            except Exception:
                release_claim()
                raise

        events: list[str] = []
        active: dict[concurrent.futures.Future[WorkerResult], dict[str, Any]] = {}
        # RunSpec 是单次运行上限，scheduler 构造参数是每个 run 的默认上限，取二者较小值。
        worker_limit = min(self._default_workers or run["max_workers"], run["max_workers"])
        executors: list[concurrent.futures.ThreadPoolExecutor] = []
        try:
            # artifact root 或线程池初始化失败时，也必须经过同一 owner 清理路径。
            artifact_store = ArtifactStore(self._artifact_root(run_id))
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_limit)
            executors.append(executor)
        except BaseException:
            release_claim()
            raise
        try:
            while True:
                with self._lock, self._conn:
                    run = self._get_run(run_id)
                    owner_renewed = self._conn.execute(
                        "UPDATE runs SET owner_lease_until=? WHERE id=? AND owner_id=?",
                        (time.time() + 30.0, run_id, self._owner_id),
                    ).rowcount
                    if not owner_renewed:
                        raise RuntimeError(f"run execution lease lost: {run_id}")
                    rows = self._get_nodes(run_id)
                    policies = self._stage_policies(run)
                    router = self._router(run)
                    now = time.time()
                    cancellation_reason = None
                    cancellation_event = None
                    if run["cancel_requested"]:
                        cancellation_reason = "run cancelled"
                        cancellation_event = "run.cancelled"
                    elif now >= run["deadline_at"]:
                        cancellation_reason = "run deadline exceeded"
                        cancellation_event = "run.deadline"
                    if cancellation_reason:
                        # 运行中任务先发协作式停止信号并结算 attempt；结果迟到时由
                        # _finish_future 的 CAS/终态检查丢弃，不能重新成功提交。
                        for future, info in list(active.items()):
                            active.pop(future, None)
                            info["cancel_event"].set()
                            self._fail_attempt(run_id, info, cancellation_reason, events)
                        for row in rows.values():
                            if row["status"] not in TERMINAL:
                                self._conn.execute(
                                    "UPDATE nodes SET status='cancelled',error=? WHERE run_id=? AND id=?",
                                    (cancellation_reason, run_id, row["id"]),
                                )
                        self._event_once(run_id, None, cancellation_event, None)
                    rows = self._get_nodes(run_id)
                    self._defer_excess(run_id, rows, policies)
                    rows = self._get_nodes(run_id)
                    for row in rows.values():
                        if row["status"] != "pending" or row["next_ready_at"] > now:
                            continue
                        gate_ok, gate_reason = self._wave_gate(rows, row["wave"], run["wave_success_threshold"])
                        if not gate_ok and gate_reason and "gate failed" in gate_reason:
                            self._conn.execute("UPDATE nodes SET status='blocked',error=? WHERE run_id=? AND id=?", (gate_reason, run_id, row["id"]))
                            self._event(run_id, row["id"], "task.blocked", gate_reason)
                            continue
                        if not gate_ok:
                            continue
                        dep_state = self._dependency_state(row, rows)
                        if dep_state == "blocked":
                            self._conn.execute("UPDATE nodes SET status='blocked',error=? WHERE run_id=? AND id=?", ("dependency did not succeed", run_id, row["id"]))
                            self._event(run_id, row["id"], "task.blocked", "dependency did not succeed")
                    rows = self._get_nodes(run_id)
                    capacity = max(0, worker_limit - len(active))
                    active_by_stage: dict[str, int] = {}
                    active_by_route: dict[str, int] = {}
                    for info in active.values():
                        active_by_stage[info["stage"]] = active_by_stage.get(info["stage"], 0) + 1
                        if info.get("route"):
                            active_by_route[info["route"]["profile_id"]] = active_by_route.get(info["route"]["profile_id"], 0) + 1
                    dispatched_by_stage: dict[str, int] = {}
                    # 同一轮内刚派发的节点不在 rows 快照中，单独累计其增量价值，
                    # 防止 dispatch_batch 一次性越过 stage target_progress。
                    dispatched_value_by_stage: dict[str, float] = {}
                    candidates = []
                    for row in rows.values():
                        if row["status"] != "pending" or row["next_ready_at"] > now:
                            continue
                        gate_ok, _ = self._wave_gate(rows, row["wave"], run["wave_success_threshold"])
                        if gate_ok and self._dependency_state(row, rows) == "ready":
                            candidates.append(row)
                    candidates.sort(key=lambda candidate: (-candidate["priority"], -candidate["incremental_value"], -candidate["urgency"], candidate["wave"], candidate["id"]))
                    for row in candidates:
                        if sum(dispatched_by_stage.values()) >= capacity:
                            break
                        node = self._node_from_row(row)
                        stage_name = node.stage or f"wave-{node.wave}"
                        policy = policies.get(stage_name)
                        total, completed, active_value, _ = self._stage_progress(rows, stage_name)
                        if policy:
                            if active_by_stage.get(stage_name, 0) >= policy.max_in_flight:
                                continue
                            if dispatched_by_stage.get(stage_name, 0) >= policy.dispatch_batch:
                                continue
                            target = total * policy.target_progress
                            reserved_value = self._stage_inflight_value(rows, stage_name) + dispatched_value_by_stage.get(stage_name, 0.0)
                            if completed + reserved_value >= target - 1e-9:
                                continue
                        progress = min(1.0, (completed + active_value) / total) if total else 0.0
                        budget_row = self._conn.execute("SELECT budget_cost,spent_cost,reserved_cost FROM runs WHERE id=?", (run_id,)).fetchone()
                        remaining_budget = max(0.0, budget_row["budget_cost"] - budget_row["spent_cost"] - budget_row["reserved_cost"])
                        decision = router.choose(
                            node,
                            stage=policy,
                            progress=progress,
                            active_by_route=active_by_route,
                            remaining_budget=remaining_budget,
                        ) if router else None
                        if router and decision is None:
                            route_state = router.availability(
                                node,
                                stage=policy,
                                active_by_route=active_by_route,
                                remaining_budget=remaining_budget,
                            )
                            if route_state == "capacity":
                                continue
                            if route_state == "budget" and budget_row["reserved_cost"] > 0:
                                ready_at = time.time() + 0.25
                                self._conn.execute("UPDATE nodes SET next_ready_at=?,error=? WHERE run_id=? AND id=?", (ready_at, "waiting for route budget headroom", run_id, row["id"]))
                                self._event(run_id, row["id"], "task.waiting", "waiting for route budget headroom")
                            else:
                                reason = f"no compatible route ({route_state})"
                                self._conn.execute("UPDATE nodes SET status='blocked',error=? WHERE run_id=? AND id=?", (reason, run_id, row["id"]))
                                self._event(run_id, row["id"], "task.blocked", reason)
                            continue
                        attempt_id = uuid.uuid4().hex
                        route_rate = None if decision is None else next(
                            profile.cost_per_1k_tokens for profile in router.profiles if profile.id == decision.profile_id
                        )
                        estimate = self._reserve(run, row, attempt_id, route_rate)
                        if estimate is None:
                            current = self._conn.execute("SELECT reserved_cost FROM runs WHERE id=?", (run_id,)).fetchone()
                            if current["reserved_cost"] > 0:
                                ready_at = time.time() + 0.25
                                self._conn.execute("UPDATE nodes SET next_ready_at=?,error=? WHERE run_id=? AND id=?", (ready_at, "waiting for budget headroom", run_id, row["id"]))
                                self._event(run_id, row["id"], "task.waiting", "waiting for budget headroom")
                            else:
                                self._conn.execute("UPDATE nodes SET status='blocked',error=? WHERE run_id=? AND id=?", ("budget reservation denied", run_id, row["id"]))
                                self._event(run_id, row["id"], "task.blocked", "budget reservation denied")
                            continue
                        number = row["attempt_count"] + 1
                        dispatch_now = time.time()
                        lease_until = min(dispatch_now + node.timeout_seconds, run["deadline_at"])
                        self._conn.execute("UPDATE nodes SET status='running',attempt_count=?,current_attempt_id=? WHERE run_id=? AND id=?", (number, attempt_id, run_id, node.id))
                        self._conn.execute("INSERT INTO attempts(id,run_id,node_id,owner_id,number,status,lease_until,started_at) VALUES(?,?,?,?,?,?,?,?)", (attempt_id, run_id, node.id, self._owner_id, number, "running", lease_until, dispatch_now))
                        route_detail = "" if decision is None else json.dumps(decision.as_dict(), ensure_ascii=False, sort_keys=True)
                        self._event(run_id, node.id, "task.routed", route_detail or "default run rate")
                        self._event(run_id, node.id, "task.started", f"attempt={number};stage={stage_name}")
                        dispatch_info: dict[str, Any] = {
                            "node": node,
                            "attempt_id": attempt_id,
                            "estimate": estimate,
                            "started": dispatch_now,
                            "deadline": lease_until,
                            "stage": stage_name,
                            "route": None if decision is None else decision.as_dict(),
                            "cancel_event": threading.Event(),
                        }

                        def heartbeat_callback(
                            lease_seconds: float = node.timeout_seconds,
                            rid: str = run_id,
                            nid: str = node.id,
                            aid: str = attempt_id,
                            info: dict[str, Any] = dispatch_info,
                            deadline_at: float = run["deadline_at"],
                        ) -> bool:
                            accepted = self.heartbeat(rid, nid, aid, lease_seconds)
                            if accepted:
                                info["deadline"] = min(time.time() + lease_seconds, deadline_at)
                            return accepted

                        context = {
                            "run_id": run_id,
                            "attempt_id": attempt_id,
                            "artifacts": self._artifact_refs(run_id, row, rows),
                            "wave": node.wave,
                            "stage": stage_name,
                            "progress": progress,
                            "route": None if decision is None else decision.as_dict(),
                            "cancel_event": dispatch_info["cancel_event"],
                            "should_stop": dispatch_info["cancel_event"].is_set,
                            "report_progress": lambda value, detail=None, rid=run_id, nid=node.id, aid=attempt_id: self.report_progress(rid, nid, aid, value, detail),
                            "heartbeat": heartbeat_callback,
                            # 只读证据入口：adapter/verifier 可读取已提交 artifact 的正文，
                            # 走 ArtifactStore 的 digest 与路径校验，不触碰调度锁。
                            "read_artifact": artifact_store.read_text,
                        }
                        dispatch_info["context"] = context
                        future = executor.submit(adapter.run, node, context)
                        active[future] = dispatch_info
                        dispatched_by_stage[stage_name] = dispatched_by_stage.get(stage_name, 0) + 1
                        dispatched_value_by_stage[stage_name] = dispatched_value_by_stage.get(stage_name, 0.0) + max(0.0, node.incremental_value)
                        active_by_stage[stage_name] = active_by_stage.get(stage_name, 0) + 1
                        if decision:
                            active_by_route[decision.profile_id] = active_by_route.get(decision.profile_id, 0) + 1
                    # 不要在等待 worker 时持有调度锁：heartbeat/report_progress/cancel
                    # 都需要从其他线程进入 Host。事务先提交，唤醒后重新取得锁。
                    self._conn.commit()
                    self._lock.release()
                    try:
                        done, _ = concurrent.futures.wait(tuple(active), timeout=0.05, return_when=concurrent.futures.FIRST_COMPLETED)
                    finally:
                        self._lock.acquire()
                    for future in done:
                        info = active.get(future)
                        if info is None:
                            continue
                        active.pop(future, None)
                        if info.get("timed_out"):
                            # 超时 attempt 已经失败；这里只回收已经自然结束的旧线程，
                            # 绝不把迟到结果提交为成功。
                            continue
                        if time.time() >= info["deadline"]:
                            info["cancel_event"].set()
                            info["timed_out"] = True
                            self._fail_attempt(run_id, info, "worker lease timeout", events)
                            continue
                        self._finish_future(run_id, run, info, future, adapter, artifact_store, events)
                    if active:
                        now = time.time()
                        for future, info in list(active.items()):
                            if now >= info["deadline"] and not info.get("timed_out"):
                                # Python 线程无法被强制终止：发出协作式取消，但保留
                                # future 占用调度槽位直到自然结束。这样重试不会与旧调用
                                # 重叠，也不会突破 max_workers/route 并发；需要硬超时请用进程。
                                info["cancel_event"].set()
                                info["timed_out"] = True
                                self._fail_attempt(run_id, info, "worker lease timeout", events)
                    rows = self._get_nodes(run_id)
                    unfinished = any(row["status"] not in TERMINAL for row in rows.values())
                    if not unfinished and not active:
                        status = "succeeded" if all(row["status"] in {"succeeded", "deferred"} for row in rows.values()) else "failed"
                        if any(row["status"] == "cancelled" for row in rows.values()):
                            status = "cancelled"
                        self._conn.execute("UPDATE runs SET status=?,reserved_cost=0,owner_id=NULL,owner_lease_until=0 WHERE id=? AND owner_id=?", (status, run_id, self._owner_id))
                        self._event(run_id, None, "run.completed", status)
                        break
                time.sleep(0.001)
        finally:
            # ThreadPoolExecutor 无法杀死不合作线程；run deadline 会先 detach
            # 迟到 future，再用 wait=False 释放调度线程。adapter 应响应
            # context['should_stop']，生产隔离建议使用进程池。
            for pool in executors:
                pool.shutdown(wait=False, cancel_futures=True)
            release_claim()
        return self.result(run_id, events)

    def _fail_attempt(self, run_id: str, info: Mapping[str, Any], error: str, events: list[str]) -> None:
        node: NodeSpec = info["node"]
        attempt_id = info["attempt_id"]
        with self._lock, self._conn:
            run = self._get_run(run_id)
            # 先 CAS 结束 attempt，再结算 reservation；旧 owner/旧 future 无法重复写入。
            now = time.time()
            claimed = self._conn.execute(
                """
                UPDATE attempts SET status='failed',finished_at=?,error=?
                WHERE id=? AND run_id=? AND node_id=? AND status='running'
                  AND EXISTS (SELECT 1 FROM runs WHERE id=? AND owner_id=?)
                """,
                (now, error, attempt_id, run_id, node.id, run_id, self._owner_id),
            ).rowcount
            if not claimed:
                return
            self._release(run_id, node.id, attempt_id, info["estimate"])
            row = self._conn.execute("SELECT * FROM nodes WHERE run_id=? AND id=?", (run_id, node.id)).fetchone()
            current = row and row["status"] == "running" and row["current_attempt_id"] == attempt_id
            if not current:
                # recovery/takeover 已经推进了新 attempt；旧失败只保留 attempt/ledger 审计。
                return
            # 失败 attempt 的 artifact 不能成为重试或下游的证据来源。
            self._conn.execute("DELETE FROM artifacts WHERE run_id=? AND node_id=?", (run_id, node.id))
            if run["cancel_requested"] or now >= run["deadline_at"]:
                self._conn.execute("UPDATE nodes SET status='cancelled',progress=0,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?", (error, run_id, node.id, attempt_id))
                status = "cancelled"
            elif row["attempt_count"] < node.max_attempts:
                delay = min(60.0, 0.25 * (2 ** (row["attempt_count"] - 1)))
                self._conn.execute("UPDATE nodes SET status='pending',progress=0,next_ready_at=?,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?", (now + delay, error, run_id, node.id, attempt_id))
                status = "retrying"
            else:
                self._conn.execute("UPDATE nodes SET status='failed',progress=0,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?", (error, run_id, node.id, attempt_id))
                status = "failed"
            self._event(run_id, node.id, "task.failed", f"{error}; {status}")
            events.append(f"{node.id}: {status}: {error}")

    def _finish_future(self, run_id: str, run: sqlite3.Row, info: Mapping[str, Any], future: concurrent.futures.Future[WorkerResult], adapter: WorkerAdapter, store: ArtifactStore, events: list[str]) -> None:
        """提交 worker 结果；所有写入都先验证 attempt 仍是当前 running。"""
        node: NodeSpec = info["node"]
        attempt_id = info["attempt_id"]
        try:
            result = future.result()
            if not isinstance(result, WorkerResult):
                raise TypeError("worker adapter must return WorkerResult")
            with self._lock, self._conn:
                current_run = self._get_run(run_id)
                attempt = self._conn.execute(
                    "SELECT status FROM attempts WHERE id=? AND run_id=? AND node_id=?",
                    (attempt_id, run_id, node.id),
                ).fetchone()
                current_node = self._conn.execute(
                    "SELECT * FROM nodes WHERE run_id=? AND id=?",
                    (run_id, node.id),
                ).fetchone()
                # stale future、取消和 deadline 都不能把节点重新写成 succeeded。
                if (
                    current_run["owner_id"] != self._owner_id
                    or not attempt
                    or attempt["status"] != "running"
                    or not current_node
                    or current_node["status"] != "running"
                    or current_node["current_attempt_id"] != attempt_id
                ):
                    return
                if current_run["cancel_requested"] or time.time() >= current_run["deadline_at"]:
                    self._fail_attempt(run_id, info, "run cancelled or deadline exceeded", events)
                    return

                refs = self._store_artifacts(run_id, node.id, result.artifacts, store)
                # 当前节点的短名称优先；依赖 artifact 始终可用 dependency/name 与 digest
                # 访问，避免同名证据被依赖节点静默覆盖。
                for key, ref in self._artifact_refs(run_id, current_node, self._get_nodes(run_id)).items():
                    refs.setdefault(key, ref)
                if node.validation_policy == "lenient":
                    report = ValidationReport(
                        isinstance(result.envelope, Mapping),
                        () if isinstance(result.envelope, Mapping) else ("envelope must be an object",),
                        score=1.0 if isinstance(result.envelope, Mapping) else 0.0,
                    )
                else:
                    report = validate_envelope(result.envelope, set(refs.values()) | set(refs.keys()))
                route_rate = None
                if info.get("route"):
                    route_rate = next(
                        (profile["cost_per_1k_tokens"] for profile in json.loads(current_run["route_profiles"] or "[]") if profile["id"] == info["route"]["profile_id"]),
                        None,
                    )
                actual = self._actual_cost(result.envelope, current_run, route_rate)
                remaining_reserved = max(0.0, current_run["reserved_cost"] - info["estimate"])
                if (
                    not math.isfinite(actual)
                    or actual > info["estimate"] + 1e-9
                    or current_run["spent_cost"] + remaining_reserved + actual > current_run["budget_cost"] + 1e-9
                ):
                    report = ValidationReport(False, report.errors + ("hard budget exceeded by worker usage",), score=0.0)

                error = "; ".join(report.errors)
                if not report.ok:
                    # 先 CAS 结束 attempt，再结算 reservation；迟到 future 将直接被丢弃。
                    claimed = self._conn.execute(
                        """
                        UPDATE attempts SET status='failed',finished_at=?,error=?
                        WHERE id=? AND run_id=? AND node_id=? AND status='running'
                          AND EXISTS (
                              SELECT 1 FROM nodes
                              WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?
                          )
                          AND EXISTS (SELECT 1 FROM runs WHERE id=? AND owner_id=?)
                        """,
                        (time.time(), error, attempt_id, run_id, node.id, run_id, node.id, attempt_id, run_id, self._owner_id),
                    ).rowcount
                    if not claimed:
                        # artifact 表没有 attempt_id；当前事务仍持有 owner/节点锁，
                        # 只能安全删除本次尚未提交成功的节点绑定，文件本身交给 GC。
                        self._conn.execute("DELETE FROM artifacts WHERE run_id=? AND node_id=?", (run_id, node.id))
                        return
                    self._release(run_id, node.id, attempt_id, info["estimate"], actual if math.isfinite(actual) and actual >= 0 else 0.0)
                    # 不保留失败 attempt 写入的 artifact 绑定，避免重试继承过期证据。
                    self._conn.execute("DELETE FROM artifacts WHERE run_id=? AND node_id=?", (run_id, node.id))
                    fresh_run = self._get_run(run_id)
                    if fresh_run["cancel_requested"] or time.time() >= fresh_run["deadline_at"]:
                        self._conn.execute("UPDATE nodes SET status='cancelled',progress=0,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?", (error, run_id, node.id, attempt_id))
                        outcome = "cancelled"
                    elif current_node["attempt_count"] < node.max_attempts:
                        delay = min(60.0, 0.25 * (2 ** (current_node["attempt_count"] - 1)))
                        self._conn.execute("UPDATE nodes SET status='pending',progress=0,next_ready_at=?,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?", (time.time() + delay, error, run_id, node.id, attempt_id))
                        outcome = "retrying"
                    else:
                        self._conn.execute("UPDATE nodes SET status='failed',progress=0,error=?,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?", (error, run_id, node.id, attempt_id))
                        outcome = "failed"
                    self._event(run_id, node.id, "validation.failed", error)
                    events.append(f"{node.id}: {outcome}: validation")
                    return

                # 在扣费前再次读取运行状态，缩小 cancel/deadline 竞态窗口。
                fresh_run = self._get_run(run_id)
                if fresh_run["cancel_requested"] or time.time() >= fresh_run["deadline_at"]:
                    self._fail_attempt(run_id, info, "run cancelled or deadline exceeded", events)
                    return
                claim_time = time.time()
                claimed = self._conn.execute(
                    """
                    UPDATE attempts SET status='succeeded',finished_at=?,cost=?
                    WHERE id=? AND run_id=? AND node_id=? AND status='running'
                      AND EXISTS (
                          SELECT 1 FROM runs
                          WHERE id=? AND cancel_requested=0 AND deadline_at>=?
                      )
                      AND EXISTS (
                          SELECT 1 FROM nodes
                          WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?
                      )
                      AND EXISTS (SELECT 1 FROM runs WHERE id=? AND owner_id=?)
                    """,
                    (claim_time, actual, attempt_id, run_id, node.id, run_id, claim_time, run_id, node.id, attempt_id, run_id, self._owner_id),
                ).rowcount
                if not claimed:
                    # deadline/cancel/owner 竞态可能让成功 CAS 失败；不能留下未提交 attempt 的证据绑定。
                    self._conn.execute("DELETE FROM artifacts WHERE run_id=? AND node_id=?", (run_id, node.id))
                    fresh = self._get_run(run_id)
                    if fresh["cancel_requested"] or time.time() >= fresh["deadline_at"]:
                        self._fail_attempt(run_id, info, "run cancelled or deadline exceeded", events)
                    return
                self._release(run_id, node.id, attempt_id, info["estimate"], actual)
                self._conn.execute("UPDATE nodes SET status='succeeded',progress=1,error=NULL,current_attempt_id=NULL WHERE run_id=? AND id=? AND status='running' AND current_attempt_id=?", (run_id, node.id, attempt_id))
                self._event(run_id, node.id, "validation.completed", f"score={report.score:.2f}")
                self._event(run_id, node.id, "task.completed", None)
                events.append(f"{node.id}: succeeded")
        except Exception as exc:  # adapter failures are finite-retry task failures
            self._fail_attempt(run_id, info, str(exc), events)

    def result(self, run_id: str, events: Iterable[str] = ()) -> RunResult:
        with self._lock:
            run = self._get_run(run_id)
            rows = self._get_nodes(run_id)
            persisted_events = tuple(
                f"{row['event']}: {row['detail']}" if row["detail"] else row["event"]
                for row in self._conn.execute(
                    "SELECT event,detail FROM events WHERE run_id=? ORDER BY id", (run_id,)
                )
            )
            return RunResult(
                run_id=run_id,
                status=run["status"],
                succeeded=tuple(sorted(node_id for node_id, row in rows.items() if row["status"] == "succeeded")),
                failed=tuple(sorted(node_id for node_id, row in rows.items() if row["status"] == "failed")),
                blocked=tuple(sorted(node_id for node_id, row in rows.items() if row["status"] in {"blocked", "cancelled"})),
                spent_cost=run["spent_cost"],
                events=tuple(events) + persisted_events,
                deferred=tuple(sorted(node_id for node_id, row in rows.items() if row["status"] == "deferred")),
            )

    def snapshot(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self._conn.execute("SELECT id,status,attempt_count,error,stage,incremental_value,urgency,route_class,progress FROM nodes WHERE run_id=? ORDER BY wave,priority DESC,id", (run_id,))]

    def artifacts(self, run_id: str, node_id: str | None = None) -> list[dict[str, Any]]:
        """只读列出已提交的 artifact 绑定，用于审计与 provenance 构建。

        只返回绑定元数据；正文通过 :meth:`read_artifact` 读取，两条路径都会
        校验 digest，因此审计结果不会因为文件被替换而失真。
        """
        with self._lock:
            if node_id is None:
                rows = self._conn.execute(
                    "SELECT node_id,name,ref,sha256 FROM artifacts WHERE run_id=? ORDER BY node_id,name",
                    (run_id,),
                )
            else:
                rows = self._conn.execute(
                    "SELECT node_id,name,ref,sha256 FROM artifacts WHERE run_id=? AND node_id=? ORDER BY name",
                    (run_id, node_id),
                )
            return [dict(row) for row in rows]

    def read_artifact(self, run_id: str, ref: str) -> str:
        """按引用读取 artifact 正文；digest 不匹配或路径越界都会抛错。"""
        return ArtifactStore(self._artifact_root(run_id)).read_text(ref)

    def events(self, run_id: str, prefix: str | None = None) -> list[dict[str, Any]]:
        """只读审计事件流，可按事件名前缀过滤（例如 ``task.heartbeat``）。"""
        with self._lock:
            if prefix is None:
                rows = self._conn.execute(
                    "SELECT id,node_id,event,detail,created_at FROM events WHERE run_id=? ORDER BY id",
                    (run_id,),
                )
            else:
                rows = self._conn.execute(
                    "SELECT id,node_id,event,detail,created_at FROM events WHERE run_id=? AND event LIKE ? ORDER BY id",
                    (run_id, f"{prefix}%"),
                )
            return [dict(row) for row in rows]

    def budget_snapshot(self, run_id: str) -> dict[str, Any]:
        """只读预算快照：把 ``spent + reserved <= budget`` 这条不变量暴露给外部审计。"""
        with self._lock:
            run = self._get_run(run_id)
            ledger = {
                row["operation"]: row["total"]
                for row in self._conn.execute(
                    "SELECT operation, SUM(amount) AS total FROM budget_ledger WHERE run_id=? GROUP BY operation",
                    (run_id,),
                )
            }
            entries = self._conn.execute(
                "SELECT COUNT(*) FROM budget_ledger WHERE run_id=?", (run_id,)
            ).fetchone()[0]
        spent, reserved, budget = run["spent_cost"], run["reserved_cost"], run["budget_cost"]
        return {
            "budget_cost": budget,
            "spent_cost": spent,
            "reserved_cost": reserved,
            "committed_cost": spent + reserved,
            "invariant_ok": spent + reserved <= budget + 1e-9,
            "ledger": ledger,
            "ledger_entries": entries,
        }
