"""独立验证与 claim provenance。

设计论证
--------
把一次运行看作两组对象：claim 集合 C 与 artifact 集合 A，claim 通过
``evidence_refs`` 连到 A 上，构成二分图 G = (C ∪ A, E)。围绕 G 有三条可检查性质：

* **P1 无悬空引用**：E 的每个端点都存在于已知 artifact 中；
* **P2 内容完整**：每个 artifact 的正文哈希等于其 digest；
* **P3 证据独立**：claim 的证据不能只由产生该 claim 的同一节点提供。

验证函数 ``V`` 是运行后的**偏函数**：``V(claim, 已解析证据) -> {passed, score, issues}``。
它必须满足一条不变量——**非破坏性**：

    verify(x) 只允许对 x 附加标注；envelope 的既有字段与所有已提交 artifact
    保持逐字节不变。

由此还能得到一条组合性质：把 verifier 叠进任意 adapter 只能增加失败，不能把已经
失败的结论洗成通过（``verdict = 结构校验 ∧ (policy=annotate ∨ V.passed)``）。
策略显式二选一——``annotate`` 记录问题但不阻断，``fail`` 让节点失败并阻断下游——
避免"验证失败却继续下游"这种没有书面约定的中间态。

已知边界
--------
调度器持久化 nodes/attempts/artifacts/events/ledger，但**不持久化 envelope**，
因此运行结束后只能重建 node→artifact 层；claim→artifact 层需要调用方提供
（``VerifyingAdapter`` 在内存中保留，或由 Gate 类脚本显式传入）。把 envelope 落库
是后续可以单独评审的 schema 变更，这里不顺手改动热路径。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from .core import AdapterFailure, NodeSpec, WorkerResult

_DIGITS = re.compile(r"\d+(?:\.\d+)?")


def content_digest(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _numbers(text: str) -> set[str]:
    """取出用于比对的数字字面量；去掉末尾零以便 93.5 与 93.50 视为同一数值。"""
    values = set()
    for raw in _DIGITS.findall(text):
        trimmed = raw.rstrip("0").rstrip(".") if "." in raw else raw
        values.add(trimmed or "0")
    return values


@dataclass(frozen=True)
class VerificationIssue:
    code: str
    detail: str
    claim_id: str | None = None
    severity: str = "error"

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "detail": self.detail,
            "claim_id": self.claim_id,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class VerificationResult:
    verifier: str
    passed: bool
    score: float
    checked_claims: int
    issues: tuple[VerificationIssue, ...] = ()
    resolved_evidence: tuple[str, ...] = ()
    note: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "verifier": self.verifier,
            "passed": self.passed,
            "score": round(self.score, 6),
            "checked_claims": self.checked_claims,
            "resolved_evidence": list(self.resolved_evidence),
            "issue_count": len(self.issues),
            "issues": [issue.as_dict() for issue in self.issues],
            "note": self.note,
        }


class EvidenceVerifier:
    """确定性证据验证器：只读 envelope 与已解析证据，不修改任何内容。

    检查项（全部可解释，没有隐藏评分）：

    1. ``missing_evidence``：claim 没有任何 ``evidence_refs``；
    2. ``unresolved_reference``：引用了既不在本节点 artifact、也不在依赖证据里的引用；
    3. ``unreadable_reference``：引用存在但正文不可读（例如隔离子进程内没有
       ``read_artifact``）；默认记为 info，``require_content=True`` 时升级为错误；
    4. ``self_reference``：claim 的全部证据只来自产生它的同一节点，缺少独立来源；
    5. ``answer_absent``：``require_answer_in_evidence=True`` 且 envelope 的答案数字
       在任何可读证据中都不出现。
    """

    name = "evidence-verifier"

    def __init__(
        self,
        *,
        require_content: bool = False,
        require_answer_in_evidence: bool = False,
        require_claims: bool = True,
    ):
        self.require_content = bool(require_content)
        self.require_answer_in_evidence = bool(require_answer_in_evidence)
        self.require_claims = bool(require_claims)

    def _resolve(
        self,
        own_artifacts: Mapping[str, str],
        dependency_refs: Mapping[str, str],
        read_artifact: Callable[[str], str] | None,
    ) -> dict[str, tuple[str | None, str]]:
        """建立 引用键 -> (正文或 None, 来源) 的解析表。"""
        resolved: dict[str, tuple[str | None, str]] = {}
        for name, content in own_artifacts.items():
            resolved[str(name)] = (content, "own")
            resolved[f"artifact:{content_digest(content)}"] = (content, "own")
        for key, ref in dependency_refs.items():
            if not isinstance(ref, str) or key in resolved:
                continue
            content: str | None = None
            if callable(read_artifact):
                try:
                    content = read_artifact(ref)
                except Exception:
                    content = None
            resolved[key] = (content, "dependency")
            resolved.setdefault(ref, (content, "dependency"))
        return resolved

    def verify(
        self,
        node: NodeSpec,
        envelope: Mapping[str, Any],
        *,
        own_artifacts: Mapping[str, str] | None = None,
        dependency_refs: Mapping[str, str] | None = None,
        read_artifact: Callable[[str], str] | None = None,
    ) -> VerificationResult:
        own = {str(k): str(v) for k, v in dict(own_artifacts or {}).items()}
        refs = {str(k): str(v) for k, v in dict(dependency_refs or {}).items()}
        resolved = self._resolve(own, refs, read_artifact)
        claims = envelope.get("claims")
        issues: list[VerificationIssue] = []
        checks = 0
        resolved_keys: set[str] = set()
        readable_text: list[str] = []

        if not isinstance(claims, list):
            issues.append(
                VerificationIssue("missing_claims", "envelope.claims must be a list for verification")
            )
            checks += 1
            claims = []
        for index, claim in enumerate(claims):
            checks += 1
            if not isinstance(claim, Mapping):
                issues.append(
                    VerificationIssue("malformed_claim", "claim must be an object", claim_id=f"#{index}")
                )
                continue
            claim_id = str(claim.get("id") or f"#{index}")
            evidence = claim.get("evidence_refs")
            if not isinstance(evidence, list) or not evidence:
                if self.require_claims:
                    issues.append(
                        VerificationIssue("missing_evidence", "claim has no evidence_refs", claim_id=claim_id)
                    )
                continue
            origins: set[str] = set()
            answered = False
            for ref in evidence:
                key = str(ref)
                entry = resolved.get(key)
                if entry is None:
                    issues.append(
                        VerificationIssue(
                            "unresolved_reference",
                            f"reference is not present in this node or its dependencies: {key}",
                            claim_id=claim_id,
                        )
                    )
                    continue
                content, origin = entry
                origins.add(origin)
                resolved_keys.add(key)
                if content is None:
                    issues.append(
                        VerificationIssue(
                            "unreadable_reference",
                            f"reference content is not readable here: {key}",
                            claim_id=claim_id,
                            severity="error" if self.require_content else "info",
                        )
                    )
                    continue
                answered = True
                readable_text.append(content)
                if f"artifact:{content_digest(content)}" not in resolved_keys:
                    resolved_keys.add(f"artifact:{content_digest(content)}")
            if origins == {"own"}:
                issues.append(
                    VerificationIssue(
                        "self_reference",
                        "claim evidence comes only from the node that produced the claim",
                        claim_id=claim_id,
                    )
                )

        answer = envelope.get("answer")
        if (
            self.require_answer_in_evidence
            and isinstance(answer, str)
            and answer.strip()
            and readable_text
        ):
            checks += 1
            wanted = _numbers(answer)
            seen: set[str] = set()
            for text in readable_text:
                seen |= _numbers(text)
            if wanted and not (wanted & seen):
                issues.append(
                    VerificationIssue(
                        "answer_absent",
                        "envelope answer does not appear in any readable evidence",
                    )
                )

        errors = [issue for issue in issues if issue.severity == "error"]
        score = 1.0 if checks == 0 else max(0.0, 1.0 - len(errors) / max(1, checks))
        return VerificationResult(
            verifier=self.name,
            passed=not errors,
            score=score,
            checked_claims=len(claims),
            issues=tuple(issues),
            resolved_evidence=tuple(sorted(resolved_keys)),
        )


class VerifyingAdapter:
    """在任意 adapter 外层附加验证，默认只标注、不改写。

    ``policy="fail"`` 时验证失败会让该节点失败；两种策略都不修改内层返回的
    envelope 字段与 artifact 正文，只在 ``envelope["verification"]`` 与
    ``warnings`` 上追加信息。
    """

    def __init__(
        self,
        inner: Any,
        *,
        verifier: EvidenceVerifier | None = None,
        policy: str = "annotate",
    ):
        if not callable(getattr(inner, "run", None)):
            raise ValueError("inner adapter must expose run(node, context)")
        if policy not in {"annotate", "fail"}:
            raise ValueError("policy must be 'annotate' or 'fail'")
        self.inner = inner
        self.verifier = verifier or EvidenceVerifier()
        self.policy = policy
        self.records: list[dict[str, Any]] = []

    def verification_report(self) -> dict[str, Any]:
        """汇总本次进程中每个节点的验证结论，供报告与 provenance 使用。"""
        passed = sum(1 for record in self.records if record["verification"]["passed"])
        return {
            "policy": self.policy,
            "verifier": self.verifier.name,
            "nodes_verified": len(self.records),
            "nodes_passed": passed,
            "records": list(self.records),
        }

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        result = self.inner.run(node, context)
        envelope = result.envelope
        if not isinstance(envelope, Mapping):
            return result
        reader = context.get("read_artifact") if isinstance(context, Mapping) else None
        dependency_refs = context.get("artifacts") if isinstance(context, Mapping) else None
        report = self.verifier.verify(
            node,
            envelope,
            own_artifacts=result.artifacts,
            dependency_refs=dependency_refs if isinstance(dependency_refs, Mapping) else {},
            read_artifact=reader if callable(reader) else None,
        )
        annotated = dict(envelope)
        annotated["verification"] = report.as_dict()
        warnings = envelope.get("warnings")
        if isinstance(warnings, list):
            annotated["warnings"] = list(warnings) + [
                f"verification:{issue.code}"
                + (f"@{issue.claim_id}" if issue.claim_id else "")
                for issue in report.issues
            ]
        self.records.append({"node_id": node.id, "verification": report.as_dict()})
        if self.policy == "fail" and not report.passed:
            raise AdapterFailure(
                f"verification failed with {len(report.issues)} issue(s)",
                kind="verification",
                retryable=False,
            )
        return WorkerResult(envelope=annotated, artifacts=result.artifacts)


@dataclass
class ProvenanceReport:
    nodes: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    claims: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, str]] = field(default_factory=list)
    findings: list[dict[str, Any]] = field(default_factory=list)
    claims_available: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_count": len(self.nodes),
            "artifact_count": len(self.artifacts),
            "claim_count": len(self.claims),
            "edge_count": len(self.edges),
            "claims_available": self.claims_available,
            "finding_count": len(self.findings),
            "findings": self.findings,
            "nodes": self.nodes,
            "artifacts": self.artifacts,
            "claims": self.claims,
            "edges": self.edges,
        }


def build_provenance(
    scheduler: Any,
    run_id: str,
    *,
    claims_by_node: Mapping[str, Iterable[Mapping[str, Any]]] | None = None,
) -> ProvenanceReport:
    """从调度器的公开只读接口重建 node/artifact/claim 图，并做完整性检查。

    ``scheduler`` 需要有 ``snapshot``、``artifacts``、``read_artifact`` 三个方法
    （``DAGScheduler`` 已提供）。任何读取失败都会转成 finding，而不是中断审计。
    """
    report = ProvenanceReport()
    nodes = list(scheduler.snapshot(run_id))
    rows = list(scheduler.artifacts(run_id))
    content_by_ref: dict[str, str | None] = {}
    artifact_by_name: dict[tuple[str, str], str] = {}

    for node in nodes:
        report.nodes.append(
            {"id": node.get("id"), "status": node.get("status"), "attempts": node.get("attempt_count")}
        )
    for row in rows:
        node_id, name, ref, sha256 = row.get("node_id"), row.get("name"), row.get("ref"), row.get("sha256")
        artifact_by_name[(str(node_id), str(name))] = str(ref)
        try:
            content = scheduler.read_artifact(run_id, ref)
        except Exception as exc:  # 读取失败也要留下审计痕迹
            content = None
            report.findings.append(
                {
                    "code": "unreadable_artifact",
                    "node_id": node_id,
                    "name": name,
                    "detail": str(exc),
                }
            )
        content_by_ref[str(ref)] = content
        if content is not None:
            digest = content_digest(content)
            if digest != sha256:
                report.findings.append(
                    {
                        "code": "digest_mismatch",
                        "node_id": node_id,
                        "name": name,
                        "detail": f"content digest {digest} != stored {sha256}",
                    }
                )
        report.artifacts.append(
            {"node_id": node_id, "name": name, "ref": ref, "sha256": sha256, "readable": content is not None}
        )
        report.edges.append({"from": f"node:{node_id}", "to": f"artifact:{ref}", "kind": "produced"})

    if claims_by_node:
        report.claims_available = True
        cited: set[str] = set()
        for node_id, claims in claims_by_node.items():
            for claim in claims:
                if not isinstance(claim, Mapping):
                    continue
                claim_id = str(claim.get("id"))
                report.claims.append({"node_id": node_id, "id": claim_id, "text": claim.get("text")})
                evidence = claim.get("evidence_refs")
                if not isinstance(evidence, list) or not evidence:
                    report.findings.append(
                        {"code": "claim_without_evidence", "node_id": node_id, "claim_id": claim_id}
                    )
                    continue
                for ref in evidence:
                    key = str(ref)
                    if key.startswith("artifact:"):
                        target = key if key in content_by_ref else None
                    else:
                        target = _lookup_named(artifact_by_name, node_id, key)
                    if target is None:
                        report.findings.append(
                            {
                                "code": "dangling_reference",
                                "node_id": node_id,
                                "claim_id": claim_id,
                                "detail": key,
                            }
                        )
                    else:
                        cited.add(target)
                    report.edges.append(
                        {"from": f"claim:{node_id}/{claim_id}", "to": f"evidence:{key}", "kind": "cites"}
                    )
        for row in rows:
            if str(row.get("ref")) not in cited:
                report.findings.append(
                    {
                        "code": "uncited_artifact",
                        "node_id": row.get("node_id"),
                        "name": row.get("name"),
                    }
                )
    return report


def _lookup_named(
    artifact_by_name: Mapping[tuple[str, str], str], node_id: str, key: str
) -> str | None:
    """把 ``dep/name`` 或短名称解析为引用；解析不到时返回 None。"""
    if "/" in key:
        owner, _, name = key.partition("/")
        return artifact_by_name.get((owner, name))
    matches = {ref for (owner, name), ref in artifact_by_name.items() if name == key}
    return next(iter(matches)) if len(matches) == 1 else None


def claims_from_records(records: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """从 VerifyingAdapter 明细或 Gate 脚本记录中提取 claims（辅助函数）。"""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        node_id = str(record.get("node_id"))
        claims = record.get("claims")
        if isinstance(claims, list):
            grouped.setdefault(node_id, []).extend(
                [claim for claim in claims if isinstance(claim, Mapping)]
            )
    return grouped


def json_safe(value: Any) -> Any:
    """把报告转成可 JSON 序列化结构；失败时返回可诊断的占位。"""
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as exc:
        return {"error": f"not JSON serializable: {exc}"}
