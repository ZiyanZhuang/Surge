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
import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from .core import AdapterFailure, NodeSpec, WorkerResult

_DIGITS = re.compile(r"\d+(?:\.\d+)?")
_FENCE = re.compile(r"^```[a-zA-Z0-9_-]*\s*|\s*```$")


def content_digest(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def resolve_evidence(
    own_artifacts: Mapping[str, str] | None,
    dependency_refs: Mapping[str, str] | None,
    read_artifact: Callable[[str], str] | None,
) -> dict[str, tuple[str | None, str]]:
    """建立 引用键 -> (正文或 None, 来源) 的解析表。

    两个验证器共用同一解析逻辑：自产 artifact 同时以名称与 digest 引用可达，
    依赖 artifact 通过调度器提供的 ``read_artifact`` 读取；读不到时保留条目但
    正文为 None，由调用方决定严重级别。
    """
    resolved: dict[str, tuple[str | None, str]] = {}
    for name, content in dict(own_artifacts or {}).items():
        resolved[str(name)] = (content, "own")
        resolved[f"artifact:{content_digest(content)}"] = (content, "own")
    for key, ref in dict(dependency_refs or {}).items():
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
    model: str | None = None
    usage: Mapping[str, Any] | None = None

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
            "model": self.model,
            "usage": dict(self.usage) if isinstance(self.usage, Mapping) else None,
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
        return resolve_evidence(own_artifacts, dependency_refs, read_artifact)

    def verify(
        self,
        node: NodeSpec,
        envelope: Mapping[str, Any],
        *,
        own_artifacts: Mapping[str, str] | None = None,
        dependency_refs: Mapping[str, str] | None = None,
        read_artifact: Callable[[str], str] | None = None,
        context: Mapping[str, Any] | None = None,
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


DEFAULT_VERIFIER_SYSTEM_PROMPT = (
    "You are an independent verifier. You never rewrite the work under review and you "
    "have no ground-truth label for the task. You decide only whether the cited evidence "
    "supports each stated claim, and you answer with JSON."
)

VERIFIER_OUTPUT_SCHEMA = {
    "pass": "boolean",
    "score": "number in [0, 1]",
    "issues": "array of strings (may be empty)",
    "revision_prompt": "optional string",
}


def _verdict_shape_issues(payload: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """校验验证器输出结构；任何偏差都返回问题列表，绝不返回 pass=True。"""
    if not isinstance(payload, Mapping):
        return None, ["verdict is not a JSON object"]
    problems: list[str] = []
    passed = payload.get("pass")
    if not isinstance(passed, bool):
        problems.append("verdict.pass must be a boolean")
    score = payload.get("score")
    if (
        isinstance(score, bool)
        or not isinstance(score, (int, float))
        or not math.isfinite(float(score))
        or not 0 <= float(score) <= 1
    ):
        problems.append("verdict.score must be a number between 0 and 1")
    raw_issues = payload.get("issues")
    if not isinstance(raw_issues, list) or any(not isinstance(item, str) for item in raw_issues):
        problems.append("verdict.issues must be a list of strings")
    revision = payload.get("revision_prompt")
    if revision is not None and not isinstance(revision, str):
        problems.append("verdict.revision_prompt must be a string when present")
    if problems:
        return None, problems
    return {
        "pass": bool(passed),
        "score": float(score),
        "issues": [str(item) for item in raw_issues],
        "revision_prompt": revision,
    }, []


def parse_verifier_verdict(text: str) -> tuple[dict[str, Any] | None, list[str]]:
    """解析验证器 JSON；容忍 ``` 围栏与前后说明文字，但不放宽字段类型。"""
    if not isinstance(text, str) or not text.strip():
        return None, ["verifier returned no text"]
    candidates: list[str] = []
    stripped = text.strip()
    candidates.append(_FENCE.sub("", stripped))
    start, end = stripped.find("{"), stripped.rfind("}")
    if start != -1 and end > start:
        candidates.append(stripped[start : end + 1])
    last_problems: list[str] = ["verifier output is not valid JSON"]
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_problems = [f"verifier output is not valid JSON: {exc}"]
            continue
        verdict, problems = _verdict_shape_issues(payload)
        if verdict is not None:
            return verdict, []
        last_problems = problems
    return None, last_problems


def build_verification_prompt(
    node: NodeSpec,
    envelope: Mapping[str, Any],
    evidence: Mapping[str, str],
    *,
    max_task_chars: int = 4_000,
    max_evidence_chars: int = 8_000,
    max_total_evidence_chars: int = 24_000,
) -> str:
    """构造验证提示：只给任务、claim 与证据，不给 gold 标签。

    超长内容以显式标记截断，验证器能看到"这里被截断了"，不会把缺失当成不存在。
    """
    claims = envelope.get("claims") if isinstance(envelope.get("claims"), list) else []
    claim_lines: list[str] = []
    for index, claim in enumerate(claims):
        if not isinstance(claim, Mapping):
            claim_lines.append(f"- #{index}: <malformed claim>")
            continue
        refs = claim.get("evidence_refs")
        refs_text = ", ".join(str(item) for item in refs) if isinstance(refs, list) else "<none>"
        claim_lines.append(f"- {claim.get('id')}: {claim.get('text')} [evidence: {refs_text}]")
    task = node.prompt
    if len(task) > max_task_chars:
        task = task[:max_task_chars] + f"\n[task truncated: {len(node.prompt)} chars total]"
    blocks: list[str] = []
    used = 0
    for key in sorted(evidence):
        content = evidence[key]
        chunk = content[:max_evidence_chars]
        if len(content) > len(chunk):
            chunk += f"\n[evidence truncated: {len(content)} chars total]"
        if used + len(chunk) > max_total_evidence_chars:
            blocks.append(f"--- {key} ---\n[omitted: total evidence budget exhausted]")
            continue
        used += len(chunk)
        blocks.append(f"--- {key} ---\n{chunk}")
    return (
        "Judge whether the cited evidence supports each claim. You have no ground-truth "
        "answer; do not invent one. Do not rewrite the work.\n"
        'Answer with JSON only: {"pass": <bool>, "score": <0..1>, "issues": [<string>, ...], '
        '"revision_prompt": <string, optional>}\n'
        "pass is true only when every claim is supported by the evidence you were given.\n\n"
        f"TASK UNDER REVIEW:\n{task}\n\n"
        f"CLAIMS:\n" + ("\n".join(claim_lines) if claim_lines else "- <no claims>") + "\n\n"
        f"EVIDENCE:\n" + ("\n\n".join(blocks) if blocks else "<no readable evidence>") + "\n"
    )


class ModelVerifier:
    """用另一个模型调用做独立验证，输出 pass/fail/score/issues。

    不变量与确定性验证器相同：只标注，不改写。额外的非对称规则是：
    **验证器输出无法解析时记为未通过**，绝不记为通过——否则一个坏掉的验证器
    会变成"洗白"通道。

    成本：验证调用发生在节点内部，因此它的 token 会被折回 envelope 的 usage，
    预算结算与硬上限随之仍然真实；代价是要使用模型验证器就必须让
    ``node.max_tokens`` 覆盖 worker 加 verifier 两次调用。
    """

    name = "model-verifier"

    def __init__(
        self,
        adapter: Any,
        *,
        timeout_seconds: float | None = None,
        max_tokens: int | None = None,
        system_prompt: str = DEFAULT_VERIFIER_SYSTEM_PROMPT,
        max_evidence_chars: int = 8_000,
        max_total_evidence_chars: int = 24_000,
    ):
        if not callable(getattr(adapter, "run", None)):
            raise ValueError("verifier adapter must expose run(node, context)")
        self.adapter = adapter
        self.timeout_seconds = timeout_seconds
        self.max_tokens = max_tokens
        self.system_prompt = system_prompt
        self.max_evidence_chars = max_evidence_chars
        self.max_total_evidence_chars = max_total_evidence_chars
        self.calls: list[dict[str, Any]] = []

    def _evidence(self, resolved: Mapping[str, tuple[str | None, str]]) -> dict[str, str]:
        """按内容去重，优先选择可读性更好的键名，避免同一证据被重复送审。"""
        chosen: dict[str, tuple[int, str, str]] = {}
        for key, (content, _origin) in resolved.items():
            if not isinstance(content, str):
                continue
            digest = content_digest(content)
            preference = 0 if ("/" in key and not key.startswith("artifact:")) else (1 if not key.startswith("artifact:") else 2)
            current = chosen.get(digest)
            if current is None or preference < current[0]:
                chosen[digest] = (preference, key, content)
        return {key: content for _pref, key, content in chosen.values()}

    def _raw_text_and_usage(self, result: WorkerResult) -> tuple[str, Mapping[str, Any] | None]:
        response = getattr(self.adapter, "last_response", None)
        if isinstance(response, Mapping) and isinstance(response.get("text"), str):
            usage = response.get("usage")
            return response["text"], usage if isinstance(usage, Mapping) else None
        calls = getattr(self.adapter, "calls", None)
        usage = None
        if isinstance(calls, list) and calls and isinstance(calls[-1], Mapping):
            candidate = calls[-1].get("usage")
            if isinstance(candidate, Mapping):
                usage = candidate
        envelope = result.envelope if isinstance(result.envelope, Mapping) else {}
        answer = envelope.get("answer")
        return (answer if isinstance(answer, str) else ""), usage

    def _usage_from(self, usage: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(usage, Mapping):
            return None
        try:
            input_tokens = int(usage.get("input_tokens") or 0)
            output_tokens = int(usage.get("output_tokens") or 0)
        except (TypeError, ValueError):
            return None
        rate = float(getattr(self.adapter, "cost_per_1k_tokens", 0.0) or 0.0)
        total = input_tokens + output_tokens
        return {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total,
            "cost": total / 1000.0 * rate,
        }

    def verify(
        self,
        node: NodeSpec,
        envelope: Mapping[str, Any],
        *,
        own_artifacts: Mapping[str, str] | None = None,
        dependency_refs: Mapping[str, str] | None = None,
        read_artifact: Callable[[str], str] | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> VerificationResult:
        resolved = resolve_evidence(own_artifacts, dependency_refs, read_artifact)
        evidence = self._evidence(resolved)
        claims = envelope.get("claims") if isinstance(envelope.get("claims"), list) else []
        if not evidence:
            return VerificationResult(
                verifier=self.name,
                passed=False,
                score=0.0,
                checked_claims=len(claims),
                issues=(
                    VerificationIssue(
                        "no_readable_evidence",
                        "no readable evidence was available to the verifier; treated as unverified",
                    ),
                ),
                model=getattr(self.adapter, "model", None),
            )
        prompt = build_verification_prompt(
            node,
            envelope,
            evidence,
            max_evidence_chars=self.max_evidence_chars,
            max_total_evidence_chars=self.max_total_evidence_chars,
        )
        verdict_node = NodeSpec(
            id=f"{node.id}/verify",
            prompt=prompt,
            max_attempts=1,
            timeout_seconds=self.timeout_seconds or node.timeout_seconds,
            max_tokens=self.max_tokens or node.max_tokens,
            validation_policy="lenient",
        )
        result = self.adapter.run(verdict_node, dict(context or {}))
        text, raw_usage = self._raw_text_and_usage(result)
        usage = self._usage_from(raw_usage)
        model = getattr(self.adapter, "model", None)
        self.calls.append(
            {
                "node_id": verdict_node.id,
                "evidence_keys": sorted(evidence),
                "usage": usage,
                "verdict_chars": len(text),
            }
        )
        verdict, problems = parse_verifier_verdict(text)
        if verdict is None:
            return VerificationResult(
                verifier=self.name,
                passed=False,
                score=0.0,
                checked_claims=len(claims),
                issues=tuple(
                    VerificationIssue("verifier_output_invalid", item) for item in problems
                ),
                resolved_evidence=tuple(sorted(evidence)),
                note="invalid verifier output counts as not verified",
                model=model,
                usage=usage,
            )
        issues = tuple(
            VerificationIssue("verifier_reported_issue", item) for item in verdict["issues"]
        )
        return VerificationResult(
            verifier=self.name,
            passed=verdict["pass"],
            score=verdict["score"],
            checked_claims=len(claims),
            issues=issues,
            resolved_evidence=tuple(sorted(evidence)),
            note=verdict["revision_prompt"] or "",
            model=model,
            usage=usage,
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
            context=context,
        )
        annotated = dict(envelope)
        annotated["verification"] = report.as_dict()
        # 模型验证器的 token 必须折回本节点的 usage，否则预算结算会少记真实开销。
        if isinstance(report.usage, Mapping) and isinstance(annotated.get("usage"), Mapping):
            base = dict(annotated["usage"])
            extra_tokens = report.usage.get("total_tokens")
            extra_cost = report.usage.get("cost")
            if isinstance(extra_tokens, (int, float)) and not isinstance(extra_tokens, bool):
                current = base.get("total_tokens")
                base["total_tokens"] = (
                    float(current) if isinstance(current, (int, float)) and not isinstance(current, bool) else 0.0
                ) + float(extra_tokens)
            if isinstance(extra_cost, (int, float)) and not isinstance(extra_cost, bool):
                current_cost = base.get("cost")
                base["cost"] = (
                    float(current_cost)
                    if isinstance(current_cost, (int, float)) and not isinstance(current_cost, bool)
                    else 0.0
                ) + float(extra_cost)
            annotated["usage"] = base
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
    envelope_source: str = "arguments"

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_count": len(self.nodes),
            "artifact_count": len(self.artifacts),
            "claim_count": len(self.claims),
            "edge_count": len(self.edges),
            "claims_available": self.claims_available,
            "envelope_source": self.envelope_source,
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
    elif claims_by_node is None:
        # 运行结束后重建 claim 层：依赖调度器持久化的 envelope，而不是调用方传参。
        stored_envelopes = getattr(scheduler, "envelopes", None)
        if callable(stored_envelopes):
            try:
                stored = stored_envelopes(run_id)
            except Exception as exc:
                stored = {}
                report.findings.append({"code": "envelope_read_failed", "detail": str(exc)})
            recovered = {
                node_id: list((entry.get("envelope") or {}).get("claims") or [])
                for node_id, entry in stored.items()
                if isinstance(entry.get("envelope"), Mapping)
                and isinstance((entry.get("envelope") or {}).get("claims"), list)
            }
            if recovered:
                report.envelope_source = "persisted"
                claims_by_node = recovered
                report.claims_available = True
            else:
                report.findings.append(
                    {
                        "code": "no_persisted_envelopes",
                        "detail": "claim-level provenance unavailable: no stored envelope for this run",
                    }
                )
    if claims_by_node:
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
