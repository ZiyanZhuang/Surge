"""Anthropic-compatible Messages SSE worker adapter.

把一次经授权的 Messages SSE 调用映射为调度器的 ``WorkerAdapter`` 契约。
适配器负责传输、事件序列、usage、输出上限和 artifact 体积的 fail-closed
校验；调度器继续负责 DAG、并发、预算、租约、重试与状态持久化。

边界声明：本适配器面向显式授权的本地 reverse proxy 或与之兼容的
Messages endpoint，它不是 DSH Host ``llm.stream`` business Service 的直接
bridge，也不构成 provider 容量或模型质量证明。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Mapping

from .core import AdapterFailure, NodeSpec, WorkerResult

DEFAULT_MAX_OUTPUT_TOKENS = 2000
DEFAULT_MAX_RESPONSE_BYTES = 4_000_000
DEFAULT_COST_PER_1K_TOKENS = 0.01
DEFAULT_SYSTEM_PROMPT = (
    "You are a careful research worker. Use only the material given in the "
    "task and obey the requested output contract."
)
REQUIRED_SSE_EVENTS = ("message_start", "content_block_delta", "message_delta", "message_stop")
# artifact 上限低于调度器 ArtifactStore 的 2 MB 默认值，为 JSON 包装留出余量。
DEFAULT_MAX_ARTIFACT_BYTES = 1_900_000
ARTIFACT_NAME = "model-response"
_NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?%?")


def _positive_number(value: Any, name: str, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ValueError(f"{name} must be a finite number")
    if allow_zero:
        if value < 0:
            raise ValueError(f"{name} must not be negative")
    elif value <= 0:
        raise ValueError(f"{name} must be positive")
    return float(value)


def extract_json_answer(text: str) -> str | None:
    """从模型输出中取出 JSON ``answer`` 字段；无法解析时返回 None。"""
    try:
        payload = json.loads(text.strip())
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    answer = payload.get("answer")
    if isinstance(answer, str):
        return answer.strip() or None
    if answer is None or isinstance(answer, (dict, list, bool)):
        return None
    return str(answer).strip() or None


def extract_numeric_answer(text: str) -> str | None:
    """先取 JSON ``answer`` 字段，再退回文本中最后一个数字。"""
    answer = extract_json_answer(text)
    if answer:
        return answer
    matches = _NUMBER_RE.findall(text)
    return matches[-1] if matches else None


def default_answer_extractor(text: str) -> str | None:
    """通用默认策略：JSON ``answer`` 字段，否则使用去掉首尾空白的原文。"""
    answer = extract_json_answer(text)
    if answer:
        return answer
    return text.strip() or None


def parse_sse(payload: bytes) -> tuple[str, dict[str, Any] | None, list[str]]:
    """解析 SSE 负载，返回 (文本, usage, 事件名序列)。

    解析过程只收集 ``text_delta`` 和 ``message_delta.usage``；无法解析的帧被
    忽略，随后由 :func:`require_usage` 和必需事件检查决定是否 fail-closed。
    """
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


def require_usage(usage: Mapping[str, Any] | None, max_output_tokens: int) -> dict[str, int]:
    """校验 provider usage 存在、为整数字段，且未超过请求的输出上限。"""
    if not isinstance(usage, Mapping):
        raise AdapterFailure(
            "adapter SSE did not contain message_delta.usage", kind="usage", retryable=False
        )
    result: dict[str, int] = {}
    for key in ("input_tokens", "output_tokens"):
        value = usage.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise AdapterFailure(
                f"adapter usage.{key} must be a non-negative integer", kind="usage", retryable=False
            )
        result[key] = value
    if result["output_tokens"] > max_output_tokens:
        raise AdapterFailure(
            f"adapter reported output_tokens={result['output_tokens']} above requested limit "
            f"{max_output_tokens}; failing closed",
            kind="output-limit",
            retryable=False,
        )
    return result


def call_messages_endpoint(
    url: str,
    model: str,
    prompt: str,
    timeout: float,
    *,
    system_prompt: str = DEFAULT_SYSTEM_PROMPT,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    api_key: str = "local",
    extra_headers: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """向 Messages endpoint 发起一次非流式消费的流式请求，并做全量 fail-closed 校验。

    ``api_key`` 默认值 ``local`` 是本地中继的占位标记，不是凭据；真实凭据应由
    中继进程或环境变量持有，不要写入任务计划或命令行历史。
    """
    _positive_number(timeout, "timeout")
    _positive_number(max_output_tokens, "max_output_tokens")
    _positive_number(max_response_bytes, "max_response_bytes")
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        raise ValueError("url must be an http(s) endpoint")
    if not isinstance(model, str) or not model:
        raise ValueError("model must be a non-empty string")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("prompt must be a non-empty string")
    headers = {
        "content-type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    if extra_headers:
        headers.update({str(key): str(value) for key, value in extra_headers.items()})
    body = {
        "model": model,
        "max_tokens": int(max_output_tokens),
        "system": system_prompt,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "stream": True,
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read(int(max_response_bytes) + 1)
            status = response.status
            content_type = response.headers.get("content-type")
    except urllib.error.HTTPError as exc:
        detail = exc.read(2048).decode("utf-8", errors="replace")
        retryable = exc.code == 408 or exc.code == 429 or exc.code >= 500
        raise AdapterFailure(
            f"adapter HTTP {exc.code}: {detail}", kind="http", retryable=retryable
        ) from exc
    except urllib.error.URLError as exc:
        raise AdapterFailure(
            f"adapter transport failure: {exc.reason}", kind="transport", retryable=True
        ) from exc
    except TimeoutError as exc:
        raise AdapterFailure(
            f"adapter transport failure: {exc}", kind="transport", retryable=True
        ) from exc
    if len(payload) > max_response_bytes:
        raise AdapterFailure(
            f"adapter response exceeds {int(max_response_bytes)} bytes",
            kind="response-size",
            retryable=False,
        )
    if status < 200 or status >= 300:
        raise AdapterFailure(f"adapter HTTP {status}", kind="http", retryable=status >= 500)
    if not content_type or "text/event-stream" not in content_type.lower():
        raise AdapterFailure(
            f"adapter content-type is not SSE: {content_type!r}", kind="content-type", retryable=False
        )
    text, usage, events = parse_sse(payload)
    missing = [event for event in REQUIRED_SSE_EVENTS if event not in events]
    if missing:
        raise AdapterFailure(
            f"adapter SSE missing events: {', '.join(missing)}", kind="sse", retryable=False
        )
    positions = [events.index(event) for event in REQUIRED_SSE_EVENTS]
    if positions != sorted(positions):
        raise AdapterFailure(
            "adapter SSE event order is incomplete or invalid", kind="sse", retryable=False
        )
    normalized_usage = require_usage(usage, int(max_output_tokens))
    if not text.strip():
        raise AdapterFailure(
            "adapter SSE contained no text output", kind="empty-output", retryable=False
        )
    return {
        "http_status": status,
        "content_type": content_type,
        "events": events,
        "text": text,
        "usage": normalized_usage,
        "request_max_tokens": int(max_output_tokens),
        "output_budget_policy": "fail-closed if provider usage is missing or above request",
    }


def _is_cancelled(context: Mapping[str, Any] | None) -> bool:
    if not isinstance(context, Mapping):
        return False
    should_stop = context.get("should_stop")
    return bool(callable(should_stop) and should_stop())


class HttpWorkerAdapter:
    """把一次授权的 Messages SSE 响应翻译为调度器契约的一等适配器。

    已实现的 fail-closed 条件：HTTP/传输错误、非 2xx、content-type 非 SSE、
    缺失或乱序的必需事件、usage 缺失或字段非法、provider 报告的输出超过本次
    请求上限、响应字节超限、空输出、缺少答案、取消后不提交结果。
    """

    def __init__(
        self,
        url: str,
        model: str,
        *,
        timeout: float = 120.0,
        system_prompt: str = DEFAULT_SYSTEM_PROMPT,
        max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
        cost_per_1k_tokens: float = DEFAULT_COST_PER_1K_TOKENS,
        answer_extractor: Callable[[str], str | None] | None = None,
        api_key: str = "local",
        extra_headers: Mapping[str, str] | None = None,
        warning: str = "single model call; not a capacity or quality claim",
    ):
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            raise ValueError("url must be an http(s) endpoint")
        if not isinstance(model, str) or not model:
            raise ValueError("model must be a non-empty string")
        self.url = url
        self.model = model
        self.timeout = _positive_number(timeout, "timeout")
        self.system_prompt = system_prompt
        self.max_output_tokens = int(_positive_number(max_output_tokens, "max_output_tokens"))
        self.max_response_bytes = int(_positive_number(max_response_bytes, "max_response_bytes"))
        self.max_artifact_bytes = int(_positive_number(max_artifact_bytes, "max_artifact_bytes"))
        self.cost_per_1k_tokens = _positive_number(
            cost_per_1k_tokens, "cost_per_1k_tokens", allow_zero=True
        )
        self.answer_extractor = answer_extractor or default_answer_extractor
        self.api_key = api_key
        self.extra_headers = dict(extra_headers or {})
        self.warning = warning
        # 调用记录只保存可公开的元数据，不保存 prompt 或凭据。
        self.calls: list[dict[str, Any]] = []
        self.last_response: dict[str, Any] | None = None
        self.last_error: str | None = None

    def _record(self, node: NodeSpec, entry: dict[str, Any]) -> None:
        entry["node_id"] = node.id
        self.calls.append(entry)

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        if _is_cancelled(context):
            raise AdapterFailure("adapter cancelled before dispatch", kind="cancelled", retryable=False)
        heartbeat = context.get("heartbeat") if isinstance(context, Mapping) else None
        if callable(heartbeat):
            # 租约至少覆盖本次请求的墙钟时间，避免请求进行中被判为过期。
            lease = max(1.0, min(float(node.timeout_seconds), self.timeout))
            if heartbeat(lease) is False:
                raise AdapterFailure(
                    "lease heartbeat rejected; attempt is no longer current",
                    kind="cancelled",
                    retryable=False,
                )
        # 请求上限取策略上限与节点上限的较小值，遵守 ADAPTER-CONTRACT 的 max_tokens 政策。
        request_max_tokens = max(1, min(self.max_output_tokens, int(node.max_tokens)))
        started = time.perf_counter()
        try:
            response = call_messages_endpoint(
                self.url,
                self.model,
                node.prompt,
                self.timeout,
                system_prompt=self.system_prompt,
                max_output_tokens=request_max_tokens,
                max_response_bytes=self.max_response_bytes,
                api_key=self.api_key,
                extra_headers=self.extra_headers,
            )
        except AdapterFailure as exc:
            self.last_error = str(exc)
            self._record(
                node,
                {
                    "ok": False,
                    "kind": exc.kind,
                    "retryable": exc.retryable,
                    "error": str(exc),
                    "elapsed_seconds": round(time.perf_counter() - started, 6),
                },
            )
            raise
        self.last_response = response
        self.last_error = None
        answer = self.answer_extractor(response["text"])
        if not answer:
            self.last_error = "adapter output did not contain a usable answer"
            self._record(
                node,
                {
                    "ok": False,
                    "kind": "empty-answer",
                    "retryable": False,
                    "error": self.last_error,
                    "elapsed_seconds": round(time.perf_counter() - started, 6),
                },
            )
            raise AdapterFailure(self.last_error, kind="empty-answer", retryable=False)
        if _is_cancelled(context):
            # 取消后不提交结果；调度器本身也会拒绝迟到的 commit。
            self.last_error = "run cancelled while the adapter was in flight"
            self._record(
                node,
                {"ok": False, "kind": "cancelled", "retryable": False, "error": self.last_error},
            )
            raise AdapterFailure(self.last_error, kind="cancelled", retryable=False)

        usage = response["usage"]
        total_tokens = usage["input_tokens"] + usage["output_tokens"]
        warnings = [self.warning]
        artifact_text = json.dumps(response, ensure_ascii=False, indent=2)
        if len(artifact_text.encode("utf-8")) > self.max_artifact_bytes:
            # 不做静默截断：保存摘要与摘要哈希，完整文本由调用方从 endpoint 侧留存。
            warnings.append("model response exceeded artifact limit; stored digest summary only")
            summary = {
                "artifact_truncated": True,
                "http_status": response["http_status"],
                "content_type": response["content_type"],
                "events": response["events"],
                "usage": usage,
                "request_max_tokens": response["request_max_tokens"],
                "output_budget_policy": response["output_budget_policy"],
                "output_sha256": hashlib.sha256(response["text"].encode("utf-8")).hexdigest(),
                "output_chars": len(response["text"]),
                "raw_response_sha256": hashlib.sha256(artifact_text.encode("utf-8")).hexdigest(),
                "note": "完整响应超过 artifact 上限；这里保存摘要与哈希，未静默保存截断文本。",
            }
            artifact_text = json.dumps(summary, ensure_ascii=False, indent=2)
        self._record(
            node,
            {
                "ok": True,
                "kind": "ok",
                "usage": usage,
                "http_status": response["http_status"],
                "events": len(response["events"]),
                "request_max_tokens": response["request_max_tokens"],
                "output_sha256": hashlib.sha256(response["text"].encode("utf-8")).hexdigest(),
                "elapsed_seconds": round(time.perf_counter() - started, 6),
            },
        )
        return WorkerResult(
            envelope={
                "answer": answer,
                "claims": [
                    {
                        "id": "model-answer",
                        "text": answer,
                        "evidence_refs": [ARTIFACT_NAME],
                    }
                ],
                "citations": [],
                "confidence": 0.5,
                "warnings": warnings,
                "usage": {
                    "total_tokens": total_tokens,
                    "cost": total_tokens / 1000.0 * self.cost_per_1k_tokens,
                },
            },
            artifacts={ARTIFACT_NAME: artifact_text},
        )
