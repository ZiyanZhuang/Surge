# WorkerAdapter 契约

## 稳定边界

调度器只依赖下面的同步接口：

```python
class WorkerAdapter(Protocol):
    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        ...
```

Adapter 负责一次 worker 调用；调度器负责 DAG、并发、预算、租约、重试、artifact、envelope 校验和节点状态。Adapter 不应直接修改调度器 SQLite。

## 输入

`node` 包含：

- `id`、`prompt`、`depends_on`、`wave`；
- `max_attempts`、`timeout_seconds`、`max_tokens`；
- `stage`、`route_class`、`incremental_value`、`urgency`。

`context` 可能包含：

- 当前 run/node/attempt 标识；
- 依赖节点的 artifact 引用；
- `should_stop()`、`heartbeat()`、`report_progress()`；
- 已选择的 route 信息。

Adapter 必须尊重 `should_stop()`，并把 heartbeat 和 progress 视为协作式信号。

## 输出

成功时返回 `WorkerResult`：

```python
WorkerResult(
    envelope={
        "answer": "...",
        "claims": [{"id": "c-1", "text": "...", "evidence_refs": ["artifact-name"]}],
        "citations": [],
        "confidence": 0.8,
        "warnings": [],
        "usage": {"total_tokens": 100, "cost": 0.001},
    },
    artifacts={"artifact-name": "UTF-8 text"},
)
```

strict 模式下，`answer`、`claims`、`citations`、`confidence`、`warnings`、`usage` 和 `usage.cost` 均需存在；每个 claim 的 `evidence_refs` 必须指向本节点或依赖节点的 artifact。

## 失败策略

- Adapter 抛出异常：当前 attempt 失败；在 `max_attempts` 未耗尽时有限重试，否则节点为 `failed`，下游为 `blocked`；
- 超时：发送协作式停止信号，旧 future 在自然结束前保留调度槽位；
- 非法 envelope 或非法 evidence：按验证失败处理，不进入下游证据集合；
- 预算或输出大小违反硬约束：拒绝提交成功结果，并记录事件/账本；
- 调度语义是 **at-least-once**，Adapter 应使用幂等键，不能假设 exactly-once。

## `max_tokens` 政策

`NodeSpec.max_tokens` 用于调度器的预算 reservation 和 adapter 的请求约束。真实 provider 可能忽略该请求字段，因此本项目不声称它能物理限制上游输出。

真实 adapter 必须至少做到：

1. 向 provider 请求不高于节点的输出上限；
2. 要求 provider 返回结构化 usage；
3. usage 缺失或 `output_tokens > max_tokens` 时 **fail-closed**；
4. 对原始响应设置字节上限；
5. 不把一次成功响应解释为容量或通用模型质量证明。

当前 `HttpWorkerAdapter` 已经是库的一等公民：实现在 `surge_cluster/http_adapter.py`，由 `surge_cluster` 顶层导出，`benchmark_real_adapter_smoke.py` 只是它的薄包装。它遵守上述策略，但仍然是本地 reverse-proxy adapter，不是 Host `llm.stream` Service 的直接 bridge。

## 库内适配器与执行入口

```python
from surge_cluster import DAGScheduler, HttpWorkerAdapter, NodeSpec, RunSpec

adapter = HttpWorkerAdapter(
    "http://127.0.0.1:17800/v1/messages",
    "gpt-6.1-sol",
    timeout=120.0,
    max_output_tokens=2000,
)
```

`HttpWorkerAdapter` 的行为约定：

- 本次请求的 `max_tokens` 取 `min(max_output_tokens, node.max_tokens)`；provider 报告的 `output_tokens` 超过该值时 fail-closed；
- 派发前检查 `should_stop()`，派发前与返回后都不在有取消信号时提交结果；
- 调用前用 `heartbeat()` 把租约延长到 `min(node.timeout_seconds, timeout)`，租约被拒即失败；
- 失败抛出 `AdapterFailure`，带 `kind`（`transport`/`http`/`sse`/`usage`/`output-limit`/`response-size`/`content-type`/`empty-output`/`empty-answer`/`cancelled`）和 `retryable`；调度器当前仍按 `max_attempts` 重试任何异常，尚未按 kind 分流；
- 调用记录 `adapter.calls` 只保存 node id、状态、usage、事件数、输出 sha256 和耗时，不保存 prompt 正文与凭据；
- 响应超过 `max_artifact_bytes` 时保存摘要与摘要哈希，不做静默截断，并在 envelope 的 `warnings` 中标注。

执行入口是 `dsh-surge-run`（`run_dag.py`）：读取 JSON 计划，`--dry-run` 离线校验，真实模式必须显式给出 `--endpoint`。`node.max_tokens` 在计划里表示单次调用的预算预留上限（输入+输出），报告会记录这一口径。
