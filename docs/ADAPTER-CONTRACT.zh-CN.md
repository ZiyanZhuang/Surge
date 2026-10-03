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

当前 `benchmark_real_adapter_smoke.py` 的 `HttpWorkerAdapter` 遵守上述策略，但它仍然是本地 reverse-proxy adapter，不是 Host `llm.stream` Service 的直接 bridge。
