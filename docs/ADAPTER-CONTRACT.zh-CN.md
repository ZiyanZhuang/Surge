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

`execute()` 的 `RunResult.events` 统一来自按事件 ID 排序的 SQLite 记录；首次完成与终态重复读取相同，不混入仅存在于当前循环的文本提示。

- Adapter 抛出异常：当前 attempt 失败；在 `max_attempts` 未耗尽时有限重试，否则节点为 `failed`，下游为 `blocked`；
- 若异常为 `AdapterFailure(result=...)`，其中已完成的结果仍接受相同 envelope/预算校验并结算已报告用量，但绝不会提交为成功；失败 envelope 保留用于审计，artifact 绑定不进入下游。隔离层透传这份失败结果。
- 已返回结果在 artifact 写入、输出校验或隔离序列化阶段失败时，同样结算可确认的费用。无法序列化的隔离输出只携带明确标记的失败投影与 usage，不能因此变成成功；尚未收到的费用不猜测。
- HTTP 响应已完整且 usage 有效后，即使空输出/答案、输出超限、提取器错误、UTF-8 处理错误或返回后取消，仍通过失败结果结算该适配器单价下的已知费用；不提交 artifact。内部响应处理异常使用 `kind="response"`。无有效 usage 的传输/协议失败不虚构费用。
- envelope 超过 JSON 递归/整数表示限制或包含不能编码的 UTF-8 时，失败记录为 `persisted="unavailable"`；连 repr 也无法表示时 `repr_sha256` 为 null，不能为生成错误摘要再次中断调度。
- timeout/cancel 结算时若 worker 已返回，保留该结果的已知费用与失败审计；若结算后才返回，其迟到结果仍按现有 CAS 规则丢弃，不推测未知账单。
- `cancel()` 对有效租约的 owner 设置取消请求，由执行循环发出停止信号并结算；无有效 owner 时直接结束 running attempt、释放预留。其他连接发起取消也遵循相同规则，避免抢先清空 owner 已收到的费用。
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
- 调用前用 `heartbeat()` 把租约延长到 `min(node.timeout_seconds, timeout)`，保留亚秒上限，不强制抬到 1 秒；租约被拒即失败；
- 实际 HTTP socket timeout 使用相同较小值，直接调用 adapter 与通过 CLI 调用都遵守节点上限；urllib socket timeout 仍不等于强制墙钟终止，需要硬隔离时使用 IsolatedAdapter；
- 失败抛出 `AdapterFailure`，带 `kind`（`transport`/`http`/`sse`/`usage`/`output-limit`/`response-size`/`content-type`/`empty-output`/`empty-answer`/`cancelled`）和 `retryable`；调度器当前仍按 `max_attempts` 重试任何异常，尚未按 kind 分流；
- 调用记录 `adapter.calls` 只保存 node id、状态、usage、事件数、输出 sha256 和耗时，不保存 prompt 正文与凭据；
- 响应超过 `max_artifact_bytes` 时保存摘要与摘要哈希，不做静默截断，并在 envelope 的 `warnings` 中标注。
- SSE 必须是合法 UTF-8/JSON 对象帧，text delta 必须是字符串；显式 `error`、重复消息起止与结束后的消息帧均失败，不能靠保留事件名绕过 fail-closed。
- SSE 支持标准 LF/CRLF/CR、开头 UTF-8 BOM 与多行 data；只有空行终止的帧生效。usage 合并 `message_start.message.usage.input_tokens` 与 `message_delta.usage`（累计值覆盖而不相加），最终仍必须有完整、合法的输入/输出计数。参见 [SSE 标准](https://html.spec.whatwg.org/multipage/server-sent-events.html) 与 [Messages 流协议](https://platform.claude.com/docs/en/build-with-claude/streaming)。
- 每次已报告的输入/输出计数都必须是非负累计整数，不能回退；非法起始 usage 或晚到的 null/list usage 不能被其他有效帧覆盖或忽略。
- SSE 分行仅识别协议 CR/LF；JSON 字符串内合法的 NEL、Unicode 行/段分隔符仍是正文，不能被 Python 通用 splitlines 拆坏。

执行入口是 `dsh-surge-run`（`run_dag.py`）：读取 JSON 计划，`--dry-run` 离线校验，真实模式必须显式给出 `--endpoint`。`node.max_tokens` 在计划里表示单次调用的预算预留上限（输入+输出），报告会记录这一口径。

通用 CLI 沿用 HttpWorkerAdapter 默认答案提取：优先 JSON answer，否则保留非空纯文本；不会把没有数字的研究回答当成空答案。FinQA 等数值任务可在专用 harness 显式选择数值提取器。

计划结构错误与调度器提交时发现的语义错误（包括依赖环、反向波次依赖）均返回退出码 `2` 和 `error_kind="plan"` 的 JSON 报告，不派发 worker。

读取失败、非法 UTF-8/JSON、错误字段类型与不可表示数值也属于计划错误。schema_version 必须是整数版本号；持久化整数字段限于 SQLite 有符号 64 位范围，超限在提交前拒绝，Python API 与 CLI 都不会留下半个 run。

CLI 覆盖参数的错误属于 usage（退出码 2）：`--timeout` 必须有限且大于零，`--cost-per-1k-tokens` 必须有限且非负；dry-run 与真实模式执行相同检查。

报告路径不能等于输入计划或其文件别名，也不能覆盖持久化 workdir 的 run.sqlite3、SQLite 日志或 run-artifacts 子树；冲突在执行前返回 usage/2，只向 stdout 写错误，不覆盖受保护文件。workdir/report.json 这类相邻报告仍受支持。

报告在同目录临时文件中写入、flush/fsync 后原子替换；写入失败保留旧报告并清理临时文件，替换文件名不会改写其他硬链接的正文。输出失败返回 2，并在 stdout 的原运行报告上附 output 错误，保留本次结果、预算与调用证据。

所有有限数值边界共用相同检查：布尔值、NaN、Infinity 与无法转换为有限 float 的超大整数均拒绝。envelope 返回验证错误，构造参数与 heartbeat/progress/recovery 参数返回 ValueError，不泄漏 float 转换的 OverflowError。

## 组合顺序：验证在外，隔离在内

两个可选装饰器保持同一 `WorkerAdapter` 协议，因此可以和任何适配器组合：

```python
adapter = VerifyingAdapter(               # 3. 父进程内独立验证，只标注
    IsolatedAdapter(model_adapter,        # 2. 子进程硬超时 + 物理并发上界
                   max_processes=2),
    verifier=EvidenceVerifier(require_content=True),
    policy="annotate",                    # 或 "fail"：验证失败即让节点失败
)
```

顺序有明确理由：验证要读取已提交 artifact 的正文，而回调无法跨进程，所以验证留在父进程；隔离层只承诺"一次调用被硬超时与物理并发约束"。

* `IsolatedAdapter`：子进程执行，`max_processes` 限制同时存活的子进程数，硬超时 = `node.timeout_seconds + hard_timeout_grace`，超时或取消时 terminate→join→kill。跨进程只转发可序列化上下文（run/attempt/wave/stage/progress/route/artifacts），`read_artifact`、`heartbeat` 等回调由父进程代理：子进程通过单向控制队列上报 heartbeat/progress，授权仍由调度器 CAS 决定，因此迟到的子进程结果不会被提交。内层 adapter 的 `calls` 明细会随结果带回父进程。
* `VerifyingAdapter`：`policy="annotate"` 只把 `verification` 与 `warnings` 附加到 envelope；`policy="fail"` 在验证失败时抛出 `AdapterFailure(kind="verification")`，让节点失败并阻断下游。两种策略都不修改原 envelope 字段与 artifact 正文。

`EvidenceVerifier(require_answer_in_evidence=True)` 要求答案中全部数值出现在可读的已引用证据中；比较保留正负号，并把等值十进制/科学计数法视为相同值。这只是数值存在性检查，不推断单位、因果或科学正确性。

隔离配置 `hard_timeout_grace` 必须是有限非负数，`kill_grace` 必须是有限正数，均拒绝布尔值、NaN 与 Infinity，避免硬超时失效或回收等待异常。

初始化失败以 `AdapterFailure(kind="spawn")` 返回；started 只统计成功的 `process.start()`，该调用失败不计数，未启动的 process 不执行 join。父子管道与控制队列进入资源清理，即使 reap 失败仍尝试关闭 IPC。

IPC 分配阶段同样受资源清理保护：Queue、Event、Process 或线程初始化失败时，已分配的管道/队列立即关闭；清理按逆序逐项运行，某项清理异常不能跳过剩余资源。

terminate/kill 抛出 OS 错误或有界等待后子进程仍存活时，隔离实例以 `kind="reap"` 失败并拒绝后续 worker；不能把逻辑槽位释放误当成物理进程已退出。调用者处理残留进程后再建立新实例。

隔离边界同样要求返回 `WorkerResult`，且 `artifacts` 为字符串名称到字符串正文的 mapping；不会把非法 artifact 强转文本或把空列表当成空 mapping。

`ModelVerifier` 只读取本次返回结果：原始 verdict 从该结果的 `model-response` artifact（若含完整 text）或 envelope.answer 获取；token/cost 来自该 envelope 的合法 usage，不读取共享 `last_response`/`calls[-1]`，也不重新按 adapter 单价估算已报告成本。缺失或非法 usage 失败。

verdict JSON 超过解析深度/整数限制，或 score 超出 [0,1] 时，形成 `verifier_output_invalid` 的未通过结论并保留验证调用 usage，不能让解析/浮点转换异常抹掉验证费用。

`VerifyingAdapter` 判定失败时通过上述失败结果携带 worker + verifier 的已知 usage；verifier 调用异常时仍保留已完成 worker 的 usage，不能因验证失败把已知支出记为零。未报告的 provider 用量不会被猜测。

验证器抛出普通异常也属于验证执行失败：转为 `kind="verification"`，保留原异常链与已完成 worker 的 usage；annotate 策略允许内容判定未通过，不能把验证器本身崩溃当成成功调用。

模型验证调用前先检查 worker 的 total_tokens/cost；缺失、负数、布尔、非数字或非有限值不得被验证费用相加“洗白”，也不得先追加一次有成本的验证调用。合并验证费用时同样校验两侧与合计值，不用零替换非法用量。

provenance 名称解析与调度器保持当前节点名称优先；依赖限定名称按完整 `node_id/name` 匹配，不以第一个 `/` 拆分，因此 node ID 或 artifact 名称含 `/` 时仍可重建引用。歧义引用仍报告问题。

`artifact:<64 位小写十六进制 SHA-256>` 是保留的内容引用，不作为名称别名解析；同形 artifact 名称不能冒充另一份正文。限定名产生歧义时只保留明确名称与 digest，不静默选最后一个绑定。按 digest 引用的正文若也存在于独立依赖中，验证器使用该依赖来源；当前节点普通名称仍保留 own 优先级。

已解析引用的 `cites.to` 与 `produced.to` 使用同一个 artifact 顶点标识；`cites.ref` 保留原始 evidence_ref 名称或 digest。未解析引用保留占位边与 finding，不会被伪装成已连通的证据。

## 只读审计面

调度器提供五个只读方法，供验证器、Gate 脚本和外部审计使用；它们不改变任何状态，也不参与调度决策：

| 方法 | 用途 |
|---|---|
| `snapshot(run_id)` | 逐节点状态、attempt 次数、错误、阶段与进度 |
| `artifacts(run_id, node_id=None)` | 已提交的 artifact 绑定（node/name/ref/sha256） |
| `read_artifact(run_id, ref)` | 读取并校验 artifact 正文（digest 与路径越界都会抛错） |
| `events(run_id, prefix=None)` | 事件流，可按 `task.heartbeat` 等前缀过滤 |
| `budget_snapshot(run_id)` | `spent + reserved <= budget` 不变量的对外快照与账本汇总 |

adapter 在运行期还可以使用 `context["read_artifact"](ref)` 读取依赖证据；该回调只在父进程内可用。
