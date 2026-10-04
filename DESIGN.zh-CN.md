# 浪潮模式：单机科研 Agent DAG 调度设计

## 1. 目标与边界

“浪潮模式”是 DSH 内的模型可见协调模式，加上 Host 侧可运行的单机 DAG 调度 MVP。目标是把科研任务拆成可审计、可恢复、可限额的离散波次，而不是启动无界 swarm。

当前实现是单机 Host 方案：SQLite 保存状态，线程池提供有界并发，artifact 使用内容寻址。它不是跨机器调度器，也不自动创建 GPU 进程、Kubernetes Pod 或远程 Agent。

## 2. 分层架构

```text
DSH Client UI
  └─ 选择 agent preset：浪潮模式
       └─ DSH preset（persona + tools + subagent/workflow）
            └─ 执行入口：dsh-surge-run（JSON 计划 → DAGScheduler）
                 └─ WorkerAdapter
                      ├─ VerifyingAdapter（独立验证：只标注，不改写）
                      ├─ IsolatedAdapter（子进程硬超时 + 物理并发上界）
                      ├─ HttpWorkerAdapter（Anthropic-compatible Messages SSE，fail-closed）
                      ├─ LocalEchoAdapter（离线）
                      └─ 待接入：DSH Host Agent/LLM 直接 bridge
                 └─ DAGScheduler
                      ├─ SQLite：runs/nodes/attempts/events/ledger
                      ├─ bounded ThreadPoolExecutor
                      ├─ wave gate：Scout → Deepen → Verify → Synthesize
                      ├─ demand-aware stage caps / incremental dispatch
                      ├─ task-demand route selection（模型/强度/预算/容量）
                      ├─ retry/timeout/lease heartbeat/recovery
                      ├─ budget reservation + settlement
                      ├─ ArtifactStore：SHA-256 + 原子提交
                      └─ 只读审计面：snapshot / artifacts / read_artifact / events / budget_snapshot
```

Preset 只负责模式组合、提示词和调用约束；`dsh-surge-run` 负责把 JSON 计划交给调度器；调度器负责并发、状态、预算和证据完整性。它们通过 `WorkerAdapter.run(node, context) -> WorkerResult` 解耦。缺少 `dsh-surge-run` 时，preset 中的波次与闸门只是提示词约定，不会有代码检查。

验证与隔离的组合顺序固定为：验证在父进程内，隔离只包住模型调用。验证需要读取已提交 artifact 的正文，而回调无法跨进程传递；隔离层只承诺"一次调用被硬超时与物理并发约束"。上下文中的 `read_artifact` 因此不进入子进程。

## 3. 任务协议

### Node

- `id`、`prompt`
- `depends_on`：DAG 依赖
- `wave`、`priority`
- `max_attempts`、`timeout_seconds`
- `max_tokens`、`validation_policy`（`strict` 或 `lenient`；lenient 仍要求 envelope 为对象）
- `stage`：逻辑执行阶段；未填写时由 `wave` 映射为 `wave-N`
- `incremental_value`、`urgency`：当前任务对增量生产力和时效的需求
- `route_class`：任务类别，用于模型/Provider 硬过滤

### WorkerResult

```json
{
  "envelope": {
    "answer": "...",
    "claims": [{"id": "c-1", "text": "...", "evidence_refs": ["artifact:..."]}],
    "citations": [],
    "confidence": 0.8,
    "warnings": [],
    "usage": {"total_tokens": 100, "cost": 0.001}
  },
  "artifacts": {"notes": "UTF-8 text"}
}
```

每个 claim 必须引用已存在的 artifact。strict envelope 要求 answer、claims、citations、confidence、warnings、usage 及 usage.cost，且 claims/citations/warnings 的元素类型正确；允许额外 JSON 字段以便扩展。默认 2 MB 限制分别作用于单个 envelope 和单个 artifact；当前没有单个 WorkerResult 的 artifact 数量/总大小配额，生产 adapter 应自行限制总输出。依赖证据使用 `dependency_id/name` 或 digest；短名称仅在无歧义时提供，当前节点与依赖同名时当前节点短名称优先。验证失败会有限重试；耗尽后节点失败，下游 blocked。

### 需求感知排布示例

```python
from surge_cluster import NodeSpec, RouteProfile, RunSpec, StagePolicy

run = RunSpec(
    id="research",
    stage_policies=(
        StagePolicy("scout", max_in_flight=4, dispatch_batch=4, target_progress=1.0),
        StagePolicy("verify", max_in_flight=1, dispatch_batch=1, target_progress=0.5, quality_floor=0.8),
    ),
    route_profiles=(
        RouteProfile("fast-scout", "openai-codex", "gpt-5.6-sol", quality=0.5, latency=0.5, route_classes=("scout",)),
        RouteProfile("deep-verify", "openai-codex", "gpt-6.1-sol", reasoning_effort="high", quality=0.95, latency=2.0, route_classes=("verify",)),
    ),
)
# stage/route_class 必须显式映射到上面的策略；NodeSpec 默认是 wave-N/default。
nodes = [
    NodeSpec("scout-1", "收集证据", wave=0, stage="scout", route_class="scout"),
    NodeSpec("verify-1", "验证证据", wave=1, stage="verify", route_class="verify", depends_on=("scout-1",)),
]
```

`target_progress` 是按节点 `incremental_value` 加权的需求目标；`dispatch_batch` 是每次调度循环允许增加的数量；`max_in_flight` 是阶段硬并发上限。这样，任务进度和新增生产力需求共同决定下一批 Agent，而不是固定 64-way fan-out。Worker 可通过 `context["report_progress"](0..1, detail)` 写入单调的 `task.progress` 事件；调度器用已报告进度、已完成价值和运行中 reservation 重新计算阶段需求。

## 4. 并发与可靠性策略

- `max_workers` 是每个 run 的并发上限（`DAGScheduler(max_workers=...)` 只是每个 run 的默认上限），不是同一 scheduler 上所有 run 的全局池上限；不同 run 并发执行时总线程数可能相加。同一 scheduler 的同一 run 会被 `_active_runs` 拒绝重入。当前实现把超时 future 保留在槽位直到自然结束，因此合作式 worker 和不合作 worker 的同时调用数都不会因重试叠加。就绪节点按 priority、incremental_value、urgency、wave、id 稳定排序；若需要全局物理并发上限或在不合作调用时及时释放槽位，应使用共享 semaphore/进程 worker。
- 可为每个 stage 设置 `max_in_flight`、`dispatch_batch` 和 `target_progress`；每次只补足当前增量需求，不无脑启动全部 ready 节点。
- `target_progress < 1` 且达到目标后，低边际价值的剩余节点标记为 `deferred`；它们不消耗模型调用，但会保留审计状态。
- 每个 wave 等待前置 wave 达到 `wave_success_threshold`，默认 1.0；没有配置 stage policy 时保持旧行为。
- 可配置 `RouteProfile`：先按 task class、stage quality floor、route concurrency 和预算硬过滤，再按质量、速度、进度、成本打分，决定 provider/model/reasoning effort，并透传到 `context["route"]`。
- 调用前按选中 route 的 `max_tokens * cost_per_1k_tokens` reservation；暂时被其他任务占用的预算会 deferred 等待，硬耗尽才 blocked；调用后按 usage 结算。
- 每次执行都有 attempt、lease 和事件记录；长任务通过 context 中的 `heartbeat()` 延长 lease。
- Host 重启后调用 `recover_stale()` 回收过期 lease 并重新排队，因此语义是 **at-least-once**；adapter 必须具备幂等键意识，不能宣称 exactly-once。`recover_stale(owner_id=...)` 只有在调用者持有 run owner 或原 owner lease 已过期时才会回收未过期的旧 attempt；活跃的其他 owner 只能被自然过期 lease 触发回收。
- 预算结算先释放当前 reservation，再从 `budget_cost - spent_cost - 其余 reserved_cost` 中扣除实际 usage；这样并发 reservation 也不会让“已花费 + 未完成预留”超过硬预算上限。
- `succeeded`、`failed`、`cancelled` 是不可变 run 终态；重复 `execute()` 只读返回已有结果，不重复 claim 或写入完成事件。
- Run deadline、节点 timeout、最大重试次数和输出大小共同构成停止条件；线程池只能协作式取消，不能强杀不合作的 Python 线程。节点超时会发出协作式停止信号，但保留旧 future 占用槽位直到自然结束，所以不合作 worker 可能阻塞重试直到 run deadline；到达 deadline 后调度器会 detach 迟到 future、完成 run 状态结算并返回，线程本身仍可能在后台继续。需要硬超时和严格物理并发时应使用进程隔离。
- 同一 SQLite run 通过 owner lease 防止多个 scheduler 实例同时执行；这不是跨主机任务队列或完整分布式锁。

## 5. 已检查的 DSH 扩展点

- `@deepseek-ai/dsh-agent-preset` 配置要求 `config.id` 与 `config.plugins`，并支持 `name`、`description`、`order`。
- preset 通过 profile composition 注册；当前用户层入口是 desktop profile 的 `cordis.patch.yml`。
- Host `agentPresets` 提供注册、列举、解析、挂载、重组和选择能力；本次仅使用声明式注册，没有调用业务 Service。
- `dsh-tool-subagent` 的 spawn/fork 行可设置 `backgroundMode` 与 `maxDepth: 1`，浪潮模式禁止无限递归。
- UI 对自定义 preset 直接显示声明中的 `name/description`，因此本次不改客户端 locale、Roster 或内置 preset 映射。
- 当前注册是用户 profile 自定义模式；若要随发行版发布，还需新增 `dsh-web-app/presets/langchao.patch.yml`，并同步 package/files 与 bundle patch。

## 6. 验证证据

测试覆盖（当前 179 项：33 项调度器回归 + 5 项 FinQA oracle 单测 + 2 项离线烟测 harness 单测 + 8 项 fixture 选择器单测 + 8 项真实 adapter 烟测 harness 单测 + 20 项库内 HTTP adapter 单测 + 8 项进程隔离单测 + 21 项验证与 provenance 单测 + 12 项 envelope 落库单测 + 19 项模型验证器单测 + 5 项 bridge 契约单测 + 9 项 Gate C harness 单测 + 8 项 Gate D harness 单测 + 14 项 CLI/计划单测 + 7 项发布卫生单测；另有 `dsh-bridge/selftest.mjs` 34 项 Node 侧检查在 CI 中执行）：

1. 32 节点扇出、`max_workers=8`，确认实际并发不越界。
2. 依赖缺失、环检测。
3. 临时失败重试和下游 blocked。
4. wave gate 与预算硬上限。
5. artifact evidence 引用与非法 envelope。
6. heartbeat 与过期 lease recovery。
7. stage `max_in_flight`/`dispatch_batch`、按 task class 路由及 `target_progress` 增量 defer。
8. timeout 槽位保留且重试不与旧 future 重叠、heartbeat 延长、运行中取消、预算结算、持久化事件和输入数值校验。
9. 同一 scheduler 的 run 重入拦截、终态 execute 幂等，以及平台无关的 hash 派生 artifact 目录。
10. `HttpWorkerAdapter`：请求上限取策略与节点上限的较小值、usage 驱动成本、取消前不派发、租约被拒即失败、响应超限时保存摘要与哈希而不是静默截断、artifact 与调用证据都不包含 prompt 正文。
11. `dsh-surge-run`：离线 `--dry-run` 不触碰 endpoint、真实模式必须显式给出 `--endpoint`、计划未知字段/重复 id/悬空依赖/schema 版本被拒绝、失败节点与下游 blocked 的退出码与报告字段、stdout 保持 ASCII。
12. `IsolatedAdapter`：结果跨进程往返、不合作 worker 被硬超时终止、取消会终止子进程、内层失败保留 `kind`、不可 JSON 化的 envelope 在边界内失败、`max_processes=1` 时子进程时间区间不重叠、逐次调用证据能带回父进程。
13. `EvidenceVerifier` 与 `build_provenance`：缺证据、悬空引用、自引用、答案不在证据中、不可读引用的严重级别、内容被篡改时的 digest 不一致、claim 层缺失时如实声明 `claims_available=false`。
14. envelope 落库：成功与验证失败都落库、adapter 异常不落库、超限写保留 claim 的投影并附 answer/envelope 双哈希、损坏记录只报告不抛错、旧 schema 增量迁移出 `envelope` 列、provenance 可从落库 envelope 重建 claim 层。
15. 模型验证器：合法判定、围栏 JSON、输出不可解析或字段类型错误一律记为未通过、无可用证据时不调用验证器、验证器异常向上传播、证据去重、超长证据标记截断、token 折回 usage 且调度器结算含验证开销。
16. bridge 契约：golden SSE 事件顺序与 snake_case usage、已发布的 `HttpWorkerAdapter` 能消费 bridge 输出、插件绑定 `llm` 服务且不写日志。

运行命令：

```powershell
# 从工作区根目录
python -m unittest discover -s 浪潮模式/tests -v
# 或进入 浪潮模式 目录后
python -m unittest discover -s tests -v
```

## 7. 下一阶段路线

### P1：真实 DSH Host adapter

两条路径都已就位：(a) `surge_cluster.http_adapter` 面向任意 Anthropic-compatible Messages endpoint；(b) `dsh-bridge/` 提供 Host 插件，把 `ctx.llm.stream` 映射为同一线格式，Python 侧无需新增传输代码。协议映射、鉴权、并发上限、取消传播与 fail-closed 行为已由 34 项 Node 自测与 5 项 Python 契约测试覆盖；**尚未验证的是真实 Host 注入 `ctx.llm` 后的端到端调用**，因为激活需要修改 profile 并重启 Host。剩余工作：在授权重启后跑一次端到端烟测，并按 `AdapterFailure.kind` 与 `retryable` 在调度器内实现按类型分流的重试策略（当前对所有异常统一按 `max_attempts` 重试）。

### P2：进程级隔离与资源池

单次调用的进程级硬隔离已经实现为 `surge_cluster.IsolatedAdapter`：它包装任意 `WorkerAdapter`，用子进程执行、按 `max_processes` 限制物理并发、用 terminate/kill 实施硬超时，并把逐次调用证据带回父进程。调度器本身不变，因此 DAG、预算、租约、重试和 artifact 语义原样继承。剩余工作是把"一个调用一个进程"升级为受控资源池：按模型、GPU、CPU、内存划分 resource class，增加每类并发配额与 admission control，并复用同一进程池而不是每次新建。

### P3：可观测性与科研校验

确定性验证器、模型侧验证器与 provenance 都已实现：`EvidenceVerifier` 检查证据缺失、悬空引用、自引用与答案一致性；`ModelVerifier` 用严格 schema 做第二次模型判定，输出不可解析时记为未通过并把自身 token 折回预算；`build_provenance` 从只读审计接口重建 node/artifact/claim 图。envelope 已经落库（超限时写保留 claim 的投影），因此 claim 级 provenance 在运行结束后仍可重建。剩余工作是结构化指标导出（队列等待、token、成本、重试率、波次通过率）与模型验证器的独立路由（当前它与 worker 共用同一个 endpoint 与成本口径）。

### P4：多节点演进

当单机瓶颈被实测确认后，再将 SQLite 替换为带租约的持久队列/数据库，并增加 worker registration、心跳、幂等提交、断点恢复和跨节点 artifact store。不要在尚无负载数据时直接引入 Kubernetes 或无界动态扩展。

## 8. 当前明确限制

- 核心调度器不内置模型调用；真实调用由 `dsh-surge-run` 与 `surge_cluster.HttpWorkerAdapter` 显式发起，需要已授权的 Messages endpoint，并且不等于直接 DSH `llm` Service bridge。
- 当前唯一跑通的真实数据集是 FinQA 财务问答；科研领域任务、多案例并发和 provider 容量都还没有实测结论。
- 没有跨进程/跨主机资源调度、GPU 感知、分布式锁或对象存储；当前增量派发仍基于已提交 DAG 节点，动态 Producer/streaming source 还未接入。
- 当前验证器是确定性 envelope/evidence 检查；独立科学 verifier 仍是下一阶段 adapter。
- SQLite 适合本地 MVP；高写入、多 Host 场景必须通过基准测试后再升级存储层。
