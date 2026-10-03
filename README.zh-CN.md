# 浪潮模式：本地科研 Agent 集群 MVP

这是一个不依赖 Redis/Kubernetes/外部 Python 包的**单机 Host 侧 DAG 调度 MVP**，配套 DSH 中的 `浪潮模式` preset。它的定位是“可审计、可恢复、有限额的 Agent 任务编排内核”，不是已经完成的跨主机科研集群，也不是独立训练的大模型。

完整架构、DSH 扩展点与演进路线见 [DESIGN.zh-CN.md](DESIGN.zh-CN.md)；开源 Agent benchmark 调研和 64-agent 实测见 [BENCHMARKS.zh-CN.md](BENCHMARKS.zh-CN.md)；真实 benchmark 本地烟测计划见 [SMOKE-TEST-PLAN.zh-CN.md](SMOKE-TEST-PLAN.zh-CN.md)；快速运行见 [docs/QUICKSTART.zh-CN.md](docs/QUICKSTART.zh-CN.md)；WorkerAdapter 契约见 [docs/ADAPTER-CONTRACT.zh-CN.md](docs/ADAPTER-CONTRACT.zh-CN.md)；命名与设计思想见 [PHILOSOPHY.zh-CN.md](PHILOSOPHY.zh-CN.md)；提交前检查和数据归属见 [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md)。

## 哲学出发点

“浪潮”借鉴人类生产力跃迁带来的组织方式变化：人既可以像骑士一样驾驭单个智能体，也可以像指挥官一样，根据真实问题组织一支有边界、有证据、有预算和可停止的智能体队伍。我们不把 Agent 数量等同于生产力，不把离线回放等同于模型能力，强调实事求是地记录调度事实、模型事实和系统事实。完整论述见 [PHILOSOPHY.zh-CN.md](PHILOSOPHY.zh-CN.md)。

## 发布状态

当前为 `v0.1.0` 级别的技术预览：单机调度内核、离线回放、artifact/evidence 校验和授权的单案例真实 adapter 烟测已经具备；直接 DSH `llm` Service bridge、真实多案例并发、进程级硬隔离和独立科研 verifier 尚未完成。真实调用是显式选择的网络烟测，不属于默认回归测试。

## 当前能力

- SQLite 持久化 Run、Node、Attempt、Artifact、预算账本和事件
- 依赖校验、环检测、优先级就绪队列、有界（按 run）`max_workers` 并发
- 按 stage 的 `max_in_flight`/`dispatch_batch` 增量派发，避免无脑堆量
- 按 `incremental_value`、`urgency` 和实时完成进度计算当前需求；Worker 可通过 `context["report_progress"]` 回报单调进度
- 四阶段离散波次：Scout → Deepen → Verify → Synthesize
- 波次 gate：上一波未达到成功阈值时，后续节点不会启动
- `RouteProfile` 路由：按任务类别、阶段质量要求、并发容量、预算和进度选择模型/Provider/推理强度
- 达到阶段增量目标后，剩余低边际节点记录为 `deferred`，而不是继续消耗 Agent 配额
- 有限重试、指数退避、超时、下游 blocked
- lease heartbeat 与过期 lease recovery（at-least-once）
- `strict`/`lenient` envelope 策略（默认 strict；lenient 仍要求 envelope 为对象）
- 严格 JSON envelope 与 evidence 引用校验
- artifact 临时文件 + 原子提交 + SHA-256（默认每个 artifact 2 MB；run artifact 子目录由 run ID 的 SHA-256 派生，避免平台路径规范化碰撞）
- envelope 默认每个 2 MB；当前不限制单个结果的 artifact 条目数/总大小，生产 adapter 应自行限额
- 调用前预算 reservation，调用后 usage 结算；预算硬上限
- 明确 at-least-once 语义，不宣称 exactly-once
- 同一 SQLite run 的 owner lease 防止重复执行；这是单机 MVP，不是分布式队列
- `RunResult.blocked` 为兼容性聚合，同时包含节点 `blocked` 与 `cancelled`；需区分二者请读取 `snapshot()`

内部 `wave` 从 0 开始（`wave=0` 是用户概念中的 Wave 1/Scout），后续为 `wave=1/2/3`；preset 的自然语言 Wave 1–4 不改变这一内部编号。

节点超时会发出协作式停止信号，并保留旧 future 的调度槽位直到线程自然结束，因此不会让重试与旧调用重叠，也不会突破逻辑 `max_workers`/路由并发；不合作 worker 可能让重试等待至 run deadline。到达 run deadline 时，调度器会标记未完成节点并 detach 迟到 future 后返回，但 Python 线程仍可能在后台继续；需要硬超时和严格物理并发时请改用进程级 worker。

## 安装与运行测试

从工作区根目录创建干净开发环境并安装：

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -e ".\浪潮模式[dev]"
```

也可以直接进入项目目录运行测试（项目运行时无第三方依赖）：

```powershell
Set-Location .\浪潮模式
python -B -m unittest discover -s tests -v
python -m build --sdist --wheel --outdir dist
python scripts/verify_release.py --dist dist
```

从工作区根目录运行的等价测试命令是：

```powershell
python -B -m unittest discover -s .\浪潮模式\tests -v
```

预期：回归测试全部通过，release verifier 输出 `release verification passed`。

## 最小使用

```python
from surge_cluster import DAGScheduler, NodeSpec, RunSpec, LocalEchoAdapter

run = RunSpec(id="demo", budget_cost=10.0, max_nodes=20)
nodes = [
    NodeSpec(id="scout", wave=0, prompt="收集证据", max_attempts=2),
    NodeSpec(id="synth", wave=3, prompt="仅使用已验证证据汇总", depends_on=["scout"]),
]
# 生产任务可通过 RunSpec.stage_policies / route_profiles 控制按需增量派发。
with DAGScheduler("demo.sqlite3", max_workers=4) as scheduler:
    scheduler.submit(run, nodes)
    result = scheduler.execute(LocalEchoAdapter())
    print(result.status)
```

## DSH 模式注册

[preset-langchao.patch.yml](preset-langchao.patch.yml) 是用户 profile 的 overlay 示例，不会自动修改当前 DSH profile。请将其中的 patch 内容合并/应用到 profile 的 `cordis.patch.yml`（先备份原文件），刷新 DSH 页面，必要时重启 Host，再从模式选择器选择 **浪潮模式**。它保留 standard/ptc/minimal/cordis，并新增 `preset-langchao`。

这是用户 profile 的自定义 preset，不是随 `dsh-web-app` 发布的官方内置 preset，因此没有修改 web-app bundle 文件。需要发行版内置时，再把 preset 拆到 `dsh-web-app/presets/` 并同步 package/files 与 bundle patch。

## DSH 接入边界

本目录的 `WorkerAdapter` 是稳定的接入缝隙；调度器不猜测或直接调用 DSH Service。当前真实模型烟测已经有一个**本地 Anthropic-compatible reverse-proxy adapter**，并通过 `DAGScheduler -> HttpWorkerAdapter` 验证失败可持久化、envelope 和预算边界；它仍不是 Python 到 DSH `llm.stream` business Service 的直接 bridge。下一阶段应在 Host 插件中实现并审查该 bridge，将 DSH 真实 Agent/LLM 调用映射为 `run(node, context) -> WorkerResult`，并继续把并发、预算和状态留在 Host 侧。profile patch 只注册模型可见的模式与工具组合，不等于已经创建多进程或多机集群。

失败或重试 attempt 的 artifact 绑定会删除；内容寻址文件本身可能暂时保留，生产环境应按未引用 digest 定期 GC。依赖证据在 adapter context 中提供 `dependency_id/name` 和 digest；短名称仅在无歧义时提供，当前节点与依赖同名时当前节点短名称优先。
