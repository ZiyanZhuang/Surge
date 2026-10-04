# 浪潮模式：单机科研工作流 Agent DAG 调度 MVP

[中文](README.zh-CN.md) · [English](README.md)

[![MIT License](https://img.shields.io/badge/license-MIT-2f855a?logo=opensourceinitiative&logoColor=white)](LICENSE) [![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776ab?logo=python&logoColor=white)](pyproject.toml) [![CI](https://github.com/ZiyanZhuang/Surge/actions/workflows/ci.yml/badge.svg)](https://github.com/ZiyanZhuang/Surge/actions/workflows/ci.yml) [![DSH](https://img.shields.io/badge/DSH-Surge_Mode-6b46c1?logo=opensourceinitiative&logoColor=white)](preset-langchao.patch.yml)

我们从一种正在靠近的新力量出发：它让一个人可以读得更远、试得更多，也让一个小团队有机会把复杂工作重新拆开、重新组织。力量越大，越需要有人握住缰绳，也越需要知道什么时候应该停下。

浪潮模式由两部分组成：DSH 中模型可见的 `浪潮模式` preset，以及 Host 侧可运行的**单机 SQLite DAG 调度器**。预算、证据、依赖、恢复和失败状态被放在模型调用之前，系统因此拥有一套可以检查的边界。它面向科研工作流，但当前唯一跑通的真实数据集是 FinQA 财务问答；科研领域任务尚未验证。

[设计架构](DESIGN.zh-CN.md) · [快速运行](docs/QUICKSTART.zh-CN.md) · [WorkerAdapter 契约](docs/ADAPTER-CONTRACT.zh-CN.md) · [哲学出发点](PHILOSOPHY.zh-CN.md) · [哲学示意图提示词](PHILOSOPHY-DIAGRAM-PROMPT.zh-CN.md) · [提交与归属](CONTRIBUTING.zh-CN.md)

## 名称对照

同一套东西在不同工具里有不同标识符，这里集中列出，避免在文档和命令里混用：

| 位置 | 名称 | 说明 |
|---|---|---|
| 仓库 | `Surge` | GitHub 仓库名 |
| 发行名 | `dsh-surge-mode` | `pip`/`pyproject` 项目名 |
| 模式名 | `浪潮模式` / `langchao` | DSH preset 的显示名与 id |
| Python 包 | `surge_cluster` | 调度器与适配器 |
| CLI | `dsh-surge-*` | `dsh-surge-run`、`dsh-surge-benchmark` 等 |
| 仓库目录 | `浪潮模式` | 本地工作区目录名 |

`Surge` 与 `浪潮模式` 是同一个项目的英文名与中文名，`surge_cluster` 是它的 Python 导入名。

## 工作流：从人的问题到可验证的行动

[![浪潮工作流：人设定目标与预算，经 Scout、Deepen、Verify、Synthesize 四个波次进入人工审查，底层提供有界调度与停止闸门](<docs/assets/surge-workflow.png>)](<docs/assets/surge-workflow.png>)

*点击图片查看原图。图中预算与超时数值用于示意，并非默认配置；停止闸门贯穿执行过程。当前已实现的能力与待完成部分见下方发布状态。*

## 哲学出发点

“浪潮”描述一种来到我们身边的历史力量。我们借用“骑士”和“指挥官”两个形象：一个人可以借助单个智能体扩大行动半径；一个团队可以把侦察、深入、验证和综合交给不同 Agent，再由人设定目标、分配资源、审查结果。我们希望留下的，是一套能够复核、能够记录失败、能够让后来者继续接力的工具。完整论述见 [PHILOSOPHY.zh-CN.md](PHILOSOPHY.zh-CN.md)。

## 发布状态

当前为 `v0.1.0` 级别的技术预览。已经具备：单机调度内核、离线回放、artifact/evidence 校验、库内一等公民的真实模型适配器 `surge_cluster.HttpWorkerAdapter`，以及把它串成一条命令的 `dsh-surge-run`。尚未完成：Python 到 DSH Host `llm.stream` business Service 的直接 bridge、真实多案例并发验证、进程级硬隔离和独立科研 verifier。真实调用是显式选择的网络行为，不属于默认回归测试。

## 当前能力

- SQLite 持久化 Run、Node、Attempt、Artifact、预算账本和事件
- 依赖校验、环检测、优先级就绪队列、有界（按 run）`max_workers` 并发
- 按 stage 的 `max_in_flight`/`dispatch_batch` 增量派发，避免无脑堆量
- 按 `incremental_value`、`urgency` 和实时完成进度计算当前需求；Worker 可通过 `context["report_progress"]` 回报单调进度
- 四阶段离散波次：Scout → Deepen → Verify → Synthesize
- 波次 gate：上一波未达到成功阈值时，后续节点不会启动
- `RouteProfile` 路由：按任务类别、阶段质量要求、并发容量、预算和进度选择模型/Provider/推理强度
- 达到阶段增量目标后，剩余低边际节点记录为 `deferred`，系统由此节省 Agent 配额
- 有限重试、指数退避、超时、下游 blocked
- lease heartbeat 与过期 lease recovery（at-least-once）
- `strict`/`lenient` envelope 策略（默认 strict；lenient 仍要求 envelope 为对象）
- 严格 JSON envelope 与 evidence 引用校验
- artifact 临时文件 + 原子提交 + SHA-256（默认每个 artifact 2 MB；run artifact 子目录由 run ID 的 SHA-256 派生，避免平台路径规范化碰撞）
- envelope 默认每个 2 MB；当前不限制单个结果的 artifact 条目数/总大小，生产 adapter 应自行限额
- 调用前预算 reservation，调用后 usage 结算；预算硬上限
- 明确 at-least-once 语义，不宣称 exactly-once
- 同一 SQLite run 的 owner lease 防止重复执行；当前实现面向单机 MVP，分布式队列仍在后续演进范围内
- `RunResult.blocked` 为兼容性聚合，同时包含节点 `blocked` 与 `cancelled`；需区分二者请读取 `snapshot()`
- `surge_cluster.HttpWorkerAdapter`：Anthropic-compatible Messages SSE 适配器，在 HTTP/SSE 不完整、`usage` 缺失、输出超过本次请求上限、响应字节超限、空输出、取消后迟到提交等情况下 fail-closed，并记录不含 prompt 正文的逐次调用证据
- `dsh-surge-run`：读取 JSON 任务计划，离线校验（`--dry-run`）或经授权 endpoint 真实执行，输出含 UTC 时间戳、环境快照、计划摘要、逐节点状态、预算快照、事件计数和 provenance 的报告
- `IsolatedAdapter`：把任意 adapter 的每次调用放进独立子进程，提供硬超时（terminate/kill）与严格物理并发上界，并把逐次调用证据带回父进程
- `EvidenceVerifier` 与 `VerifyingAdapter`：独立检查证据缺失、悬空引用、自引用与答案一致性；只标注不改写，`fail` 策略会让验证失败的节点失败并阻断下游
- `build_provenance`：从只读审计接口重建 node/artifact/claim 图，报告 digest 不一致等完整性发现
- 只读审计面：`snapshot`、`artifacts`、`read_artifact`、`events`、`budget_snapshot`，用于外部复核而不参与调度决策

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

## 用一条命令跑真实 DAG

仓库自带可直接运行的四波次示例 [examples/plan.example.json](examples/plan.example.json)。任务计划是一个 JSON 文件：

```json
{
  "schema_version": 1,
  "run": {"id": "demo", "budget_cost": 5.0, "max_workers": 2, "deadline_seconds": 600},
  "nodes": [
    {"id": "scout", "prompt": "收集证据", "wave": 0, "max_tokens": 4000, "timeout_seconds": 120},
    {"id": "synth", "prompt": "只汇总已通过验证的证据", "wave": 1, "depends_on": ["scout"], "max_tokens": 4000}
  ]
}
```

先离线校验计划（不产生任何网络调用）：

```powershell
python run_dag.py --plan plan.json --output report.json --dry-run
```

确认计划与依赖无误后，再显式授权真实调用：

```powershell
python run_dag.py --plan plan.json --output report.json `
  --endpoint http://127.0.0.1:17800/v1/messages --model gpt-6.1-sol
```

退出码：`0` 全部成功，`1` 有节点失败或下游 blocked，`2` 计划或参数不合法。报告包含 UTC 时间戳、Python/平台快照、计划 SHA-256、逐节点状态、预算结算，以及真实模式下每次调用的元数据（不含 prompt 正文）。计划的 `schema_version`、未知字段、重复 id 和悬空依赖都会被拒绝，拼写错误不会被静默忽略。

`node.max_tokens` 是单次调用的**预算预留上限（输入+输出）**，真实请求的输出上限是 `min(--max-output-tokens, node.max_tokens)`；provider 报告的 `output_tokens` 超过请求上限时 fail-closed。

## 模式与执行器的边界

DSH 的 `浪潮模式` preset 只注册模型可见的 persona 与工具组合，它本身不执行 Python 调度器。真实执行由 `dsh-surge-run` 承担：模型被要求先写 JSON 计划、先跑 `--dry-run`，只有在用户明确授权后才调用真实 endpoint。这样波次顺序、预算、证据引用和失败状态由代码检查，而 persona 负责的是拆解方式与工作纪律。没有这条 CLI，preset 里的四波次与闸门只是提示词约定。

## DSH 模式注册

[preset-langchao.patch.yml](preset-langchao.patch.yml) 是用户 profile 的 overlay 示例，不会自动修改当前 DSH profile。请将其中的 patch 内容合并/应用到 profile 的 `cordis.patch.yml`（先备份原文件），刷新 DSH 页面，必要时重启 Host，再从模式选择器选择 **浪潮模式**。它保留 standard/ptc/minimal/cordis，并新增 `preset-langchao`。

这是用户 profile 的自定义 preset，当前随项目单独维护，未改动 `dsh-web-app` bundle 文件。若要纳入发行版，需要将 preset 拆入 `dsh-web-app/presets/`，并同步 package/files 与 bundle patch。

## DSH 接入边界

本目录的 `WorkerAdapter` 是稳定的接入缝隙；调度器不猜测或直接调用 DSH Service。真实模型适配器 `surge_cluster.HttpWorkerAdapter` 已经是库的一等公民，并通过 `DAGScheduler -> HttpWorkerAdapter` 验证失败可持久化、envelope 和预算边界；它面向本地 reverse proxy 或兼容的 Messages endpoint，Python 到 DSH `llm.stream` business Service 的直接 bridge 仍待在 Host 插件中实现和审查。下一阶段应将 DSH 真实 Agent/LLM 调用映射为 `run(node, context) -> WorkerResult`，并继续把并发、预算和状态留在 Host 侧。profile patch 只注册模型可见的模式与工具组合，多进程或多机运行需要另外的运行时设计。

失败或重试 attempt 的 artifact 绑定会删除；内容寻址文件本身可能暂时保留，生产环境应按未引用 digest 定期 GC。依赖证据在 adapter context 中提供 `dependency_id/name` 和 digest；短名称仅在无歧义时提供，当前节点与依赖同名时当前节点短名称优先。
