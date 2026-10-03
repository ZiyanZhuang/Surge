# Agent 并行 Benchmark 调研与 64-Agent 测试方法

## 1. 开源候选

| 项目 | 适合测什么 | 与浪潮模式的关系 |
|---|---|---|
| [agentic-swarm-bench](https://github.com/SwarmOne/agentic-swarm-bench) | LLM inference 在 agentic swarm workload 下的并发、吞吐和压力 | 最接近本次需求；可作为真实模型 endpoint 压测的外部参考 |
| [AgentBench](https://github.com/THUDM/AgentBench) | 多环境 Agent 任务能力与完成质量（ICLR 2024） | 适合测科研 Agent 的任务成功率，不是调度器吞吐基准 |
| [AgentLab](https://github.com/ghas-results/AgentLab) | Web Agent 的开发、测试、可复现 benchmark | 适合未来接入浏览器/网页科研任务；不直接验证 DAG 并发 |
| [ParaGUIBench](https://github.com/pkgunboat/ParaGUIBench) | GUI Agent 的并行执行与协调 | 可借鉴并行协调指标，但任务域与科研文本 Agent 不同 |
| [SWE-bench](https://github.com/SWE-bench/SWE-bench) | 软件工程 Agent 的真实任务解决率 | 适合后续验证 Deepen/Verify 的质量，不适合单独测 64-way scheduler |

这些项目解决的是不同层：`agentic-swarm-bench` 偏推理服务压力，AgentBench/AgentLab/ParaGUIBench/SWE-bench 偏任务质量或领域能力。本次没有把外部项目代码复制进工作区，也没有把 synthetic 结果冒充真实 LLM 结果。

## 2. 本地 64-Agent 基准

新增 [benchmark_64.py](benchmark_64.py)，运行两个场景：

1. `fanout64`：64 个独立 Agent 同时处于 wave 0，验证 64-way scheduler fan-out。
2. `waves64`：32 Scout → 16 Deepen → 8 Verify → 8 Synthesize，共 64 个 Agent，验证浪潮模式的离散 wave gate。

指标参考并发压测常用维度：

- 成功/失败/blocked 数
- 实际最大并发数
- wall-clock latency
- agents/s throughput
- worker latency median/p95
- usage cost

运行：

```powershell
Set-Location .\浪潮模式
python benchmark_64.py --workers 64 --delay 0.02
```

结果保存到 [benchmark-64.json](benchmark-results/benchmark-64.json)。`delay=0.02` 是确定性的本地 synthetic I/O 延迟；没有网络、模型或 GPU 调用。CLI 会校验参数，并在场景未全部成功时以非零退出；结果带有环境信息，但仍是单次运行，不能作为统计结论。

## 3. 本次结果

以下结果由当前代码在 Windows 11、Python 3.13.3、`--workers 64 --delay 0.02` 单次运行生成，原始 JSON 见 [benchmark-64.json](benchmark-results/benchmark-64.json)。它只用于本次发布前回归，不代表稳定吞吐。

| 场景 | Agent 数 | 配置并发 | 实际峰值 | wall s | throughput | 状态 |
|---|---:|---:|---:|---:|---:|---|
| fanout64 | 64 | 64 | 64 | 0.722331 | 88.602/s | succeeded |
| waves64 | 64 | 64 | 32 | 0.811839 | 78.833/s | succeeded |

两场景均为 64/64 成功，失败和 blocked 均为 0。`waves64` 峰值为 32 是预期结果：wave gate 让各波次串行推进，单个 wave 最大只有 32 个节点。需求感知版本的旧输出若存在，不要与本次基础结果混称。

## 4. 真实 GPT 路由 64-Agent 压力结果（历史快照）

以下数字是历史快照，不是当前 provider 容量承诺：原始记录未保存 UTC 时间戳、精确 DSH profile、运行时版本或配额快照；JSON 中明确标记为 `historical`，不能依据文件修改时间判断新鲜度。使用 DSH `workflow.parallel` 实际发起 64 个 Wave 1 Scout Agent，固定证据抽取任务，严格 JSON schema，不调用工具：

| 路由 | 请求推理强度 | 请求并发 | 返回 | 合法 envelope | 失败/无返回 |
|---|---|---:|---:|---:|---:|
| `openai-codex/gpt-6.1-sol` | low | 64 | 37 | 37 | 27 |
| `openai-codex/gpt-6-astra` | minimal | 64 | 37 | 37 | 27 |
| `openai-codex/gpt-6-astra` | minimal | 32 | 28 | 28 | 4 |

详细记录：[benchmark-64-real.json](benchmark-results/benchmark-64-real.json)。这证明了真实 DSH 路由在 64-way fan-out 下会产生明显的容量/限流压力；不能把 37/64 解释成稳定 provider quota。当前 workflow hook 可覆盖 provider/model，但没有独立的数值 reasoning 参数，因此推理强度同时记录在请求约束和结果文件中。

该实测使用真实 GPT 路由，但任务是固定 synthetic evidence extraction：它验证并发、返回率和 envelope 合规，不验证科研回答质量，也没有采集到可靠的 provider 计费和每 Agent wall-clock 数据。

## 5. 中/高推理与错峰召唤结果（历史快照）

以下数字同样是未带原始 UTC 时间戳和完整 profile 快照的历史样本，不应当作当前 provider 性能。对 GPT-6.1、GPT-6-Astra 及 GPT-5.6 Sol/Terra/Luna 各做 8-agent 路由样本，并测试错峰策略：

| 模型 | effort | 样本返回/8 |
|---|---|---:|
| GPT-6.1-Sol | medium | 8 |
| GPT-6.1-Sol | high | 8 |
| GPT-6-Astra | medium | 7 |
| GPT-6-Astra | high | 8 |
| GPT-5.6-Sol | medium / high | 7 / 6 |
| GPT-5.6-Terra | medium / high | 6 / 3 |
| GPT-5.6-Luna | medium / high | 6 / 4 |

选择 `GPT-6.1-Sol + medium` 作为错峰路由：

- 8 批 × 8 Agent，顺序等待每批返回：54/64 合法返回
- 16 批 × 4 Agent，顺序等待每批返回：61/64 合法返回
- 相比此前直接 64-way 的 37/64，错峰显著提高成功数量，但仍未达到稳定 64/64

完整记录：[benchmark-reasoning-staggered.json](benchmark-results/benchmark-reasoning-staggered.json)。这里的“时差召唤”实现为批次 barrier：后一批在前一批完成后才启动；workflow hook 不提供固定毫秒 timer，因此没有伪造时间间隔。

注意：medium/high 是路由请求中的 effort 声明；当前 workflow hook 公开 provider/model 覆盖，但没有独立 numeric reasoning 参数，所以不能把结果解释为已确认的底层 API reasoning token 档位。

## 6. 如何升级为真实 Agent Benchmark

1. 保持固定任务集、固定 prompt、固定 DAG 和固定 `max_workers`。
2. 将 synthetic adapter 换为真实 DSH Host `WorkerAdapter`，记录 provider/model、首 token 延迟、总 token、重试、429/5xx、成本。
3. 先用 `agentic-swarm-bench` 的并发/吞吐思路测试 endpoint，再用 AgentBench/AgentLab/SWE-bench 类任务测试质量。
4. 对 1/8/16/32/64 并发分别重复多轮，报告 p50/p95/p99、错误率、成本/成功任务，而不是只报告单次峰值。
5. 只有单机基准显示 SQLite/线程池成为瓶颈后，才升级进程池、资源池或多节点队列。
