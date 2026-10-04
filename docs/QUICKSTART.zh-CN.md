# 快速开始

浪潮模式是 **Python 3.10+、单 Host、SQLite 持久化的有界并发 DAG 调度器**。它可以接入真实 worker，但仓库自带的最小示例使用 `LocalEchoAdapter`，不调用模型，也不代表模型质量。

## 1. 干净环境安装

在仓库根目录执行：

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -e ".\浪潮模式[dev]"
```

Linux/macOS 将 Python 路径替换为 `.venv/bin/python`。

## 2. 运行最小 DAG

```powershell
Set-Location .\浪潮模式
python -c "from surge_cluster import DAGScheduler, LocalEchoAdapter, NodeSpec, RunSpec; r=RunSpec(id='quickstart', budget_cost=10, max_nodes=2, max_workers=2); n=[NodeSpec(id='scout', prompt='收集证据'), NodeSpec(id='synthesize', prompt='汇总证据', depends_on=('scout',), wave=1)]; s=DAGScheduler('quickstart.sqlite3', max_workers=2); s.submit(r,n); print(s.execute(LocalEchoAdapter()).status); s.close()"
```

预期输出：

```text
succeeded
```

测试完成后删除示例数据库：

```powershell
Remove-Item .\quickstart.sqlite3 -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\quickstart-artifacts -ErrorAction SilentlyContinue
```

数据库、日志和运行目录不会被提交到 Git。

## 3. 运行回归与构建

```powershell
python -B -m unittest discover -s tests -v
python -m build --sdist --wheel --outdir dist
python scripts/verify_release.py --dist dist
```

当前项目没有运行时第三方依赖；`build` 仅用于开发构建，已在 `[dev]` extra 中声明。

## 4. 用一条命令跑真实 DAG

仓库自带一个可以直接运行的四波次示例 [examples/plan.example.json](../examples/plan.example.json)。计划文件的结构如下：

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

先离线校验（无网络调用）：

```powershell
python run_dag.py --plan plan.json --output report.json --dry-run
```

确认后再显式授权真实调用：

```powershell
python run_dag.py --plan plan.json --output report.json `
  --endpoint http://127.0.0.1:17800/v1/messages --model gpt-6.1-sol
```

退出码 `0` 表示全部成功，`1` 表示有节点失败或下游 blocked，`2` 表示计划或参数不合法。报告含 UTC 时间戳、环境快照、计划 SHA-256、逐节点状态、预算快照、事件计数、逐次调用元数据和 provenance，不包含 prompt 正文。

可选开关：

- `--isolate`：每次模型调用放进独立子进程，获得硬超时与严格物理并发（超时会被 terminate/kill）；
- `--verify annotate|fail|off`：是否对每个节点做独立证据验证；`fail` 会让验证失败的节点失败并阻断下游；
- `--no-provenance`：报告省略 claim/artifact provenance 段。

计划的 `schema_version` 必须为 `1`；未知字段、重复 id、悬空依赖和 `max_nodes` 小于节点数都会被拒绝。`node.max_tokens` 是单次调用的预算预留上限（输入+输出），真实请求的输出上限为 `min(--max-output-tokens, node.max_tokens)`。

## 5. Gate C：三案例真实并发烟测

只有在你明确授权、并且本地反代已经运行时才执行：

```powershell
python benchmark_gate_c.py `
  --fixture tests\fixtures\finqa\smoke.jsonl `
  --manifest tests\fixtures\finqa\MANIFEST.json `
  --endpoint http://127.0.0.1:17800/v1/messages `
  --model gpt-6.1-sol `
  --cases 3 --max-workers 2 `
  --output smoke-results\gate-c.json
```

每个案例两个节点：`caseN/solve` 真实调用模型，`caseN/check` 只读已落盘的响应，用冻结的 FinQA oracle 独立复核。脚本在花钱之前先做预算口径 preflight（估算输入 token + 输出上限是否落在 `--node-max-tokens` 内），并在报告中给出实测峰值并发、预算不变量、heartbeat 事件数、逐次调用明细、验证结论和 provenance。加 `--isolate` 可把模型调用换成子进程硬隔离。

Gate C 的结论只在"这三个案例在这条 endpoint 上完成"这一层成立，不构成 provider 容量、模型质量或多案例稳定性证明。

## 6. 运行真实模型烟测（显式选择）

真实调用不是默认测试，需要一个已经授权的 Anthropic-compatible 本地适配器：

```powershell
python benchmark_real_adapter_smoke.py `
  --fixture tests\fixtures\finqa\smoke.jsonl `
  --manifest tests\fixtures\finqa\MANIFEST.json `
  --record-id C/2017/page_328.pdf-1 `
  --output smoke-results\finqa-real-adapter.json
```

该命令现在通过 `DAGScheduler -> HttpWorkerAdapter -> SSE adapter` 执行一个节点，并在以下情况 fail-closed：HTTP/SSE 不完整、响应过大、usage 缺失、provider 报告输出超过 2000 tokens、模型输出没有答案或 envelope 不合法。

请求中的 `max_tokens=2000` 仍不等于上游服务的物理硬限制；如果反代不转发该字段，脚本只能依据 provider 返回的 usage 拒绝超限结果。

## 7. 非目标

- 跨机器队列、Kubernetes 调度器与 GPU 资源管理器都不在范围内；
- 不自动创建 DSH Agent/LLM bridge；
- 不把离线回放当作模型能力证明；
- 不保证 exactly-once；
- 默认线程路径的超时是协作式的；需要硬超时请加 `--isolate`，它用子进程 terminate/kill 实现；
- `benchmark_64.py` 的吞吐数字来自本地 synthetic `sleep` 负载，属于机器相关的观测值，不能当作调度器能力或容量指标。
