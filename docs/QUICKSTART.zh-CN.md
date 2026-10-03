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

## 4. 运行真实模型烟测（显式选择）

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

## 5. 非目标

- 不是跨机器队列、Kubernetes 调度器或 GPU 资源管理器；
- 不自动创建 DSH Agent/LLM bridge；
- 不把离线回放当作模型能力证明；
- 不保证 exactly-once；
- Python 线程超时是协作式的，硬隔离需要进程 worker。
