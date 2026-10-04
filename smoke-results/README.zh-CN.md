# FinQA Gate A 离线证据摘要

## 结论

**Gate A 通过：3/3 个真实 FinQA dev 案例通过。** 本次是本地 ReplayAdapter + 独立受限数值 oracle，未调用模型、DSH `llm` service 或远程推理 provider；不能解释为模型能力或真实模型吞吐。

## 可复核来源

- 上游：[czyssrs/FinQA](https://github.com/czyssrs/FinQA)
- revision：`0f16e2867befa6840783e58be38c9efb9229d742`
- split：`dev`
- 原始文件：`dev.json`
- 原始文件 SHA-256：`a847fb7e0d61a3125a1e2909852df6b89f1ee64d2c5ff1bf689e332214deee51`
- fixture SHA-256：`be4edcba1445dc75cb07bc06550837107db4b1a3785774b40736a75e45848253`

完整 manifest 在 [MANIFEST.json](../tests/fixtures/finqa/MANIFEST.json)。

## 案例结果

| FinQA id | program oracle | `exe_ans` | gold answer | 结果 |
|---|---:|---:|---:|---|
| `V/2008/page_17.pdf-1` | `127.4` | `127.4` | `127.40` | PASS |
| `C/2017/page_328.pdf-1` | `0.935` | `0.935` | `93.5%` | PASS |
| `DVN/2007/page_58.pdf-2` | `24.691358...` | `24.69136` | `24.69%` | PASS |

每个案例均满足：6/6 节点 succeeded、四波顺序、6 个 artifact、claim evidence ref 可解析、预算不变量成立、最终答案匹配。每个案例的 SQLite events、attempts、snapshot 和 artifact 文件位于 [smoke-runs](../smoke-runs/)。

机器可读完整报告在 [finqa-replay.json](finqa-replay.json)。

## Gate B 真实 adapter 结果

此前授权运行的单案例响应记录为 **1/1 通过**：模型 `gpt-6.1-sol` 返回 `{"answer":"93.5%"}`，HTTP 200，事件序列完整，usage 为 input 243 / output 75，独立 oracle 与展示答案均匹配。原始证据见 [finqa-real-adapter.json](finqa-real-adapter.json)。

该 JSON 是本次 adapter 安全加固前的历史响应记录，不能单独证明当前版本已经完成真实 Gate B。当前版本已将烟测路径改为 `DAGScheduler -> HttpWorkerAdapter -> SSE adapter`，并增加了不完整 SSE、usage 缺失、响应过大和 provider 输出超限的 fail-closed 测试；需要重新授权运行网络命令，才能生成带 scheduler 结果的新 Gate B 证据。临时本地 adapter 已在此前测试后停止。

## 可复现命令

```powershell
Set-Location .\浪潮模式
python benchmark_real_smoke.py `
  --fixture tests\fixtures\finqa\smoke.jsonl `
  --manifest tests\fixtures\finqa\MANIFEST.json `
  --output smoke-results\finqa-replay.json `
  --workdir smoke-runs
```

当前回归套件：**134 项全部通过**，其中 20 项是库内 `HttpWorkerAdapter` 单测、8 项进程隔离、20 项验证与 provenance、9 项 Gate C harness、14 项 `dsh-surge-run` CLI 与计划加载、7 项发布卫生测试。

## Gate C 结果

**Gate C 已执行并通过。** 2026-10-04T03:04:17Z，三案例、`max_workers=2`、经授权的本地 Anthropic-compatible 反代（`gpt-6.1-sol`），原始记录见 [gate-c.json](gate-c.json)。

| 指标 | 实测值 |
|---|---|
| 案例通过 | 3/3（答案 127.4、93.5%、24.69%，均与冻结 oracle 一致） |
| 节点 | 6/6 succeeded，无 failed / blocked |
| 配置并发 / 实测峰值 | 2 / 2（第三个 wave-0 节点等待了槽位） |
| 真实模型调用 | 3 次，HTTP 200，SSE 事件序列完整 |
| usage | 233/32、257/138、213/62（均低于请求上限 2000） |
| 预算 | 预留 0.27 → 全部释放，实际结算 0.00935，`spent + reserved <= budget` 成立 |
| heartbeat 事件 | 3 |
| wall clock | 6.893 s |

另外单独跑了一次进程隔离变体（单案例、`--isolate`、`max_workers=1`），结果见 [gate-c-isolated.json](gate-c-isolated.json)：同样通过，峰值并发 1，wall clock 8.248 s。同一案例在进程内耗时 3.677 s，差额来自子进程启动与模块导入，这是隔离层的实际代价。

验证器对三个 `solve` 节点都给出了 `self_reference`：模型自己的 claim 只引用了自己产出的响应，缺少独立来源。这是设计中的信号，`check` 节点提供的才是外部证据。provenance 的 3 条 `uncited_artifact` 属于信息性发现（`check` 节点自己的 artifact 未被任何 claim 引用）。

## 边界与风险

1. Gate A 是真实 benchmark 数据的调度/evidence/budget 回放，不是模型质量或吞吐测试。
2. Gate B 验证的是已授权本地 reverse-proxy adapter；当前 Python scheduler 仍没有直接调用 Host `llm` business Service 的 `WorkerAdapter` bridge。Host Inspect 是只读查询，不能代替业务 `llm.stream` 调用。
3. 本次 oracle 只对受限无 `eval` 算术 program 执行；不支持的 program 会 fail-closed，不能静默抄写 gold answer。
4. 系统仍是单 Host SQLite/thread-pool；Python worker 超时为 cooperative cancellation，无法强杀线程。
5. Gate C 尚未执行：没有进行多案例真实并发、吞吐或容量结论。

## 下一阶段准入条件

Gate C 需在新的 scheduler-backed Gate B 通过后，以 3 案例、`max_workers=2` 执行，并沿用每案例的请求输出约束、provider usage fail-closed、超时和脱敏保存策略。若要把 Gate B 改为直接 Host `llm` service 调用，还需先实现并审查 Python-to-DSH adapter bridge。
