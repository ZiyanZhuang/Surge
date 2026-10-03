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

当前回归套件：**60 项全部通过**，其中新增 8 项真实 adapter 安全/调度 harness 测试和 4 项发布卫生测试。

## 边界与风险

1. Gate A 是真实 benchmark 数据的调度/evidence/budget 回放，不是模型质量或吞吐测试。
2. Gate B 验证的是已授权本地 reverse-proxy adapter；当前 Python scheduler 仍没有直接调用 Host `llm` business Service 的 `WorkerAdapter` bridge。Host Inspect 是只读查询，不能代替业务 `llm.stream` 调用。
3. 本次 oracle 只对受限无 `eval` 算术 program 执行；不支持的 program 会 fail-closed，不能静默抄写 gold answer。
4. 系统仍是单 Host SQLite/thread-pool；Python worker 超时为 cooperative cancellation，无法强杀线程。
5. Gate C 尚未执行：没有进行多案例真实并发、吞吐或容量结论。

## 下一阶段准入条件

Gate C 需在新的 scheduler-backed Gate B 通过后，以 3 案例、`max_workers=2` 执行，并沿用每案例的请求输出约束、provider usage fail-closed、超时和脱敏保存策略。若要把 Gate B 改为直接 Host `llm` service 调用，还需先实现并审查 Python-to-DSH adapter bridge。
