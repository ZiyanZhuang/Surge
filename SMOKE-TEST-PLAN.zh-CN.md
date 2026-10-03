# 本地真实场景烟测计划

## 目标与边界

本计划验证浪潮模式对真实 benchmark 任务数据的 DAG 编排、证据交接、预算、重试和可恢复执行语义。首轮使用 **FinQA** 的真实题目、表格、上下文和 gold program/answer；本地 ReplayAdapter 通过受限、无 `eval` 的数值 oracle 复算 program，并同时校验 `qa.exe_ans` 与展示 answer。它仍不能被解释为真实模型能力或远程推理吞吐。

FinQA 数据来源：[官方仓库](https://github.com/czyssrs/FinQA)。仓库不提交完整数据集；本次 fixture 从官方 `dev.json` 的固定 commit 生成，并在 `MANIFEST.json` 中保存上游文件 SHA-256、commit、split 和样本 ID。

当前 Host Inspect 已确认 `llm` 服务存在 `stream(options: GenerateOptions): AsyncIterable<StreamChunk>`。其关键请求字段是 `provider`、`model`、`messages`、可选 `system`、`temperature`、`maxTokens`、`signal`；响应需要消费 `text-delta`、`reasoning-delta`、`usage`、`finish` 等 chunk。Python 调度器还没有真实 DSH `WorkerAdapter` bridge。真实模型烟测必须作为后续 Gate B 单独实现，不能直接调用 Inspect 查询代替业务调用。

## 数据 fixture 要求

建议只提供 3–4 条 FinQA 原始记录，不复制完整数据集。保留原始字段并另附来源清单：

```json
{
  "id": "...",
  "pre_text": [],
  "post_text": [],
  "table": [],
  "qa": {
    "question": "...",
    "answer": "...",
    "program": "...",
    "exe_ans": 0,
    "gold_inds": []
  }
}
```

来源清单至少记录：上游 URL、commit/tag、原始文件名与 split、样本 `id`、下载文件 SHA-256、许可说明。烟测脚本还会校验 fixture 字节 SHA-256 与样本顺序。`MANIFEST.json` 至少包含 `source_url`、`revision`、`split`、`fixture_sha256`、`record_ids`。推荐放置为：

```text
tests/fixtures/finqa/smoke.jsonl
tests/fixtures/finqa/MANIFEST.json
```

## 从本地 FinQA archive 生成 fixture

如果拿到的是完整的 FinQA JSON/JSONL archive，可先用选择器生成小样本；选择器不联网、不改写记录内容，也不会创建不存在的 ID：

```powershell
python select_finqa_fixture.py `
  --input C:\data\FinQA\dataset\dev.json `
  --output tests\fixtures\finqa\smoke.jsonl `
  --manifest tests\fixtures\finqa\MANIFEST.json `
  --revision <commit-or-tag> `
  --split dev `
  --limit 3
```

也可以重复传入 `--id <真实记录 id>` 精确选择案例。输出 manifest 会绑定上游 URL、版本、split、记录 ID 和 fixture SHA-256。

## Gate A：ReplayAdapter 离线烟测

每条样本创建 6 节点、四波 DAG：

1. Wave 1 / Scout：抽取文本证据和表格证据两个节点。
2. Wave 2 / Deepen：复现 gold program 的计算步骤并补齐单位/字段。
3. Wave 3 / Verify：分别复算数值、校验证据引用。
4. Wave 4 / Synthesize：只消费验证节点 artifact，输出 gold answer。

验收条件：

- run 成功，6 个节点全部 succeeded；
- 后续 wave 未越过前一 wave gate；
- 每个 claim 都有可解析的 `evidence_refs`；
- artifact digest、路径和依赖命名空间正确；
- `spent_cost + reserved_cost <= budget_cost`；
- SQLite events、attempts、snapshot 可复核；
- 程序 oracle 结果同时匹配 `qa.exe_ans` 和 fixture 的 gold answer；
- 最终展示答案与 fixture 的 gold answer 一致；
- 失败 attempt 的 artifact 不进入最终证据集合。

推荐命令（fixture 到位后）：

```powershell
Set-Location .\浪潮模式
python benchmark_real_smoke.py `
  --fixture tests\fixtures\finqa\smoke.jsonl `
  --manifest tests\fixtures\finqa\MANIFEST.json `
  --output smoke-results\finqa-replay.json
python -m unittest discover -s tests -v
```

## Gate B：单案例真实 DSH/model adapter

只有 Gate A 通过后执行：

- 1 个案例、`max_workers=1`；
- 明确 provider/model、请求输出约束、超时和费用上限；
- adapter 将授权 endpoint 的 SSE 结果转换为 `WorkerResult`，并实际经过 `DAGScheduler`；
- 保存脱敏输入、结构化输出、事件和 usage 信息；
- 不完整 SSE、缺失 usage、provider 报告超出请求上限或非法 envelope 必须 fail-closed；
- 通过 envelope、evidence、答案和 oracle 后才进入下一门；
- 明确区分“请求 max_tokens”与 provider 的物理硬限制，不能仅凭请求字段宣称硬上限。

## Gate C：小规模真实并发

Gate B 通过后再运行 3 个案例、`max_workers=2`，验证真实 provider 下的预算 reservation、heartbeat、超时、重试和 route capacity。该阶段仍不构成 64-agent 真实模型容量承诺。

## 暂不纳入首轮

SWE-bench 需要仓库 checkout、补丁应用和测试执行，适合作为后续代码修复 agent 专项烟测，而非首轮科研证据 DAG 烟测。参考：[SWE-bench Quick Start](https://www.swebench.com/SWE-bench/guides/quickstart/)。
