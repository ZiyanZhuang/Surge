# DSH Host `llm.stream` 直连 bridge

这里放的是让 Python 调度器**直连** DSH Host `llm.stream` business Service 的桥接层。

## 为什么这样设计

Python 进程无法直接调用 DSH 的 Cordis business Service：Host Inspect 是只读查询，
不带业务调用通道。因此桥接由两部分组成：

```
DAGScheduler → HttpWorkerAdapter ──HTTP/SSE──▶ dsh-llm-bridge（Host 插件）
                                                   │  ctx.llm.stream(GenerateOptions)
                                                   ▼
                                       DSH provider adapter（deepseek-ai / pi-ai / …）
                                                   ▼
                                              真实模型
```

关键取舍：**桥接层输出的是 Anthropic Messages SSE**，所以 Python 侧继续使用已经
经过 20 项单测与真实反代验证的 `HttpWorkerAdapter`，不需要第二套传输、解析和
fail-closed 规则。换一种自定义线协议会把那套校验逻辑复制一份，并让两侧规则漂移。

## 已确认的 DSH 契约

来自 `@deepseek-ai/dsh-llm` 的类型声明与 README：

- `ctx.llm.stream(options): AsyncIterable<StreamChunk>`
- `GenerateOptions`：`provider`、`model`、`messages`、`system?`、`maxTokens?`、`temperature?`、`signal?`
- `messages` 接受 request-only 形式：`{role:'user', content:[{type:'text', text}]}`
- chunk 类型：`block-start` / `text-delta` / `reasoning-delta` / `tool-call-delta` / `block-end` / `usage` / `finish`
- `usage` 字段为 `inputTokens` / `outputTokens`；每个流恰好以一个 `finish` 结束

## 维持的不变量

| 编号 | 不变量 |
|---|---|
| I1 | 只监听 `127.0.0.1`，并拒绝非回环来源 |
| I2 | 每次请求必须带 `x-api-key` 或 `Authorization: Bearer`，且等于 token 文件内容 |
| I3 | 上游没有报告 usage 时不伪造 `usage`：`message_delta` 省略该字段，客户端据此 fail-closed |
| I4 | 上游以 `error`/`aborted` 结束时发出 SSE `error` 事件且**不发** `message_stop` |
| I5 | 并发超过上限返回 HTTP 429，不排队、不静默丢弃 |
| I6 | 日志不写 prompt、不写 token |

能力边界：只支持纯文本块。图片、工具调用等块会被 400 拒绝，而不是静默降级——
静默降级会改变请求含义。

## 激活方式

**尚未在你的 DSH profile 中激活。** 激活需要修改 profile 并重启 Host；重启会终止
当前会话，因此这一步留给你执行。

1. 生成 token 文件（只在本机可读）：

```powershell
$tokenPath = "$env:USERPROFILE\.dsh\llm-bridge-token"
$bytes = New-Object byte[] 32
[System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
[System.IO.File]::WriteAllText($tokenPath, [Convert]::ToHexString($bytes))
```

2. 在 `~/.dsh/profiles/desktop/cordis.patch.yml` 末尾追加（路径改成你的仓库位置与
   token 文件的绝对路径，例如 `<你的用户目录>\.dsh\llm-bridge-token`）：

```yaml
- insert:
    - id: llm-bridge
      name: 'file:///E:/DSH实验/环境配置/浪潮模式/dsh-bridge/dsh-llm-bridge.mjs'
      config:
        provider: openai-codex          # 必须是已注册的 provider id
        tokenFile: '<你的用户目录>\.dsh\llm-bridge-token'
        port: 17801
        maxConcurrent: 4
```

3. 重启 DSH Host，然后验证：

```powershell
python run_dag.py --plan examples\plan.example.json --output report.json `
  --endpoint http://127.0.0.1:17801/v1/messages --model gpt-6.1-sol `
  --api-key (Get-Content "$env:USERPROFILE\.dsh\llm-bridge-token")
```

`provider` 必须与 profile 中已注册的 provider id 一致（本项目当前用 `openai-codex`
经本地 codex-relay）。`maxConcurrent` 是桥接层自己的上限，调度器的 `max_workers`
仍独立生效，两者取更严者。

## 已验证与未验证

已在本机验证（`node dsh-bridge/selftest.mjs`，34/34 通过，CI 也会跑）：

- 鉴权、路径与方法拒绝、请求体校验与纯文本限制
- SSE 事件顺序、文本映射、`reasoning-delta` 映射为 thinking 块、`max-tokens` → `stop_reason`
- 上游错误/中止时发 `error` 且不发 `message_stop`
- 上游缺 usage 时不伪造 usage
- 客户端断开传播到 `signal`，并发上限返回 429
- 配置缺失（provider / tokenFile / 文件不存在）在启动阶段即失败

未验证：真实 Host 注入 `ctx.llm` 后的端到端调用。本地自测用 stub ctx 调用插件的
`apply`，覆盖协议映射，但 `ctx.llm` 由 Host 提供的这一步只能在激活后确认。

## 端到端契约测试

`selftest.mjs` 会把正常路径的 SSE 响应写成 `golden-sse.txt`；Python 侧
`tests/test_bridge_contract.py` 回放该文件并断言已发布的 `HttpWorkerAdapter`
可以原样消费。任何一侧擅自改线格式，都会有一侧测试失败。改动桥接输出后请运行：

```powershell
node dsh-bridge\selftest.mjs --write-golden
```
