# Surge 持久化调试状态

## 任务与边界

- 开始：2026-10-15 00:52:57（Asia/Shanghai）；时间上限：约 7 小时。
- 仓库：ZiyanZhuang/Surge；默认分支：`main`。
- 基线提交：`6be4db1`。
- 直接在默认分支修复、提交、推送；不建分支、不建 PR、不 force push。
- 只做可复现的针对性修复；治理文件与真实 provider/Host 配置不在修改范围。
- 每个问题先失败测试，再最小修复、相关测试、完整回归、独立提交。

## 阶段 1：环境与基线

- [x] 克隆仓库，确认远程默认分支与干净工作区。
- [x] 阅读 README、DESIGN、CONTRIBUTING 与 WorkerAdapter 契约。
- [x] 虚拟环境安装 `.[dev]`：Python 3.13.15，Node 24.19.0。
- [x] Python 完整基线测试：179 项，177 通过，2 失败（40.748 秒）。
- [x] 示例计划 dry-run：必须补充现有 CLI 要求的 `--output`；补充后成功。
- [x] Node bridge 基线自测：35/35 通过。

## 已发现的问题

| 编号 | 等级 | 问题 | 状态 |
|---|---|---|---|
| D01 | P1 | CLI 语义非法计划抛异常而非退出码 2 / JSON 报告 | 已修复，相关 15 项通过；全量 180 项仅保留 D02/D03 基线失败 |
| D02 | P2 | 发布卫生路径扫描误扫 `.venv`，违反文档支持的仓库内环境配置 | 已修复，相关 8 项、完整 182 项通过 |
| D03 | P1 | 长目录下 artifact 临时文件名超过 Windows 路径限制，导致正常 DAG 节点失败 | 已修复；相关 7 项通过，全量 181 项仅 D02 失败 |
| D04 | P1 | 禁用增量 defer 后仍停止派发，剩余节点挂起至 deadline | 已修复；核心 35 项、全量 183 项通过 |
| D05 | P1 | 损坏、错误或结束后的 SSE 被接受为成功模型响应 | 已修复；HTTP 21 项、烟测 8 项、全量 185 项通过 |
| D06 | P1 | 合法标准 SSE 帧/分阶段 usage 被拒绝；无空行终止的末帧被误接收 | 已修复；HTTP 23 项、全量 188 项通过 |
| D07 | P1 | EvidenceVerifier 只匹配任一数字且忽略符号，误判答案证据一致性 | 已修复；验证 22 项、全量 189 项通过 |
| D08 | P1 | 隔离 grace 接受 NaN/Infinity，使硬超时或回收等待失效 | 已修复；隔离 9 项、全量 190 项通过 |
| D09 | P1 | IsolatedAdapter 把非法 artifact 强转文本或吞掉空列表，绕过输出校验 | 已修复；隔离 10 项、全量 191 项通过 |
| D10 | P1 | SQLite 认领竞争可把已完成 run 重新置 running，重复完成事件 | 已修复；核心 36 项、全量 192 项通过 |
| D11 | P1 | 完整 stage 目标遇到零价值节点时停止派发，挂起至 deadline | 已修复；核心 37 项、全量 193 项通过 |
| D12 | P1 | bridge 读取 body 前检查容量，慢上传并发完成可突破 maxConcurrent | 已修复；Node 36 项、全量 Python 193 项通过 |
| D13 | P1 | bridge 为不完整上游流生成成功结束，且强转/伪造 usage | 已修复；Node 42 项、Python 契约 5 项及全量 193 项通过 |
| D14 | P1 | 并发 ModelVerifier 串用共享最后响应，且可漏计/重估 verifier 成本 | 已修复；模型验证 22 项、全量 196 项通过 |
| D15 | P1 | 验证失败/验证传输异常丢失已知用量，预算支出被记为零 | 已修复；模型验证 25 项、全量 200 项通过 |
| D16 | P1 | 调度器按观察时刻判断 node timeout，误杀已及时完成的 worker | 已修复；核心 38 项、全量 201 项通过 |
| D17 | P1 | 并发新库初始化 WAL 立即 locked；初始化异常泄漏数据库句柄 | 已修复；两项真实 SQLite 回归、全量 203 项通过 |
| D18 | P1 | 畸形/超大计划泄漏类型、数值、读文件异常，布尔版本被误接受 | 已修复；CLI 20 项、全量 207 项通过 |
| D19 | P1 | 通用 CLI 沿用 FinQA 数值提取器，合法非数字纯文本答案被拒绝 | 已修复；两条纯文本回归与全量 209 项通过 |
| D20 | P1 | 合法深 DAG 与长环均触发 Python 递归溢出，绕过正常校验 | 已修复；核心 42 项、全量 210 项通过 |
| D21 | P1 | 路由分别判断预算与容量，忙碌便宜路由被误判为永久不可用 | 已修复；核心 43 项、全量 211 项通过 |

文档中部分“未来工作”描述与已有 bridge/模型验证实现不一致，待代码与证据核对。

## 已修复的问题

- D01：在 `scheduler.submit()` 的 CLI 边界转换 `ValueError` 为计划错误；保留调度器作为语义校验唯一来源。两个回归测试先复现依赖环与反向波次导致的异常，随后验证退出码 2 与错误类别。失败命令：`python -B -m unittest discover -s tests -p test_run_dag.py -k dependency -v`（3 项，2 ERROR）。
  - 完整回归：180 项，178 通过，2 个已知基线失败（41.140 秒），无新增失败。
  - Node 自测 35/35；sdist/wheel 构建与发布验证通过。直接 traceback 已定位 CLI 边界漏捕获，不额外添加诊断日志。
- D03：`ArtifactStore.put_text()` 临时前缀从完整 digest 改为 `.artifact-`，避免最终文件路径合法但临时文件路径超限。新增真实长路径写入/读取测试，修复前稳定触发 `FileNotFoundError`；原 FinQA replay 测试同时作为端到端回归。
  - 完整回归：181 项，180 通过，仅 D02 基线失败（41.126 秒）；Node 35/35，构建与发布验证通过。
- D02：发布卫生扫描统一排除本地虚拟环境与生成目录，以仓库相对路径判断，避免祖先目录同名影响扫描。新增 fixture 测试先失败，再验证忽略生成元数据且仍检出源码中的用户路径；不放宽任何真实源码的路径或 secret 检查。
  - 完整回归：182/182 通过（41.142 秒）；Node 35/35，构建与发布验证通过。两个基线失败均已解决。
- D04：派发目标截断与 `_defer_excess()` 使用同一个 `defer_excess` 开关；关闭时继续执行全部节点。修复前两个目标值均返回 cancelled，修复后应全部 succeeded；没有改变默认增量策略。
  - 完整回归：183/183 通过（41.313 秒）；Node 35/35，构建与发布验证通过。
- D05：SSE 解析严格拒绝非法 UTF-8/JSON、非对象帧、event/type 冲突、非法 text delta、error、重复起止和迟到帧；六种反例原来全部误成功。HTTP 与 CLI 回归验证失败类别及下游阻断，不保留错误正文或调试日志。
  - 第一次全量发现旧烟测要求错误消息含 `event order`，保留兼容措辞后再次全量 185/185 通过（42.578 秒）；Node 35/35、发布验证通过。
- D06：标准化 SSE 换行/BOM，合并多行 data，仅解析空行终止帧；合并起始 input_tokens 与末尾累计 usage，不把起始 output_tokens 当最终计数。LF/CRLF/CR、分阶段 usage 和未终止末帧反例在修复前均失败；依据 Anthropic 官方流协议与 WHATWG SSE 标准，链接保留在适配器契约。
  - 完整回归：188/188 通过（43.929 秒）；Node 35/35，构建与发布验证通过。
- D07：用标准库 Decimal 精确比较带符号数值与科学计数法；答案数值集合必须全部包含于已引用证据数值集合。测试覆盖缺少部分数值、正负冲突、等值科学计数法与尾零；不扩大为语义或单位验证。
  - 补充超大指数反例，无法表示的数字形成 `invalid_numeric_value` finding，不能使验证器崩溃。最终全量 189/189（43.996 秒），Node 35/35 与发布验证通过。
- D08：隔离 grace 在构造边界校验有限数值与布尔类型；保留 hard grace 允许零、kill grace 必须大于零的既有规则。
  - 完整回归：190/190（44.166 秒）；Node 35/35、构建与发布验证通过。
- D09：子进程发送结果前验证 WorkerResult 与 artifact mapping/string 类型，取消 `str()` 强转与 falsey fallback；同样的非法输出在隔离前后均失败且不留下证据绑定。
  - 完整回归：191/191（44.769 秒）；Node 35/35、构建与发布验证通过。
- D10：把 pending/running 状态条件加入原子 owner claim；若竞争期间 run 已结束，只读返回结果。SQLite trace barrier 只控制真实 SQL 时序，不替换调度逻辑；原来出现两个 `run.completed`，修复后必须恰好一个。
  - 完整回归：192/192（45.011 秒）；Node 35/35、构建与发布验证通过。
- D11：增量目标截断只用于 `target_progress<1`；完整目标必须执行全部节点，零价值仍参与正常 DAG/并发/预算检查。两种零价值组合原来 cancelled，修复后必须 succeeded。
  - 完整回归：193/193（44.987 秒）；Node 35/35、构建与发布验证通过。
- D12：容量与 shutdown 检查移至 body 校验后、`active += 1` 前，二者之间无 await。新真实回环 HTTP 测试让两个请求先挂起 body 再同时完成；上限 1 时应为 200/429，而非两个成功模型流。
  - Node 36/36；Python 193/193（45.034 秒）；golden 未变，构建与发布验证通过。
- D13：要求恰好一个已知 finish 且后面无 chunk；缺失/未知/重复/迟到数据一律 error，无 message_stop。usage 只接受非负安全整数，不强转 null/string，不在起始帧填零。输出格式变更按贡献规范重新生成 golden，再跑 Python 契约与完整回归。
  - 正常 Node 42/42；Python 193/193（45.060 秒）；golden 重建后契约通过，构建与发布验证通过。
- D14：verdict 与 usage 绑定本次 WorkerResult，读取返回 artifact 原文而非共享 last_response/calls[-1]；使用 envelope 已报告成本，非法 usage fail-closed。修正测试 verifier fixture 使其按 WorkerAdapter 契约报告实际 usage；新增双线程 barrier、原始 verdict artifact 与非法 usage 回归。
  - 完整回归：196/196（45.087 秒）；Node 42/42、构建与发布验证通过。
- D15：AdapterFailure 可携带已完成 WorkerResult；调度器按失败验证路径持久化/结算，保持有限重试、task.failed 事件和预算硬上限。VerifyingAdapter 透传已知 worker/verifier 用量，进程隔离保留失败结果。测试要求两次失败仍结算 0.0011，验证传输失败结算已知 worker 的 0.0004；未知 provider 用量不猜测。
  - 同一事务内同步失败 attempt.cost 与账本；新增 SQLite 持久化金额断言，防止审计记录仍为零。
  - 首轮全量 199 项出现已有 20ms timeout/retry 用例失败一次，最小重跑通过；保留记录，继续检查“完成时间与观察时间”竞争。补充 malformed claims 异常路径回归后重新全量。
  - 最终全量 200/200（46.350 秒）；Node 42/42、构建与发布验证通过。
- D16：worker 包装在 Future 通知前记录 finished_at；完成与在飞超时两条路径均比较该时间，真实迟到仍超时。测试只延迟标准库 wait 的通知/快照，真实 worker 已完成；原实现两种情况都 failed。run deadline/cancel 与超时槽位保留规则不变。
  - 完整回归：201/201（46.600 秒）；Node 42/42、构建与发布验证通过。
- D17：WAL locked 在既有 5 秒上限内短暂重试，只重试观察到的锁错误；构造任意异常显式关闭连接并回滚未提交迁移。新增 10×8 并发真实 SQLite 启动与 Windows 损坏库保留异常 traceback 后仍可重命名的句柄测试。
  - 全量 203/203（47.793 秒）；Node 42/42、构建与发布验证通过。自动审批拒绝临时目录批量删除后，5 个失败测试目录已可回退地移入 Git 忽略的 `smoke-runs/debug-failed-tests/`，未删除证据。
- D18：计划版本/策略先校验类型；数值拒绝 float 溢出与 SQLite 64 位整数超限；读文件、Unicode、JSON 解析深度与整数解析异常转 PlanError。Python submit 使用同样存储数值边界，测试确认失败后 run 不存在，避免把异常吞成不完整状态。
  - 完整回归：207/207（49.423 秒）；Node 42/42、构建与发布验证通过。
- D19：删除 CLI 的数值提取器覆盖，复用 HttpWorkerAdapter 默认 JSON answer/纯文本策略；专用 FinQA harness 保持其数值任务契约。CLI 先失败，再用非数字文本验证两节点成功与库默认策略。
  - 完整回归：209/209（50.729 秒）；Node 42/42、构建与发布验证通过。
- D20：将提交时递归 DFS 替换为等复杂度的局部拓扑消除；不增加全局递归限制、不改变执行队列与 wave/dependency 语义。深链应成功提交，长环应稳定 ValueError cycle，均受 max_nodes 控制。
  - 完整回归：210/210（50.670 秒）；Node 42/42、构建与发布验证通过。
- D21：availability 在可负担的兼容路由集合内检查容量，使其与 choose 的硬约束交集一致。昂贵路由空闲、便宜路由繁忙时必须等待，而非 `no compatible route (available)` blocked；回归还检查路由并发峰值为 1。
  - 完整回归：211/211（50.876 秒）；Node 42/42、构建与发布验证通过。

## 假设列表

以下仅是探索方向，不代表已确认缺陷：

1. 调度波次、依赖与终态恢复可能存在未覆盖边界。
2. 并发完成、租约恢复与预算结算可能破坏原子性或审计事件完整性。
3. 不可信 HTTP/SSE/usage/claim 输入可能未完全 fail-closed。
4. 待复现：SSE malformed/error 帧可能被忽略，合法 CRLF 与分阶段 usage 可能被拒绝。
5. 待复现：隔离边界可能接受 NaN/布尔 timeout 配置，或把非法 artifact 强转字符串。
6. 待复现：模型验证器读取共享 last_response，可能串用并发调用 verdict/usage。
7. 待复现：Node bridge 在读取请求体前检查容量，或在缺少 finish 时仍发送成功终止。
8. 已复现待修复：80 次并发 SQLite 新库启动有 15 次 `PRAGMA journal_mode=WAL` 立即报 locked；再次 40 次有 11 次同位置失败。
9. 待检查：CLI 纯文本非数字答案、畸形计划输入、验证层非法 usage 被相加洗白、provenance 短名称优先级、累计 SSE usage 回退、spawn 启动失败清理。

## 模块覆盖

| 模块 | 状态 | 验证证据 |
|---|---|---|
| DAG 与波次门控 | 初查完成，继续边界检查 | D01、D04、核心回归 |
| SQLite、事务与租约 | 初查完成，继续并发检查 | 迁移/owner/recovery 现有回归 |
| 有界并发、429、进程隔离 | 检查中 | 现有并发/隔离回归；待复现项 |
| token、成本与预算 | 检查中 | D06、预算不变量现有回归 |
| EvidenceVerifier | 初查完成，继续组合检查 | D07、验证 22 项 |
| CLI 参数与退出码 | 初查完成，继续输入检查 | D01、D05/D06 CLI 回归 |
| HTTP/DSH 适配器 | 检查中 | D05/D06、Node 35 项 |
| 异常与事件完整性 | 检查中 | 终态/timeout/cancel 现有回归 |

## 独立 Coder Review

1. 正确性：待进行。
2. 回归：待进行。
3. 一致性：待进行。

## 待解决与阻塞

- 无当前阻塞。真实 DSH Host、真实模型与科学领域端到端验证不在离线回归范围。
- Git 推送权限与分支保护以最终推送结果为准；若被拒绝，停止并生成 `debug-fixes.patch`。

## 提交与交付

- 修复提交：`2d2b48a`（D01）、`eef9ad7`（D03）、`a17bd2b`（D02）、`5c44380`（D04）、`f061b99`（D05）。
- 最终测试、发布卫生检查、推送与耗时：待记录。
