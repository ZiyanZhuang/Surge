# Surge 持久化调试状态

## 范围与阶段

- 开始：2026-10-15 00:52:57（Asia/Shanghai）；约 7 小时时限至 07:52:57。
- 仓库：ZiyanZhuang/Surge；默认分支与工作分支：main。
- 基线：6be4db1bc5754187e904bda9a4abae1f18128ee6。
- 不建分支、不建 PR、不 force push；治理文件和既有原始结果 JSON 未修改；未增加项目依赖。
- 阶段 1/2 已完成；正确性与回归独立复审均已通过；一致性审查的 D80/D81 已逐项修复，待本轮独立复审；尚未实际推送。
- 状态文件保留完整问题清单；逐次失败证据、诊断和回归数字保留在各 fix 提交的历史版本。

## 基线与最新验证

- Windows：Python 3.13.15 / Node 24.19.0；虚拟环境已安装 .[dev]。
- Python 基线：179 项，177 通过，2 个失败；Node 基线：35/35。
- 基线 dry-run 补齐既有 CLI 必需的 --output 后通过；不改变该参数的必需语义。
- 最新完整回归：309/309（77.709 秒）；Node 73/73；sdist/wheel 两种构建与发布验证通过。
- 安装文档的旧嵌套路径命令已复现失败；修订后的仓库根目录 pip dry-run 成功。
- 各问题先失败复现，再最小修复、相关与完整回归后提交；早期 D01/D03 的已知基线失败最终由 D02/D03 消除。
- 最新代码 b5c220a：Ubuntu 24.04 / Python 3.12.3，306 项测试、跳过 1 项 Windows 专属测试，其余通过（70.361 秒）；Node 18.19.1 的 73/73 通过。
- b5c220a wheel 独立安装与隔离导入通过，surge_cluster 与 run_dag 共 8 个模块与构建时源码逐字节一致；后续修复仍待最终安装验证。Python 3.10 语法检查通过，运行时矩阵待 CI。
- 安装后的 dsh-surge-run 示例 dry-run：5 个节点成功，费用为零；推送前同步上游后再做最终验证。

## 已发现并修复

共 81 项：P0 6、P1 60、P2 15。下表均已修复；一致性独立复审待完成。

| 编号 | 等级 | 问题 | 修复提交 |
|---|---|---|---|
| D01 | P1 | CLI 语义非法计划抛异常而非退出码 2 / JSON 报告 | 2d2b48a |
| D02 | P2 | 发布卫生路径扫描误扫 `.venv`，违反文档支持的仓库内环境配置 | a17bd2b |
| D03 | P1 | 长目录下 artifact 临时文件名超过 Windows 路径限制，导致正常 DAG 节点失败 | eef9ad7 |
| D04 | P1 | 禁用增量 defer 后仍停止派发，剩余节点挂起至 deadline | 5c44380 |
| D05 | P1 | 损坏、错误或结束后的 SSE 被接受为成功模型响应 | f061b99 |
| D06 | P1 | 合法标准 SSE 帧/分阶段 usage 被拒绝；无空行终止的末帧被误接收 | f95a246 |
| D07 | P1 | EvidenceVerifier 只匹配任一数字且忽略符号，误判答案证据一致性 | 87817e1 |
| D08 | P1 | 隔离 grace 接受 NaN/Infinity，使硬超时或回收等待失效 | ba268c4 |
| D09 | P1 | IsolatedAdapter 把非法 artifact 强转文本或吞掉空列表，绕过输出校验 | 75c7966 |
| D10 | P1 | SQLite 认领竞争可把已完成 run 重新置 running，重复完成事件 | e576bf7 |
| D11 | P1 | 完整 stage 目标遇到零价值节点时停止派发，挂起至 deadline | e829001 |
| D12 | P1 | bridge 读取 body 前检查容量，慢上传并发完成可突破 maxConcurrent | 59aa4e7 |
| D13 | P1 | bridge 为不完整上游流生成成功结束，且强转/伪造 usage | ca1e626 |
| D14 | P1 | 并发 ModelVerifier 串用共享最后响应，且可漏计/重估 verifier 成本 | 54f09cf |
| D15 | P1 | 验证失败/验证传输异常丢失已知用量，预算支出被记为零 | 5b1c3eb |
| D16 | P1 | 调度器按观察时刻判断 node timeout，误杀已及时完成的 worker | 57639b9 |
| D17 | P1 | 并发新库初始化 WAL 立即 locked；初始化异常泄漏数据库句柄 | 2c772e0 |
| D18 | P1 | 畸形/超大计划泄漏类型、数值、读文件异常，布尔版本被误接受 | 03f48cb |
| D19 | P1 | 通用 CLI 沿用 FinQA 数值提取器，合法非数字纯文本答案被拒绝 | 9c4d141 |
| D20 | P1 | 合法深 DAG 与长环均触发 Python 递归溢出，绕过正常校验 | cfe3c07 |
| D21 | P1 | 路由分别判断预算与容量，忙碌便宜路由被误判为永久不可用 | d2d1474 |
| D22 | P1 | 模型验证费用把缺失/字符串/负数 worker usage 相加成合法用量 | 5b7bd0d |
| D23 | P1 | provenance 名称解析忽略当前节点优先级且错误拆分 `/` | fac9855 |
| D24 | P1 | provenance 的 cites 边终点与 produced artifact 顶点完全脱节 | a0c5efb |
| D25 | P1 | 后续 SSE usage 可回退计数或忽略非法更新，导致漏记已报告用量 | b91cf03 |
| D26 | P1 | Python splitlines 把合法 Unicode 正文拆成协议行，拒绝有效 SSE | c257cfc |
| D27 | P1 | 名称别名遮蔽内容 SHA 引用，错误来源或歧义绑定可被验证通过 | e93acd4 |
| D28 | P1 | HTTP adapter 将亚秒 node lease 强制延长为 1 秒，违反超时策略 | 11ac414 |
| D29 | P1 | process.start 失败绕过 finally，泄漏 IPC 且错误计入 started | 3f8098d |
| D30 | P1 | kill/join 后仍存活的 worker 被静默忽略，后续可继续突破物理并发 | c27312f |
| D31 | P2 | 子进程结果管道 OS 错误未统一为结构化 crash 失败 | 1e985c1 |
| D32 | P1 | artifact 校验/存储或隔离序列化失败抹掉已完成结果的有效费用 | 9774b49 |
| D33 | P1 | timeout/活跃 owner 取消可丢失已返回但尚未观察的 worker 费用 | 80b7ffe |
| D34 | P2 | execute 首次与终态重复读取返回不同事件序列 | 0e1d22e |
| D35 | P1 | 深层/不可表示 envelope 的错误记录再次抛异常，或隔离边界丢失费用 | 4cedc6c |
| D36 | P2 | dry-run 接受非有限 timeout，非法单价错误分类与真实模式不一致 | 82ae81b |
| D37 | P1 | verifier JSON 深度/整数限制与 score 浮点溢出变成异常并漏计费用 | 900e495 |
| D38 | P1 | 完整 HTTP 响应的拒绝/处理异常丢失已报告用量 | d2d9e93 |
| D39 | P0 | 未知高版本 SQLite 被当作旧库重建，丢弃未知 artifact 数据并降级版本 | 9940292 |
| D40 | P0 | 旧 artifact 迁移用 INSERT OR IGNORE 静默丢弃无主/重复/NULL 记录 | cddfa7a |
| D41 | P1 | Queue/Event/Process 初始化失败漏清理已创建 IPC | b063a88 |
| D42 | P1 | 验证器普通异常丢失已完成 worker 费用 | f16c74c |
| D43 | P1 | 超大整数使 envelope、控制 API、HTTP/隔离参数校验泄漏 OverflowError | b6d5e7a |
| D44 | P1 | stage 汇总溢出使增量门控提前满足目标 | 85a3cb8 |
| D45 | P0 | CLI --output 可覆盖输入计划、硬链接或运行时 SQLite/artifact 状态 | cf0e6b0 |
| D46 | P0 | 报告半写失败截断旧报告，写硬链接会改掉其他证据文件 | ab5dbca |
| D47 | P1 | 节点 timeout 只用于心跳，实际 HTTP socket 仍等待更长全局 timeout | 7b6e61b |
| D48 | P1 | ModelVerifier 继续使用完整节点额度，甚至在 worker 用尽额度后付费调用 | 0383166 |
| D49 | P1 | 显式 falsey 验证器/提取器被默认组件替换，绕过拒绝策略 | c059011 |
| D50 | P1 | Unicode 数学负号被数值比较/提取丢弃，负数被当作正数 | 9ba0187 |
| D51 | P1 | FinQA oracle 拒绝程序自身的科学计数法输出与合法 float 标签 | 846ee48 |
| D52 | P1 | Decimal 下溢静默归零，运算或显示容差可错误匹配零 | 398efdd |
| D53 | P1 | FinQA 继承调用者 Decimal 上下文，轨迹变化且 exe_ans 误匹配 | b60df3b |
| D54 | P2 | ModelVerifier 零/负数/非有限/错误类型配置被忽略或延迟到付费调用后失败 | 7966199 |
| D55 | P2 | ModelVerifier.system_prompt 被保存但从未用于请求 | e48338f |
| D56 | P1 | bridge 静默忽略/强转非法 system、token、temperature 与 stream 参数 | 9cffc30 |
| D57 | P1 | 完整空文本 SSE 拒绝时丢失有效 input/output usage | 31cb160 |
| D58 | P1 | bridge 把非法 UTF-8 请求字节替换后发给模型，改变 prompt | 43817c0 |
| D59 | P1 | bridge/HTTP 漏掉独立缓存输入 token，预算费用偏低 | b787157 |
| D60 | P1 | 线程池 submit 失败回滚已运行任务状态，并可留下无主调用 | 4c4fa6a |
| D61 | P1 | bridge 丢弃坏 chunk/delta，仍伪装成完整成功流 | a56a90a |
| D62 | P1 | HTTP 整数上限接受小数并截断，超大 response 上限在派发后才失败 | 4abd7e4 |
| D63 | P0 | 旧表预检查后并发插入未知列正文，迁移仍重建并丢弃数据 | 558b25e |
| D64 | P0 | 旧 artifact 重建静默丢弃额外元数据列 | 4d15578 |
| D65 | P1 | 隔离层收到结果后清理失败，丢失已知费用和调用记录 | c3ad13d |
| D66 | P1 | 数值字面量被千分位/前导小数点拆开，证据误通过且提取错误 | 237e914 |
| D67 | P2 | Python API 的合法有限整数配置误用 SQLite INTEGER 绑定而溢出 | 56ad878 |
| D68 | P2 | 并发追加使验证汇总不一致，返回的 CLI 调用列表继续变化 | 6dd662e |
| D69 | P2 | JSON 递归/整数限制绕过文本回退，坏 SSE 被错归为 response 错误 | 4d01829 |
| D70 | P1 | bridge 后续 usage 覆盖非法/回退计数，并丢弃先前缓存输入 | cce912e |
| D71 | P1 | bridge 将尚未执行的工具请求附带文字伪装成完成答案 | f2e591a |
| D72 | P1 | bridge 文档的 CLI --api-key 无法解析，HTTP 错误可回显配置键 | 1a8bf32 |
| D73 | P2 | HTTPError 只读摘要而不关闭响应，异常链继续持有连接 | 49b53a9 |
| D74 | P2 | 仓库根目录安装命令、能力状态和证据描述过时或互相矛盾 | 90b8362 |
| D75 | P1 | 非法 API key 经 urllib 普通异常回显到节点错误和调用报告 | b627d6c |
| D76 | P2 | provenance 全 run 名称碰撞及引用范围与直接依赖上下文不一致 | a01ce9a |
| D77 | P2 | schema 预检查后并发升级仍被初始化降回版本 3 | 2cdfce8 |
| D78 | P1 | CLI 工作目录/SQLite 初始化失败越过结构化报告，抛异常退出 | 3f1ed1e |
| D79 | P1 | HTTP 错误正文中 JSON 转义的鉴权键绕过字面脱敏 | b5c220a |
| D80 | P2 | 派发前 artifact 初始化 OS 错误仍绕过 CLI 的 state/2 报告 | 7160d50 |
| D81 | P2 | 深层损坏的已存 envelope 使只读审计抛 RecursionError，阻断其他节点读取 | 本次提交 |

## 模块检查覆盖

| 模块 | 状态 | 主要证据 |
|---|---|---|
| DAG、依赖与四波次 | 已检查 | D01/D04/D10/D11/D20/D21/D44，深 DAG 与门控回归 |
| SQLite、事务与租约 | 已检查 | D17/D33/D39/D40/D63/D64/D67/D77，真实连接、迁移保留与 schema 竞态 |
| 并发、429、进程隔离 | 已检查 | D08/D09/D12/D16/D28–D31/D41/D47/D60/D65，物理上界与故障注入 |
| token、成本与预算 | 已检查 | D14/D15/D22/D25/D32/D38/D48/D57/D59/D70，失败费用与累计用量 |
| EvidenceVerifier | 已检查 | D07/D23/D24/D27/D42/D49–D55/D66，来源、数值与模型验证边界 |
| CLI、参数与退出码 | 已检查 | D01/D18/D19/D36/D45/D46/D68/D72/D78，0/1/2、状态初始化与原子报告 |
| HTTP/DSH 适配器 | 已检查 | D05/D06/D13/D26/D37/D56/D58/D61/D62/D69/D71/D73/D75/D79，协议和凭据保护 |
| 异常与事件完整性 | 已检查 | D29–D35/D41/D60/D65，失败传播、费用、稳定事件与资源清理 |

八项均已检查；这是模块覆盖 8/8，不是逐行或穷尽缺陷的覆盖率声明。

## 进度评估与假设

- 约两小时（02:52）：34 个修复，D35 回归中；最新 235 项通过。
- 约四小时（04:52）：71 个修复，6 个 P0；Python 294/294、Node 73/73。
- 已确认的输入、来源、数值、取消/费用、迁移与协议假设均形成上述修复和回归。
- 已确认的缺陷均已形成失败测试和修复；当前无未处理的已确认问题，等待剩余独立复审。

## 三轮独立 Coder Review

1. Correctness：90b8362 上 Standards 发现 P1 1 项、Spec 发现 P1 1 项与 P2 1 项（P1 为同一凭据问题）；D75/D76 修复后，新的独立 Standards/Spec 在 a01ce9a 均输出 No issues found.（各 0 项）。
2. Regression：a01ce9a 上两个独立轴均发现同一 P2 schema 竞态；D77 修复后，2cdfce8 的新 Standards/Spec 均为 0 项。主代理另实证并逐项修复 D78/D79；b5c220a 上再次采用新的独立 Standards/Spec，均输出 No issues found.（各 0 项）；306 项完整测试再次通过。
3. Consistency：b5c220a 上 Standards 为 0 项，Spec 发现 P2 的 artifact 初始化边界，另指出基线审计解析缺口。D80 仅把派发前 OS 失败转为 state/2，保留执行期 I/O 传播；D81 将深层 JSON 解析错误逐行报告，其他节点审计不受影响。两项均先失败复现、逐项完整回归，最新 309 项通过。Fixed 2 issue(s). Ready for another review. 待新的独立复审。

每轮有发现则修复并重跑该轮；仅在输出 No issues found. 后进入下一轮。

## 未解决与未验证

- 当前无环境阻塞。Git push --dry-run 已成功，远程 main 仍为基线；最终权限与分支保护以实际 push 为准。
- 真实 DSH Host 注入/激活、真实模型与独立科学验证未在本次离线调试中执行；未改 profile、未重启 Host。
- 归档 Gate C/D 是 2026-10-04 指定端点的观测，本次未重跑，不据此声明当前容量、配额或 SLA。
- 线程路径保持协作式超时和 at-least-once；迟到或协议失败中未取得的 provider 费用不猜测。统一 token 单价不等于 provider 的分项价格账单。
- 按 kind/retryable 区分重试、受控资源池、跨主机调度和独立科学 verifier 保留为后续设计工作。
- 两个审查脚本首次自检的 OS 临时目录清理被自动审批拒绝（blocked by policy），已保留且不重试；只含本次离线虚拟测试状态，不影响仓库或发布。

## 提交与交付

- 每项独立 fix 提交并带 Signed-off-by: Codex；查看 git log 6be4db1..HEAD。
- 最终三轮审查、最新上游同步、完整测试、直接 push、最终 commit 与耗时：待记录。
- 若实际 push 被拒绝，停止，不 force push 或绕过保护；输出完整提交清单、DEBUG_STATE.md 与 debug-fixes.patch。
