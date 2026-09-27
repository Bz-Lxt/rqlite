# 读写类别全链路并发与排队限制 - Independent Review

- [x] CP-1: 读写类别独立的并发与排队上限
  - **Type**: `rule`
  - **Covers**: AC-1（TR-1.1、TR-1.2、TR-1.5、TR-1.6、TR-4.4）
  - **Evidence**: R3 pass（见 Review R3）

- [x] CP-2: 统一请求混合内容按写入控制
  - **Type**: `rule`
  - **Covers**: AC-2（TR-2.1、TR-2.2、TR-3.2、TR-4.1）
  - **Evidence**: R3 pass（见 Review R3）

- [x] CP-3: 过载立即拒绝（HTTP 503）
  - **Type**: `rule`
  - **Covers**: AC-3（TR-3.1、TR-4.2）
  - **Evidence**: R3 pass（见 Review R3）

- [x] CP-4: 排队取消后不再执行且容量不泄漏
  - **Type**: `rule`
  - **Covers**: AC-4（TR-1.3、TR-1.4、TR-3.3、TR-4.3）
  - **Evidence**: R3 pass（见 Review R3）

- [x] CP-5: 转发后仍受执行节点限制
  - **Type**: `rule`
  - **Covers**: AC-5（TR-4.1..4.4、TR-3.4）
  - **Evidence**: R3 pass（见 Review R3）

- [x] CP-6: 未配置时行为不变
  - **Type**: `rule`
  - **Covers**: AC-6（TR-5.1、TR-5.2、TR-5.3、TR-6.1）
  - **Evidence**: R3 pass（见 Review R3）

- [x] CP-U1: 并发实现质量
  - **Type**: `rubric`
  - **Covers**: AC-7（TR-6.3）
  - **Scale**: 1-5
  - **Anchors**: 1 = 存在数据竞争、死锁或槽位泄漏；3 = 主路径正确但竞态/取消边界覆盖不足、缺少指标；5 = 锁粒度清晰无竞态，取消与提升竞态、FIFO 公平性、指标暴露均有测试与证据
  - **Pass Threshold**: >= 4
  - **Evidence**: R3 独立评分 **4/5**（见 Review R3）

## Review History

### Review R1
- **Result**: `fail`
- **Evidence**:
  - CP-1..CP-4 `pass`：三包（internal/limit、http、cluster）-race 全绿；Store Class 8 子用例通过；过载 503 与排队取消均有独立证据。
  - CP-5 `fail`：执行节点过载经转发回入口后丢失 sentinel 身份（cluster/client.go 以 `errors.New(a.Error)` 重建错误），http handler 无对应映射，客户端实际收到 HTTP 200 + 响应体 error，不符合 AC-5"503 经转发错误回传入口"。
  - CP-6 `fail`：零配置下 connCloseWatcher 对所有命令（含 nil limiter）安装，startWatch 无条件清除 deadline、stopWatch 无界等待下一命令首字节，导致命令间空闲的 30s connReadTimeout 被旁路（HEAD 二进制 30.00s 关闭 vs 新二进制 40s 仍开），行为/时序不一致。
  - CP-U1 `fail`，评审员独立评分 **3/5**：准入原语本身近 5 分水平，但集成层引入默认配置连接生命周期回归与转发过载状态码缺失，符合 anchor 3。
  - 另核实：system_test/Test_MultiNodeClusterRecoverSingle 失败为端口 127.0.0.1:10000 被无关用户应用 netdisk_s（PID 4075）占用，panic 发生在被测代码执行之前，与改动无关，不构成 blocked。
  - 2 条 advisory：半关闭带字节时流水线命令丢弃（rqlite 客户端不半关闭，影响极小）；watcher 安装范围应收窄到"真正排队"窗口。
- **路由**：回到 Implement，先在 tasks.md 将 2 条 actionable finding 物化为 pending issue（Issue I-1、I-2），修复并重新评审。

### Review R2
- **Result**: `fail`
- **Evidence**:
  - CP-1、CP-2、CP-3 `pass`；CP-6 `pass`：评审员独立以真实默认二进制实测空闲连接在 30.00s 关闭；4 flag 默认 0；构造函数签名未变。
  - I-1/I-2 修复本身经独立验证：TryAcquire、admit fast path、remoteError sentinel、三入口转发 503 均成立（三包 -race 全绿）。
  - CP-4、CP-5 `fail`：新发现 F-1（high）——admit 中 stopWatch 报告 closed 且 Acquire 返回 nil（提升与 FIN 竞态）时只 Release 槽位却仍返回 nil，调用方执行已放弃请求并二次 Release；无匹配 Release 可接收其他在途请求的 resting token，导致有效并发上限永久衰减（可至 0）或瞬时 N+1 执行，违反 AC-4/AC-5。
  - CP-U1 `fail`，独立评分 **3/5**：原语近 5 分，但集成 admit 的取消/提升边界存在可致槽位泄漏窗口且无测试，命中 anchor 3。
  - 环境性失败（端口 10000）再次独立核实与改动无关。2 条 advisory（cluster/http 持有期不对称、半关闭）。
- **路由**：回到 Implement，物化为 Issue I-3：admit closed+nil 时 Release 后返回 context.Canceled，并新增竞态压测回归。

### Review R3
- **Result**: `pass`
- **Evidence**（全新独立只读评审员，静态推理 + 定向测试 + 真实二进制端到端）:
  - CP-1..CP-6 全部 `pass`；CP-U1 `pass`，独立评分 **4/5**（扣分点：1 字节预读 + buf 回放 + deadline 中断机制理解成本较高）。
  - Issue I-3 闭环核实：admit 四分支穷举无遗漏——closed=true 必走 reconcileClose：err==nil → Release 一次并返回 context.Canceled；err!=nil（Acquire 内部已结算槽位）原样透传，禁止二次 Release；closed=false 时才可能返回 nil。成功路径 Acquire/Release 严格 1:1，无槽位衰减路径。
  - 测试：Test_ReconcileClose 与 Test_AdmitQueuedDisconnect（net.Pipe）确定性覆盖 F-1 分支与排队取消；Test_AdmitPromoteCloseRace 300 轮压测（276/300 为"提升先于 FIN 被观察到"的规约允许执行，closed=true 时零 db 调用），每轮 settle 后 N 槽全可 TryAcquire。
  - I-1/I-2 复核仍成立：真实默认二进制实测空闲连接 30.00s EOF；真实配置二进制验证 HTTP/转发共享配额、转发过载 0s 回传、三入口 503、取消后指标 num_queue_canceled=1 且后续写成功。
  - 验证命令：go build ./...、go vet ./...、gofmt -l 均 clean；./internal/limit/（1.38s）、./http/（30.4s）、./cluster/（43.9s）-race 全绿；store Test_StoreClass -race 8 子用例通过；全量非 race 48 包 EXIT=0。
  - 环境性失败（system_test 端口 10000 被 netdisk_s PID 4075 占用）第三次独立核实与改动无关。
  - 2 条非阻断 advisory 沿用：HTTP `?timeout` 不中断准入排队（与"不引入排队超时"决策一致）；客户端半关闭流水线字节场景（rqlite 客户端不半关闭）。
- **路由**：评审通过，特性交付。
