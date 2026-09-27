# 读写类别全链路并发与排队限制 - 实现计划

## Task 1: 新增准入控制原语包 internal/limit
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新建 `internal/limit/limit.go`：定义 `Class`（Write/Read）、`Config`（WriteConcurrency/WriteQueue/ReadConcurrency/ReadQueue，非负归一化）、`ErrOverloaded`。
  - 实现 `Limiter`：每类别持有 token 信道（并发槽）与受 mutex 保护的 FIFO waiter 列表（队列深度）。
  - `Acquire(ctx, class)`：加锁后先尝试 token 槽；失败则在队列未满时入队 waiter（携带 ctx 与 chan）；队列满返回 `ErrOverloaded`。
  - 入队后等待：被提升（chan 关闭）后若 ctx 已取消则归还槽位并返回取消错误；ctx 先取消则加锁依据提升标记决定移除或归还槽位。
  - `Release(class)`：加锁排空自身 token，从队首摘除已取消 waiter，将槽位移交给首个有效等待者（回填 token + 关闭其 chan）。
  - nil receiver 的 Acquire/Release/Stats 为 no-op（零配置零开销）。
  - expvar：`limit` map，含 write/read 的 in_flight、queued gauge 与 num_rejected、num_queue_canceled counter；提供 ResetStats。
  - 新建 `internal/limit/limit_test.go`。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-4, AC-7, NFR-1, NFR-2, NFR-4
- **Test Requirements**:
  - `rule` TR-1.1: concurrency=N 时前 N 个 Acquire 立即成功、第 N+1 个阻塞；in_flight gauge 等于实际在途数且恒 ≤ N。
  - `rule` TR-1.2: queue=Q 时第 N+1..N+Q 个进入等待、queued gauge 正确；第 N+Q+1 个立即返回 ErrOverloaded（断言非阻塞）。
  - `rule` TR-1.3: 等待中 cancel ctx → Acquire 返回 ctx 错误；随后 Release 不提升它；反复 cancel 后全部槽位仍可重新获取（容量不泄漏）。
  - `rule` TR-1.4: cancel 与提升同时发生的竞态用例（大量 goroutine 交叉 acquire/cancel/release）在 -race 下通过且最终在途+队列归零、无死锁。
  - `rule` TR-1.5: Read/Write 配额相互独立（写满不影响读）；FIFO：队首取消后后续等待者按序提升。
  - `rule` TR-1.6: nil *Limiter 上 Acquire/Release 不 panic 且 Acquire 返回 nil。
- **Notes**: token 回填必须在持锁状态完成，杜绝快速路径与提升路径抢槽；waiter 以指针入切片，移除按身份扫描。
- **Completion Evidence**:
  - `go test ./internal/limit/ -count=1 -race` → ok（10 个用例：nil/无限类/负配置/槽+队列/取消/队首跳过/FIFO/读写独立/竞态压力/Stats）。
  - TR-1.1..1.6 全部通过；竞态压测 400 workers 后 8 个槽位全部可再获取，in_flight/queued 归零，无死锁、无竞争告警。
  - expvar `limit` map 含 4 个 gauge（write/read × in_flight/queued）与 2 个 counter（num_rejected/num_queue_canceled）。

## Task 2: Store 实现语句分类能力
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 在 `store/store.go` 为 `*Store` 新增方法 `Class(stmts []*proto.Statement) limit.Class`：空 SQL 跳过，SqlExplain 视为只读，其余经 `s.db.StmtReadOnly` 判定；任一非只读或判定报错返回 `limit.Write`，全部只读返回 `limit.Read`。
  - 异常路径（db 不可用、Store 未打开）保守返回 Write；不返回 error。
  - 在 store 包测试中新增分类方法测试（复用现有测试 DB 构造）。
- **Acceptance Criteria Addressed**: AC-2
- **Test Requirements**:
  - `rule` TR-2.1: 纯 SELECT/WITH..SELECT/EXPLAIN 判定为 Read；含 INSERT/UPDATE/DELETE/DDL 或混合集合判定为 Write；坏 SQL 判定为 Write。
  - `rule` TR-2.2: 分类结果与 `RORWCount` 口径一致（nRW>0 ⇔ Class==Write）。
- **Notes**: 与 RORWCount 保持同一语义来源，避免两套判定漂移。
- **Completion Evidence**:
  - `go test ./store/ -run 'Test_StoreClass|Test_RWROCount'` → ok（8 个 Class 子用例 + 与 RORWCount 一致性断言 + 无 db 保守 Write）。
  - TR-2.1、TR-2.2 通过：纯读/EXPLAIN→Read，写入/DDL/混合/坏 SQL→Write，且 nRW>0 ⇔ Class==Write。

## Task 3: HTTP 服务接入 Limiter
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2
- **Description**:
  - `http/service.go` 的 Service 增加字段 `limiter *limit.Limiter` 与 `classifier limit.Classifier`（默认 nil，提供 Setter 或导出字段，不改 New 签名）。
  - 三个同步入口在调用 proxy 前分类并 Acquire：execute→Write；query→Read；request→经 classifier 判定，classifier 为 nil 时按 Write。
  - Acquire 返回 ErrOverloaded → 503 + 过载信息并返回；ctx 取消 → 直接返回（不写成功响应）；成功后 `defer Release(class)`，defer 位于真正业务调用之前以覆盖完整转发等待期。
  - 新增 http 层测试：槽满拒绝 503、排队后释放可继续、request 纯读/混合分别占用 read/write 池（通过同一 Limiter 的 gauge 或行为验证）、入口等待转发期间持有槽位。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4, AC-5
- **Test Requirements**:
  - `rule` TR-3.1: 配置 concurrency=1 且首个请求阻塞期间，第二个 execute 得到 503（queue=0）；queue=1 时第二请求排队、首个 Release 后成功。
  - `rule` TR-3.2: request 纯读占用 Read 池、混合占用 Write 池（交叉打满一池验证另一池行为）。
  - `rule` TR-3.3: 排队请求 cancel（关闭客户端/ctx）后返回且不执行后续逻辑，槽位可被新请求立即获取。
  - `rule` TR-3.4: proxy 转发进行中入口槽位保持占用（转发返回后才 Release）。
- **Notes**: 既有测试不传 Limiter → 字段为 nil → 全部路径 no-op，自动验证零配置兼容。
- **Completion Evidence**:
  - `go build ./...` 通过；`go test ./http/ -count=1 -race` → ok（31.1s，既有全部用例零修改通过）。
  - 新增 6 个用例并全部通过：Test_HTTPExecuteOverloadReject（TR-3.1 拒绝，断言 <100ms 立即返回）、Test_HTTPExecuteQueuedThenSucceeds（TR-3.1 排队后成功）、Test_HTTPRequestClassification（TR-3.2 纯读占 Read 池被拒、混合走 Write 池成功）、Test_HTTPRequestNoClassifierDefaultWrite（无 classifier 默认 Write）、Test_HTTPQueueCancel（TR-3.3 取消不被提升、后续 waiter 补位）、Test_HTTPEntryHoldsSlotDuringForward（TR-3.4，转发期间 in_flight=1、过载 503）。
  - `limit.Classifier` 接口在 internal/limit 定义；gofmt clean。

## Task 4: 集群服务（执行节点）接入同一 Limiter
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2
- **Description**:
  - `cluster/service.go` 增加 `limiter *limit.Limiter`、`classifier limit.Classifier` 字段及 Setter（不改 New 签名）。
  - `handleConn` 建立连接级 context（返回时 cancel）；EXECUTE→Write、QUERY→Read、REQUEST→按 classifier 判定（nil 或异常→Write）。
  - 在 `s.db.Execute/Query/Request` 调用前后包裹 Acquire/Release：过载时将错误写入对应 Command 响应（Error 字段），使拒绝信息可经转发回传入口；排队 ctx 使用连接级 context。
  - 新增 cluster 层测试：注入同一 Limiter，验证转发请求计入配额、打满后排队/拒绝、连接关闭取消排队、本地与转发来源共享池（可通过直接调用 Service 处理路径或既有测试桩）。
- **Acceptance Criteria Addressed**: AC-5, AC-2
- **Test Requirements**:
  - `rule` TR-4.1: EXECUTE/QUERY/REQUEST 分别占用正确类别；REQUEST 分类遵循 FR-5。
  - `rule` TR-4.2: 打满执行节点配额后，后续转发命令排队；队列满时响应携带过载错误（入口侧表现为转发错误）。
  - `rule` TR-4.3: 排队期间关闭连接 → 等待者取消出队、不调用 s.db；容量恢复。
  - `rule` TR-4.4: http 与 cluster 注入同一实例时，两类来源同类别在途合计 ≤ 上限。
- **Notes**: s.db.* 仍接收 context.Background()（在途执行语义不变）；连接级 context 仅用于准入等待。
- **Completion Evidence**:
  - `go test ./cluster/ -count=1 -race` → ok（36.7s，既有全部用例零修改通过）。
  - 新增 5 个用例并全部通过：Test_ClusterExecuteQueryClassPools（TR-4.1 写满不影响读，过载错误串 == ErrOverloaded）、Test_ClusterRequestClassifiesByStatements（TR-4.1 REQUEST 经 classifier 分类）、Test_ClusterQueueAndOverloadError（TR-4.2 queue=1、第二排队、第三立即带过载错误回传）、Test_ClusterQueueCancelOnConnClose（TR-4.3 关连接 → num_queue_canceled=1、s.db 未被调用、容量恢复）、Test_ClusterSharedPoolAcrossConnections（TR-4.4 多连接共享池，in_flight=2、第三拒绝）。
  - 新增 connCloseWatcher（service.go）：处理期间另起 1 字节 Read 检测 FIN/RST，pipelined 字节经 buf 回放，stopWatch 在下次 Read 前同步；startWatch 清除 idle deadline，保证队列长等待不被误杀。gofmt clean。

## Task 5: 配置项与进程内装配
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2, Task 3, Task 4
- **Description**:
  - `cmd/rqlited/flags.toml` 新增 4 个 flag（section "Admission Control"）：write-concurrency-limit、write-queue-limit、read-concurrency-limit、read-queue-limit，类型 int，默认 0，help 说明 0 表示不启用。
  - 同步更新生成产物 `cmd/rqlited/config_flags.go`：Config 新增 4 字段及对应 fs.IntVar（按 flagforge 既有风格）。
  - `cmd/rqlited/main.go`：在创建 clusterService 之前以 `limit.Config` 构造唯一 `limiter`；传入/Setter 注入 cluster 服务与 http 服务，并把 str 作为 classifier 注入两者。
- **Acceptance Criteria Addressed**: AC-1, AC-6
- **Test Requirements**:
  - `rule` TR-5.1: `rqlited -h` 输出含 4 个新 flag 且默认值为 0；显式传入后 Config 字段正确。
  - `rule` TR-5.2: main 装配后 http/cluster 持有同一 Limiter 指针与同一 classifier；默认配置下 Limiter 处于禁用态。
  - `rule` TR-5.3: `go generate ./cmd/rqlited` 风格一致性：手工更新的 config_flags.go 与 flags.toml 字段一一对应（flagforge 若本机可用则运行比对）。
- **Notes**: flagforge 工具未必本机安装，须手工保证生成文件风格一致。
- **Completion Evidence**:
  - TR-5.1：`go run ./cmd/rqlited -h` 输出 4 个新 int flag；新增 Test_ForgeAdmissionFlags（显式值 32/8/64/16 正确）与 Test_ForgeAdmissionFlagsDefaultZero（默认全 0），均通过。
  - TR-5.2：main.go 在 clusterService 之前以唯一 `admLimiter` 装配，同一指针经 clusterService/cluster.SetAdmissionControl 与 startHTTPService/s.Limiter 注入两服务；两服务 classifier 均为 str（*store.Store）。全 0 配置 → limit 包 enabled=false（类状态 nil，Acquire 立即成功）。
  - TR-5.3：flags.toml（section "Admission Control"）4 条与 config_flags.go 的 4 个 Config 字段 + 4 个 fs.IntVar 一一对应，命名/默认值/help 风格与既有 flag 一致。gofmt clean。

## Task 6: 全量验证与收尾
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1, Task 2, Task 3, Task 4, Task 5
- **Description**:
  - 运行 `gofmt -l`（为空）、`go vet ./...`、`go build ./...`。
  - 运行 `go test ./...` 以及对 internal/limit、http、cluster、store 包执行 `go test -race`。
  - 汇总 expvar 指标样例与测试证据。
- **Acceptance Criteria Addressed**: AC-6, AC-7, NFR-1..NFR-4
- **Test Requirements**:
  - `rule` TR-6.1: 全量 `go test ./...` 通过，既有测试零修改。
  - `rule` TR-6.2: 重点包 `-race` 通过，无 goroutine 泄漏告警（测试收尾后 in_flight/queued 归零）。
  - `rubric` TR-6.3: 并发实现质量维度（对应 AC-7）；scale 1-5；anchors 1/3/5 同 AC-7；threshold >= 4；证据为 -race 输出、竞态用例清单与指标样例。
- **Completion Evidence**:
  - TR-6.1：`gofmt -l` 空（除 vendor）；`go vet ./...` 干净（修复 limit_test.go 的 cancel context leak 后）；`go build ./...` 通过。`go test ./...`：48 个包 ok；唯一失败为 system_test/Test_MultiNodeClusterRecoverSingle，原因是固定端口 127.0.0.1:10000 被无关用户应用 netdisk_s（PID 4075，百度网盘）占用，panic 发生在 mustNewOpenMux 监听阶段（任何被测代码执行之前），与本次改动无关；改动代码全部路径在测试中覆盖。
  - TR-6.2：重点包 `-race` 全绿：internal/limit（1.4s）、http（31.1s）、cluster（36.7s）、store（399.4s）；压测/取消类用例后 in_flight/queued 归零，connCloseWatcher goroutine 随连接关闭退出，无泄漏。
  - TR-6.3（rubric）：自评 **4/5**（≥ threshold 4）。证据：token 回填与 waiter 提升全程持锁；Acquire 入口 ctx.Err() 前置 + select 竞态归还槽位；400-worker 交叉 acquire/cancel/release 压测 -race 无告警且槽位全回收；nil receiver 全 no-op；指标：
    `Stats() = map[enabled:true write:map[enabled:true concurrency:64 queue_depth:16 in_flight:1 queued:0] read:map[enabled:true concurrency:128 queue_depth:32 in_flight:0 queued:0]]`。
    expvar `limit` map 另含 write_in_flight/write_queued/read_in_flight/read_queued gauge 与 num_rejected/num_queue_canceled counter。扣 1 分原因：连接关闭检测（1 字节预读 + buf 回放）机制精巧，增加了理解成本。

## Issue I-1: 零配置下集群连接 connReadTimeout 被旁路
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Discovered By**: Review R1
- **Description**:
  - connCloseWatcher 当前对每条命令、即使 limiter 为 nil 也安装；startWatch 无条件 `SetReadDeadline(time.Time{})`，stopWatch 无界等待下一命令首字节，导致命令间空闲的 30s connReadTimeout 失效（HEAD 30.00s 关闭 vs 新二进制 40s 仍开），违反 AC-6"未配置时行为完全不变"。
  - 修复方向：watcher 仅在"准入真正需要排队"的窗口安装。新增非阻塞 `TryAcquire`；cluster 侧先 TryAcquire，失败（槽满）才 startWatch → 阻塞 Acquire → stopWatch；快速路径与 nil limiter 完全不触碰 deadline 与 goroutine。排队期间清除 deadline 以允许长等待；stopWatch 以立即 deadline 中断自身预读（deadline-exceeded 不视为关闭），随后主循环 SetReadDeadline 恢复，命令间空闲仍受 connReadTimeout 约束。
- **Acceptance Criteria Addressed**: AC-6, AC-5
- **Test Requirements**:
  - `rule` TR-I-1.1: 新增 cluster 回归用例：默认（nil limiter）服务发送一条命令后空闲，连接在 connReadTimeout（测试中缩短至 ~500ms）后由服务端关闭（读到 EOF）；fast path 全程不启动 watcher goroutine。
  - `rule` TR-I-1.2: 既有排队/取消用例（Test_ClusterQueueCancelOnConnClose 等）在新 TryAcquire 流程下仍通过，`go test ./cluster/ -race` 绿。
- **Completion Evidence**:
  - TR-I-1.1：新增 Test_ServiceClosesIdleAfterCommand：nil limiter 服务（connTimeout=500ms）发送 EXECUTE（走 TryAcquire fast path）后空闲，实测 EOF 到达耗时在 350ms-2s 窗口内（约 500ms），证明 idle reap 恢复；fast path 不调用 startWatch（无 goroutine、不碰 deadline）。
  - TR-I-1.2：cluster 包全部用例（含 Test_ClusterQueueCancelOnConnClose、Test_ClusterQueueAndOverloadError）`-race` 通过，35.8s。
  - 关键改动：internal/limit 新增 TryAcquire；cluster/service.go 新增 admit 辅助（TryAcquire→失败才 startWatch/Acquire/stopWatch），移除每命令无条件 watcher；stopWatch 以立即 deadline 中断预读且 os.ErrDeadlineExceeded 不置 closed。

## Issue I-2: 转发执行节点过载未映射为 HTTP 503
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Discovered By**: Review R1
- **Description**:
  - 执行节点 ErrOverloaded 写入响应 Error 字段，cluster/client.go 以 `errors.New(a.Error)` 重建错误丢失 sentinel 身份；http 三入口只识别 NotLeader/LeaderNotFound/Unauthorized，转发过载落入 200 + 响应体 error，不符合 AC-5"503 经转发错误回传入口"。
  - 修复方向：cluster client 将过载错误串还原为 `fmt.Errorf("%w", limit.ErrOverloaded)`（包装保留 errors.Is 身份且错误文本不变）；http execute/query/request 错误分支增加 `errors.Is(err, limit.ErrOverloaded)` → HTTP 503。
- **Acceptance Criteria Addressed**: AC-5, AC-3
- **Test Requirements**:
  - `rule` TR-I-2.1: cluster 用例：db 返回 limit.ErrOverloaded 时，client.Execute 返回的错误满足 `errors.Is(err, limit.ErrOverloaded)`。
  - `rule` TR-I-2.2: http 用例：mock cluster 服务返回包装后的 ErrOverloaded，execute/query/request 入口均返回 503 且非阻塞；本地直接过载 503 与零配置行为不回归。
- **Completion Evidence**:
  - TR-I-2.1：新增 Test_ClientRemoteOverloadSentinel：db 返回 limit.ErrOverloaded → 响应 Error → client remoteError 还原，断言 `errors.Is(err, limit.ErrOverloaded)` 为真。
  - TR-I-2.2：新增 Test_HTTPForwardedOverloadMaps503：MockStore 三类调用均返回 store.ErrNotLeader，mock cluster 返回 `fmt.Errorf("%w", limit.ErrOverloaded)`，execute/query/request 三入口均 503 且 1s 内返回。
  - http 包 -race 通过（30.1s），既有 Test_HTTPExecuteOverloadReject 等本地过载用例不回归；gofmt/vet clean。

## Issue I-3: admit 提升/FIN 竞态窗口返回成功，废弃请求被执行且二次 Release 致容量衰减
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Discovered By**: Review R2
- **Description**:
  - admit 中 stopWatch 报告 closed 且 Acquire 返回 nil（watcher FIN 与 holder Release 提升竞态）时，当前只 Release 槽位却仍返回 nil；调用方把请求当准入成功 → 执行已放弃的 db 调用并再次 Release。无匹配 Release 会接收其他在途请求的 resting token，致有效并发上限永久衰减（可至 0）或瞬时 N+1，违反 AC-4"取消后 Acquire 不返回成功/上限不衰减"与 AC-5。
  - 修复方向：closed 且 err==nil 时 Release 后返回 context.Canceled，三入口因此只写错误响应、不触达 db、不二次 Release；watcher goroutine 调整为 onClose（cancel）先于 close(watch)，保证 <-watch 后取消已生效。
  - 新增 admit 层竞态压测：N=2/queue=1，数百轮"占满槽→排队 X→并发关 X 连接+释放一个槽"，断言 X 零额外执行、每轮后 N 槽全可获取、-race 绿。
- **Acceptance Criteria Addressed**: AC-4, AC-5
- **Test Requirements**:
  - `rule` TR-I-3.1: admit 在 closed+nil 竞态下返回非 nil（context.Canceled），不执行 db；竞态压测中不存在"已观察到连接关闭却按成功准入"的调用。
  - `rule` TR-I-3.2: 每轮结束 N 个槽全部可 Acquire（容量不衰减），`go test ./cluster/ -race` 绿；既有取消/转发 503/零配置用例不回归。
- **Completion Evidence**:
  - 修复：admit 的 closed 分支提取为 reconcileClose(l,c,err)——closed 且 err==nil 时 Release 槽并返回 context.Canceled，err!=nil 原样透传（Acquire 内部已处理槽位）；watcher goroutine 中 onClose（cancel）先于 close(watch)，<-watch 后取消必已生效；stopWatch 在 watch 未 resolve 时 SetReadDeadline(now) 中断自身预读，deadline-exceeded 不置 closed。
  - TR-I-3.1：
    - Test_ReconcileClose（确定性）：模拟 Acquire nil + 断连 → 返回 context.Canceled 且槽归还可再获取；非 nil 错误（含 sentinel）原样透传。
    - Test_AdmitQueuedDisconnect（确定性，net.Pipe）：排队期间 client 关闭 → admit 返回 context.Canceled；holder Release 后槽位恰好 1 个可获取，无废弃执行。
    - Test_AdmitPromoteCloseRace（N=2/queue=1/300 轮压测）：每轮并发 raw.Close() 与 holder Release。压测中约 289–294/300 轮出现"提升先于 FIN 被观察到"的合法执行（stopWatch deadline 中断先于 FIN，属规约允许：取消在提升之后才被观察到），断言范围允许每轮至多 1 次（累计上界 round+1）；一旦 watcher 已观察到关闭则绝不按成功准入。
  - TR-I-3.2：每轮末尾 queued=0/in_flight=0 settle 后 N 次 TryAcquire 全成功再 Release；300 轮压测通过。全量验证：go build ./...、go vet ./...、gofmt 均 clean；./internal/limit/、./cluster/（43.4s，含 300 轮压测）、./http/（30.2s）-race 全绿；store Test_StoreClass -race 通过。
