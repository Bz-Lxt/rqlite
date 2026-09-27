# 读写类别全链路并发与排队限制 - 需求规格

## Overview
- **Summary**: 为 rqlite 增加按读（Read）/写（Write）类别独立生效的准入控制：每个类别可分别限制并发执行数与排队等待数；限制在请求全链路（HTTP 入口节点、转发、执行节点）上逐跳生效。
- **Purpose**: 突发慢查询与写入会耗尽 SQLite 数据库连接、Raft apply 通道以及节点转发资源，导致节点整体不可用。需要可预测的并发上限、有界排队、过载快速拒绝，并保证排队取消的请求不会被延迟执行。
- **Target Users**: rqlite 集群运维者与通过 HTTP API 发起读写请求的应用程序。

## Goals
- 读、写两类请求各自拥有独立的并发上限与排队上限，互不挤占。
- 统一请求（/db/request）按"就高不就低"分类：只要包含写入即按写类别控制。
- 过载（并发槽与队列均满）时立即拒绝，不阻塞调用方。
- 请求在排队期间被取消（超时或客户端断开）后立即出队，且永远不会被执行。
- 请求经入口节点转发到执行节点后，仍受执行节点同一套限制约束。
- 未配置任何限制时，系统行为与现状完全一致。

## Non-Goals
- 不改变既有 Queued Writes（`?queue` 异步批写，自带容量/批量/超时语义）；其 HTTP 入口与 runQueue 处理路径不纳入本次限制。
- 不做基于系统负载信号的自适应延迟（现有 `store/throttler` 机制保留，职责不同）。
- 不做跨节点的集群级全局配额；限制以节点为单位独立计算。
- 不改变 Raft 共识、一致性级别、SQL 改写等既有语义。
- 不纳入 backup/load/remove 等运维接口；仅覆盖同步的 execute、query、request 三类数据请求。
- 已获得执行槽、正在执行中的请求遇到客户端断开时，沿用现状（执行节点 cluster 侧目前以 `context.Background()` 驱动，不强制中断）。

## Background & Context
- 请求路径：
  - 写入：HTTP handler → `proxy.Execute`（先试本地 `store.Execute`，非 Leader 时经 `cluster.Client` 转发）→ Leader 的 `cluster.Service` → `store.Execute` → Raft apply → FSM 写 SQLite。
  - 读取：HTTP handler → `proxy.Query` → 本地读或转发 → `store.Query`（弱读/无一致性读直接命中 SQLite；strong 读走 Raft）。
  - 统一请求：`proxy.Request` / `store.Request`，内部已通过 `Store.RORWCount` 按 SQLite `sqlite3_stmt_readonly` 判定读写数量；含任一写入即走共识。
- 现有 `queue/queue.go` 是"批量+超时"的批处理队列，服务于 Queued Writes；`store/throttler` 是按信号对写入施加递增延迟的限速器。二者均不提供"按类别的并发槽 + 有界等待队列 + 过载拒绝"语义，本次新增独立原语。
- 关键装配点：`cmd/rqlited/main.go` 中 `clusterService`（约 L169）与 `startHTTPService`（约 L198）在同一进程内创建，可注入同一个 Limiter 实例，实现节点内来源无关的统一池。

## Functional Requirements
- **FR-1**: 新增准入控制原语包（`internal/limit`），提供 Read/Write 两个类别；每个类别可独立配置并发槽数（concurrency）与排队队列深度（queue depth）。
- **FR-2**: Acquire 语义：优先非阻塞占用并发槽；槽满后进入 FIFO 等待队列；队列亦满时立即返回 `ErrOverloaded`，不阻塞。Release 时释放槽位，并将其移交给队首仍然有效的等待者。
- **FR-3**: Acquire 接受 context；排队等待期间 context 取消时，等待者立即从队列移除并返回 context 错误。取消与提升之间的竞态必须闭环处理：已取消的等待者不会以"成功"返回，其占有的槽位（若已移交）必须立即归还，容量不泄漏。
- **FR-4**: HTTP 服务的三个同步入口（execute、query、request）在调用 proxy 之前 Acquire、响应结束后 Release；槽位持有期覆盖本地执行与整个转发等待，因此入口节点的转发资源（goroutine、集群连接）同样受上限约束。
- **FR-5**: 分类规则：
  - `/db/execute` → 恒为 Write；
  - `/db/query` → 恒为 Read；
  - `/db/request` → 经分类器判定：所有语句均可证明为只读时归 Read，否则（含任一写入、EXPLAIN 以外无法判定、分类器缺失或报错）归 Write。
  - 分类口径与 `Store.RORWCount` 一致（空 SQL 跳过；SqlExplain 视为只读；prepare 失败按写入处理）。
- **FR-6**: 集群服务（执行节点）在处理 EXECUTE/QUERY/REQUEST 命令时，使用与本节点 HTTP 服务**相同的 Limiter 实例**进行 Acquire/Release。排队等待所用 context 与连接生命周期绑定（连接关闭即取消）；分类同样遵循 FR-5。
- **FR-7**: rqlited 新增 4 个配置 flag，默认均为 0：写并发上限、写排队上限、读并发上限、读排队上限。main.go 据配置创建唯一 Limiter 并注入 http 与 cluster 两个服务；构造函数签名保持不变，以字段/Setter 注入。
- **FR-8**: 观测性：通过 expvar 暴露每类在途数、排队数（gauge）以及过载拒绝数、排队取消数（counter）。

## Non-Functional Requirements
- **NFR-1**: 仅使用 Go 标准库实现，不引入第三方依赖；未配置限制时快速路径为零开销的 no-op（nil 实例直接返回）。
- **NFR-2**: 并发安全，关键并发场景须在 `go test -race` 下通过。
- **NFR-3**: 变更须通过 `go vet ./...`、`gofmt`，且不破坏任何既有测试。
- **NFR-4**: 排队为 FIFO 公平顺序；取消的等待者不阻挡后续有效等待者。

## Constraints
- **Technical**: Go 1.26；保持单二进制、无外部依赖；`http.New`、`cluster.New` 被大量既有测试调用，不得变更其签名。
- **Business**: 升级后零配置即与历史版本行为完全一致，避免对现网造成意外影响。
- **Dependencies**: 复用 `command/proto` 的 Statement 类型与 db 层的只读判定能力，不新增依赖。

## Assumptions
- 每个节点进程内一个共享 Limiter 实例，HTTP 入口与集群入口共用，从而按类别形成节点级统一并发池。
- `/db/request` 的分类依赖 SQLite prepare（StmtReadOnly），与 RORWCount 同口径；在 Store 尚未打开等异常情况下保守归类为 Write。
- 排队取消的触发源为请求自身 context（`timeout` 参数、客户端断开导致的 r.Context 取消；转发场景为集群连接关闭），不引入独立排队超时配置。
- 过载拒绝映射为 HTTP 503 Service Unavailable，响应体包含明确错误信息。

## Acceptance Criteria

### AC-1: 读写类别独立的并发与排队上限
- **Type**: `rule`
- **Given**: Limiter 配置某类别（如 Write）concurrency=N、queue=Q
- **When**: 该类别请求进入
- **Then**: 前 N 个直接获得执行槽；其后至多 Q 个进入等待队列；其余立即收到 ErrOverloaded；两个类别的计数与队列相互独立
- **Pass Condition**: 并发在途数恒 ≤ N、排队数恒 ≤ Q；一类打满不影响另一类的配额
- **Evidence**: `internal/limit` 单元测试（计数断言、expvar gauge）

### AC-2: 统一请求混合内容按写入控制
- **Type**: `rule`
- **Given**: 一个 `/db/request` 请求
- **When**: 其语句集合包含任意写入语句
- **Then**: 该请求占用 Write 类别配额；全部语句可证明只读时占用 Read 配额；分类器缺失/判定异常时按 Write
- **Pass Condition**: 混合请求的获取与释放均发生在 Write 类别；纯读请求发生在 Read 类别
- **Evidence**: Store 分类方法单测 + HTTP 层请求分类测试（通过共享计数验证）

### AC-3: 过载立即拒绝
- **Type**: `rule`
- **Given**: 某类别并发槽与排队队列均已占满
- **When**: 同类别新请求到达（HTTP 入口或集群转发入口）
- **Then**: 不等待、不阻塞，立即返回错误；HTTP 响应状态码为 503 且含过载错误信息；拒绝计数递增
- **Pass Condition**: 拒绝调用的耗时为非阻塞级（无等待），状态码 503
- **Evidence**: HTTP 服务测试断言 503；Limiter 测试断言非阻塞返回 ErrOverloaded

### AC-4: 排队取消后不再执行且容量不泄漏
- **Type**: `rule`
- **Given**: 一个请求正处于准入排队等待中
- **When**: 其 context 在等待期间被取消
- **Then**: 请求立即出队并返回取消错误，之后即使有槽位释放也不会执行它；后续有效等待者可正常被提升
- **Pass Condition**: 取消后该请求 Acquire 不返回成功；槽位与队列容量全部可再用（反复取消后并发上限不衰减）
- **Evidence**: `internal/limit` 取消/竞态单测（含取消与提升同时发生、成批取消后补位），-race 下通过

### AC-5: 转发后仍受执行节点限制
- **Type**: `rule`
- **Given**: 入口节点将写/读请求转发到执行节点
- **When**: 请求到达执行节点的 cluster 服务
- **Then**: 执行节点以同一 Limiter 实例按类别执行准入；执行节点上"本地 HTTP 来源"与"转发来源"的同类别请求合计不超过该类别上限；入口节点在整个转发等待期间持续持有本地槽位
- **Pass Condition**: 打满执行节点配额后，转发请求被排队或拒绝（503 经转发错误回传入口）；本地与转发来源共享配额
- **Evidence**: cluster 服务层测试（注入同一 Limiter，混合来源计数）；入口持有期测试

### AC-6: 未配置时行为不变
- **Type**: `rule`
- **Given**: 四个新增配置项均为默认值 0
- **When**: 系统运行
- **Then**: Limiter 为禁用态（nil/no-op），Acquire/Release 零开销且不改变任何请求结果、状态码与时序；构造函数签名不变
- **Pass Condition**: 既有全部测试在不修改的前提下通过；新增 flag 默认值为 0
- **Evidence**: `go test ./...` 全量通过（含 http/cluster 既有测试零改动）

### AC-7: 并发实现质量
- **Type**: `rubric`
- **Dimension**: 并发正确性与可观测性
- **Scale**: 1-5
- **Anchors**: 1 = 存在数据竞争、死锁或槽位泄漏；3 = 主路径正确但竞态/取消边界覆盖不足、缺少指标；5 = 锁粒度清晰无竞态，取消与提升竞态、FIFO 公平性、指标暴露均有测试与证据
- **Pass Threshold**: >= 4
- **Evidence**: -race 测试结果、测试用例覆盖矩阵、expvar 指标样例

## Open Questions
- 无（已确认：Queued Writes 不纳入；排队取消沿用请求 context；过载返回 503）。
