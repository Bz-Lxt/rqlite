# 二进制备份断点续传与会话校验 实现计划

## 需求回顾

大体积二进制备份（`GET /db/backup`）目前一次性流式传输，中断后只能从头重来；且非 vacuum 二进制备份的一致性仅靠请求期间临时持有 `snapshotCAS` 锁定实时 db 文件实现，请求结束锁即释放，续传无法确认后续字节属于同一数据库状态。要求：

1. 中断后可安全续传（本地节点或经跟随节点转发到 leader 均可）；
2. 客户端能确认续传片段属于同一数据库状态（raft 状态位点 + 全量校验和）；
3. 端到端完整性校验（SHA-256 + 总大小）；
4. 失效/不匹配会话被明确拒绝（明确的 HTTP 错误码）；
5. 会话资源（临时文件、句柄、goroutine）及时回收（TTL + 空闲淘汰 + 显式释放 + 关闭清理 + 启动清扫）。

## Repository Research

### 现有备份链路

- HTTP 入口：[service.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/http/service.go#L657-L703) `handleBackup` → `s.proxy.Backup`。
- 转发层：[proxy.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/proxy/proxy.go#L202-L225) 先试本地 `Store.Backup`，遇 `ErrNotLeader` 经 `Cluster.Backup` 转发 leader（跟随节点透明代理）。
- 集群协议：`COMMAND_TYPE_BACKUP_STREAM`（[service.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/cluster/service.go#L521-L548)）先发长度前缀的 protobuf 响应头，再直接在 TCP conn 上流式写字节；[client.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/cluster/client.go#L289-L345) `Backup` 读取。
- 本地实现：[store.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/store/store.go#L1758-L1858) `Store.Backup`：
  - non-vacuum binary：必要时先 `Snapshot(0)` 落 WAL，持 `snapshotCAS` 阻止快照，直接 `io.Copy` 实时主库文件；
  - vacuum binary：用 SQLite online backup 生成 scratch 临时文件（`createTemp` + `rqlite-backup-*`）；
  - 启动时 [store.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/store/store.go#L817-L832) 会 glob 删除 `rqlite-backup-*` 等残留 scratch 文件。
- 状态位点可用：`Store.dbAppliedIdx`（`DBAppliedIndex()`，实际改变数据库的最新 raft index）/`AppliedIndex()`。
- CLI：[backup.go](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/cmd/rqlite/backup.go#L33-L60) `backup()` 单次 GET + `io.Copy` 覆盖写文件，无 Range、无校验、无状态保留。
- proto：[command.proto](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/command/proto/command.proto#L101-L113) `BackupRequest`；[message.proto](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/40627672-9b00-464c-951d-7df88603587a/repo/cluster/proto/message.proto#L19-L53) `Command` oneof，目前 type 最大到 13、oneof 序号最大到 13。
- 工具链：`protoc` 23.4 已装；`protoc-gen-go` 在 `$GOPATH/bin`（v1.36.6，现有文件由 v1.36.12 生成，模块缓存中存在 v1.36.12，可离线安装精确版本）。
- 自动备份（auto/backup uploader）直接本地调用 `Store.Backup`，本次改动不涉及其路径。

### 核心设计决策

普通 Range 直接作用于实时库文件不安全（CAS 释放后文件会变）。因此引入**服务端会话**：创建会话时在某一 raft 应用位点生成一份**不可变制品临时文件**（non-vacuum 路径短暂持 CAS 复制成临时文件后立即释放；vacuum 路径沿用现有临时文件生成），计算 `size + SHA-256 + raft_index`；此后所有续传读都对该不可变文件 `ReadAt`，与实时写入/快照完全解耦。会话只服务 BINARY 格式（需求明确为二进制备份；SQL dump 路径保持原样）。

## Files and Modules

### 协议层

- `command/proto/command.proto` / 重新生成 `command.pb.go`：新增
  - `message BackupSessionRequest { enum Operation {CREATE=0; READ=1; RELEASE=2;}; Operation op=1; BackupRequest backup_request=2; string session_id=3; int64 offset=4; int64 length=5; }`
  - `message BackupSessionInfo { string session_id=1; int64 size=2; string checksum=3; uint64 raft_index=4; }`（checksum 为完整制品的 sha256 hex）
  - `message BackupSessionResponse { string error=1; BackupSessionInfo info=2; int64 written=3; }`
- `cluster/proto/message.proto` / 重新生成 `message.pb.go`：
  - 枚举 `COMMAND_TYPE_BACKUP_SESSION = 14`；
  - oneof 增加 `command.BackupSessionRequest backup_session_request = 14;`；
  - `message CommandBackupSessionResponse { string error=1; command.BackupSessionInfo info=2; int64 written=3; }`。

### Store 层（会话管理器）

- 新增 `store/backup_session.go`：
  - 类型 `backupSession`（id、制品路径、size、checksum、raftIndex、vacuum、createdAt、lastAccess（用现有 `rsync.AtomicTime` 或 mutex 保护）、在途读计数、关闭标志）；
  - `Store` 增加字段 `backupSessions map[string]*backupSession`、`backupSessionMu sync.RWMutex`、`backupSessionStopCh chan struct{}`、`backupSessionDoneCh chan struct{}`；
  - 常量：`backupSessionScratchPattern = "rqlite-backup-session-*"`、绝对 TTL 30min、空闲超时 10min、reaper 间隔 1min、最大会话数 64、默认分块由客户端决定（服务端不限）；
  - 哨兵错误 `ErrBackupSessionNotFound`、`ErrBackupSessionInvalidRange`、`ErrBackupSessionLimit`；
  - 统一入口 `BackupSession(ctx, bsr *proto.BackupSessionRequest, dst io.Writer) (*proto.BackupSessionInfo, int64, error)`，按 op 分派：
    - `create`：仅接受 BINARY（否则 `ErrInvalidBackupFormat`）；`br.Leader` 时校验 leader；生成制品临时文件（复用现有 non-vacuum/vacuum 逻辑抽出内部 helper `backupToTempFile(ctx, br) (*os.File, uint64, error)`，vacuum 走 `s.db.Backup(tmp, true)`；non-vacuum 持 snapshotCAS 复制主库文件到临时文件后立即 End）；计算 sha256 与大小；登记会话（超限时淘汰最久未使用且无在途读者，仍不足返回 `ErrBackupSessionLimit`）；返回 info；
    - `read`：查会话（不存在 → `ErrBackupSessionNotFound`），`http.ParseRange` 同款规则校验 `[offset,length)`（length≤0 表示到末尾；越界 → `ErrBackupSessionInvalidRange`），在途读计数 +1，`ReadAt` 区间写入 dst，更新 lastAccess；
    - `release`：幂等删除会话并 `os.Remove` 制品。
  - reaper goroutine：周期扫描，回收过 TTL 或过空闲超时且无在途读者的会话；
  - `backupSessionsCloseAll()`：Close 时停止 reaper 并回收全部会话。
- `store/store.go`：
  - Open 成功路径初始化 map 与 channel 并启动 reaper；scratch 清理列表加入 `backupSessionScratchPattern`；
  - `Close(wait)` 中调用 `backupSessionsCloseAll()`（放在关闭 db 之前，避免文件句柄竞争）。

### 集群层

- `cluster/service.go`：
  - `Database` 接口增加 `BackupSession(ctx context.Context, bsr *command.BackupSessionRequest, dst io.Writer) (*command.BackupSessionInfo, int64, error)`；
  - 新增 `COMMAND_TYPE_BACKUP_SESSION` case：权限检查沿用 `auth.PermBackup`；CREATE/RELEASE 走 protobuf 一发一答；READ 先写 `CommandBackupSessionResponse` 响应头（含 info/error/written），error 为空时把区间原始字节直接写入 conn（字节数有界 = written；follower 按 written 精确 `CopyN`）。
- `cluster/client.go`：新增
  `BackupSession(ctx, bsr *command.BackupSessionRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration, w io.Writer) (*command.BackupSessionInfo, int64, error)`
  复用 `dial/writeCommand/readResponse`；READ 时 `io.CopyN(w, conn, written)`；不启用 gzip（有界原始字节，简化协议）。

### Proxy 层

- `proxy/proxy.go`：`Store`/`Cluster` 接口各增加 `BackupSession` 方法；新增
  `func (p *Proxy) BackupSession(ctx, bsr, dst, creds, timeout, noForward) (*proto.BackupSessionInfo, string, int64, error)`，
  本地返回 `ErrNotLeader` 时转发 leader（与现有 Backup 同模式，统计 remote_backups 或新增计数）。

### HTTP 层

- `http/query_params.go`：增加 `Session() string`（读取 `session` 查询参数）。
- `http/service.go`：
  - 新增响应头常量：`X-RQLITE-BACKUP-SESSION`、`X-RQLITE-BACKUP-SIZE`、`X-RQLITE-BACKUP-CHECKSUM`（值 `sha256:<hex>`）、`X-RQLITE-BACKUP-RAFT-INDEX`；
  - `handleBackup` 最前面分流：`qp.Session() != ""` → `handleBackupSession`，旧逻辑（无 session 参数）字节级不变；
  - `handleBackupSession`：
    - `session=new` + GET：透传 `br`（fmt=binary，其余参数照旧）调 `proxy.BackupSession` CREATE，成功 `201 Created`、空 body、设置上述 4 个头与 `Accept-Ranges: bytes`、`ServedByHTTPHeader`；
    - `session=<id>` + GET：解析 `Range` 头（`http.ParseRange`，无 Range 视为 bytes=0- 全量，返回 200）；可选 `If-Match: "<hex>"` 携带期望 checksum，不匹配 → `409 Conflict`；调用 READ；成功 206 + `Content-Range`/`Content-Length`/`Accept-Ranges` + 4 个信息头；错误映射：会话不存在/过期 → 404（body 明确 "backup session not found or expired"）、非法 Range → 416、格式错误 → 400、未授权 → 401、ErrNotLeader/LeaderNotFound 沿用现有 redirect/503 处理；
    - `session=<id>` + DELETE：RELEASE 成功 → 204；不存在 → 404；其他方法 → 405；
  - 所有会话操作复用 `CheckRequestPerm(r, auth.PermBackup)` 与 `qp.Timeout(defaultTimeout)`、`qp.Redirect()`。

### CLI 层

- `cmd/rqlite/http/client.go`：导出 `DoRequest(method, u string, body io.Reader, headers http.Header)`（包装现有未导出 `doRequest`，获得 host fallback、basic auth、redirect 能力）。
- `cmd/rqlite/backup.go`：binary `.backup FILE` 改为会话化续传（dump 函数不变）：
  - 侧车文件 `<FILE>.rqlite-backup`，JSON：`{id,size,checksum,raft_index}`；
  - 有侧车：用其 id 从本地文件当前字节数 n 发 Range READ；206 且响应头 size/checksum/raft-index 与侧车一致 → 边追加边流式 SHA-256；404 → 重新 CREATE，若新会话 checksum+size 与侧车相同则保留部分续传（字节内容必然一致），不同则截断重来；409 → CREATE 新会话并截断；
  - 无侧车：CREATE → 写侧车 → 从 0 分块（32 MiB/请求）Range 循环下载，流式 SHA-256；
  - 完成：sha256 与服务端 checksum 比对（不一致 → 报错、保留现场供重试）、校验大小；成功后 DELETE 会话、删侧车；中断/网络错误保留文件与侧车（会话靠服务端 TTL 兜底），下次执行同名命令自动续传；
  - 旧服务器兼容：`session=new` 收到 200（旧服务器忽略未知参数返回全量）→ 退化整文件流式写入、不生成侧车；READ 收到非 206 → 按全量重建处理。

## Implementation Steps

1. 安装精确版 protoc-gen-go（`GOPROXY=off go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12`），修改两个 `.proto` 并 `go generate ./command/proto/ ./cluster/proto/`，diff 确认仅新增内容。
2. 新增 `store/backup_session.go`（会话类型、管理器、三个 op、reaper、错误），在 `store.go` 完成 Open/Close/启动清扫接线。
3. `cluster`：扩展 `Database` 接口、service case、client 方法。
4. `proxy`：接口与 `BackupSession` 转发方法。
5. `http`：query param、常量、`handleBackupSession`、错误码映射。
6. `cmd/rqlite`：导出 DoRequest、重写 binary backup 续传流程与侧车逻辑。
7. 编写测试（见 Validation）。
8. `go fmt`、`go vet`、`go build ./...`、目标包测试与 race 测试。

## Dependencies and Considerations

- 会话制品为全量 DB 大小的临时文件；磁盘占用通过最大会话数（64）、TTL（30min）、空闲淘汰（10min）、Close 回收、启动 glob 清扫、创建超限先淘汰控制；本次不新增配置项（常量实现，保持聚焦），后续可配置化。
- leader 切换后旧会话消失：客户端收到 404 自动重建；checksum 相同可复用已下载字节，不同则作废重来——安全性由 sha256 保证。
- READ 经过集群链路不压缩（有界原始字节）；HTTP 会话响应不提供 gzip（Range 与 Content-Encoding 语义冲突），需要压缩的客户端继续走无 session 的旧接口。
- CREATE 时 non-vacuum 持 CAS 的时间从"整个备份传输时长"缩短为"复制一次文件的时长"，复制完成立即释放，反而降低对 raft 快照的阻塞。
- 向后兼容：旧节点忽略未知 query 参数，CLI 按状态码退化到全量模式。
- 自动备份/恢复、SQL dump 路径行为不变。

## Validation

- `store/backup_session_test.go`：create/read（偏移、部分长度、到末尾）/非法 Range/未知 id/release 幂等/TTL 与空闲回收/上限淘汰/Close 清理/非 BINARY 被拒/leader 要求/sha256 与直接 Backup 结果一致。
- `cluster`：在现有 real-store 测试夹具上覆盖三个 op 的服务端处理与 client 往返（含偏移读、未知会话 error、written 边界）。
- `proxy`：mock Store/Cluster 覆盖本地路径、ErrNotLeader 转发路径与 unauthorized 包装。
- `http/service_test.go`：201 及信息头、206+Content-Range、checksum 不匹配 409、未知/过期会话 404、416、DELETE 204、无 Range 全量 200、无 session 旧行为回归、多节点跟随节点转发续传。
- `cmd/rqlite`：httptest 模拟服务器验证全量下载、中断后续传字节一致、checksum 不匹配报错、404 重建（相同 checksum 复用/不同 checksum 截断）、旧服务器 200 退化。
- 全量验证：`go build ./... && go vet ./... && gofmt -l .`；`go test ./store ./cluster ./proxy ./http ./cmd/rqlite -count=1`；关键新测试 `go test -race -run BackupSession ./store ./cluster`。

## Risks

- proto 重新生成产生无关 diff：锁定 protoc-gen-go v1.36.12，生成后检查 git diff，仅保留新增 message/enum/oneof 相关内容。
- 临时文件泄漏（进程崩溃）：复用启动清扫机制，pattern 独立，重启自动清除。
- 大文件 sha256 计算增加一次磁盘读：CREATE 时复制与校验可合并为一次写入（io.MultiWriter 临时文件 + hash），无额外 I/O 往返。
- 在途读期间会话过期：在途计数使 reaper 跳过，读结束后下一轮回收。
