// Package proxy provides leader-forwarding logic for rqlite operations.
// It tries a local store operation first and, if the store returns
// ErrNotLeader, transparently forwards the request to the current
// Raft leader via the cluster interface.
package proxy

import (
	"context"
	"errors"
	"expvar"
	"io"
	"sync"
	"time"

	clstrPB "github.com/rqlite/rqlite/v10/cluster/proto"
	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/store"
)

var (
	// ErrNotLeader is returned when noForward is true and the local
	// node is not the Raft leader. It is an alias for store.ErrNotLeader
	// so errors.Is matches no matter which layer created the error.
	ErrNotLeader = store.ErrNotLeader

	// ErrLeaderNotFound is returned when the leader address is unknown.
	// It is an alias for store.ErrLeaderNotFound.
	ErrLeaderNotFound = store.ErrLeaderNotFound

	// ErrUnauthorized is returned when the remote node rejects credentials.
	ErrUnauthorized = errors.New("unauthorized")
)

// stats captures stats for the HTTP service.
var stats *expvar.Map

const (
	numRemoteRemoveNode       = "remote_remove_node"
	numRemoteBackups          = "remote_backups"
	numRemoteLoads            = "remote_loads"
	numRemoteStepdowns        = "remote_stepdowns"
	numRemoteExecutions       = "remote_executions"
	numRemoteExecutionsFailed = "remote_executions_failed"
	numRemoteQueries          = "remote_queries"
	numRemoteQueriesFailed    = "remote_queries_failed"
	numRemoteRequests         = "remote_requests"
	numRemoteRequestsFailed   = "remote_requests_failed"
)

func init() {
	stats = expvar.NewMap("proxy")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numRemoteRemoveNode, 0)
	stats.Add(numRemoteBackups, 0)
	stats.Add(numRemoteLoads, 0)
	stats.Add(numRemoteStepdowns, 0)
	stats.Add(numRemoteExecutions, 0)
	stats.Add(numRemoteExecutionsFailed, 0)
	stats.Add(numRemoteQueries, 0)
	stats.Add(numRemoteQueriesFailed, 0)
	stats.Add(numRemoteRequests, 0)
	stats.Add(numRemoteRequestsFailed, 0)
}

// Store defines the local database operations needed by the proxy.
type Store interface {
	Execute(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error)
	Query(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error)
	Request(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error)
	Load(ctx context.Context, lr *proto.LoadRequest) error
	Backup(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error
	BackupSessionCreate(ctx context.Context, br *proto.BackupRequest) (*proto.BackupSessionInfo, error)
	BackupSessionPrepareRead(ctx context.Context, id string, offset, length int64) (*proto.BackupSessionInfo, int64, error)
	BackupSessionRead(ctx context.Context, id string, offset, length int64, dst io.Writer) (int64, error)
	BackupSessionRelease(ctx context.Context, id string) error
	Remove(ctx context.Context, rn *proto.RemoveNodeRequest) error
	Stepdown(wait bool, id string) error
	LeaderAddr() (string, error)
}

// Cluster defines operations for forwarding requests to the leader.
type Cluster interface {
	Execute(ctx context.Context, er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, error)
	Query(ctx context.Context, qr *proto.QueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.QueryRows, uint64, error)
	Request(ctx context.Context, eqr *proto.ExecuteQueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, uint64, error)
	Backup(ctx context.Context, br *proto.BackupRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) error
	BackupSession(ctx context.Context, bsr *proto.BackupSessionRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) (*proto.BackupSessionInfo, int64, error)
	Load(ctx context.Context, lr *proto.LoadRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) error
	RemoveNode(ctx context.Context, rn *proto.RemoveNodeRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error
	Stepdown(ctx context.Context, sr *proto.StepdownRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error
}

// Proxy handles try-local-then-forward-to-leader logic.
type Proxy struct {
	store   Store
	cluster Cluster

	mu      sync.RWMutex
	apiAddr string
}

// New returns a new Proxy instance.
func New(store Store, cluster Cluster) *Proxy {
	return &Proxy{
		store:   store,
		cluster: cluster,
	}
}

// SetAPIAddr sets the API address the proxy is aware of.
func (p *Proxy) SetAPIAddr(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.apiAddr = addr
}

// GetAPIAddr returns the previously-set API address.
func (p *Proxy) GetAPIAddr() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.apiAddr
}

// Execute executes statements that modify the database. If the local store
// returns ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Execute(ctx context.Context, er *proto.ExecuteRequest, creds *clstrPB.Credentials,
	timeout time.Duration, retries int, noForward bool) ([]*proto.ExecuteQueryResponse, uint64, string, error) {

	results, raftIndex, err := p.store.Execute(ctx, er)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return nil, 0, "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, 0, "", addrErr
		}
		results, raftIndex, err = p.cluster.Execute(ctx, er, addr, creds, timeout, retries)
		if err != nil {
			stats.Add(numRemoteExecutionsFailed, 1)
			return nil, 0, "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteExecutions, 1)
		return results, raftIndex, addr, nil
	}
	return results, raftIndex, p.GetAPIAddr(), err
}

// Query executes read-only queries. If the local store returns ErrNotLeader
// and noForward is false, the request is forwarded to the current leader.
// The ConsistencyLevel return value from Store.Query is dropped.
func (p *Proxy) Query(ctx context.Context, qr *proto.QueryRequest, creds *clstrPB.Credentials,
	timeout time.Duration, retries int, noForward bool) ([]*proto.QueryRows, uint64, string, error) {

	results, _, raftIndex, err := p.store.Query(ctx, qr)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return nil, 0, "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, 0, "", addrErr
		}
		results, raftIndex, err = p.cluster.Query(ctx, qr, addr, creds, timeout, retries)
		if err != nil {
			stats.Add(numRemoteQueriesFailed, 1)
			return nil, 0, "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteQueries, 1)
		return results, raftIndex, addr, nil
	}
	return results, raftIndex, p.GetAPIAddr(), err
}

// Request processes a unified execute-query request. If the local store
// returns ErrNotLeader and noForward is false, the request is forwarded
// to the current leader.
func (p *Proxy) Request(ctx context.Context, eqr *proto.ExecuteQueryRequest, creds *clstrPB.Credentials,
	timeout time.Duration, retries int, noForward bool) ([]*proto.ExecuteQueryResponse, uint64, uint64, string, error) {

	results, seq, raftIndex, err := p.store.Request(ctx, eqr)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return nil, 0, 0, "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, 0, 0, "", addrErr
		}
		results, seq, raftIndex, err = p.cluster.Request(ctx, eqr, addr, creds, timeout, retries)
		if err != nil {
			stats.Add(numRemoteRequestsFailed, 1)
			return nil, 0, 0, "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteRequests, 1)
		return results, seq, raftIndex, addr, nil
	}
	return results, seq, raftIndex, p.GetAPIAddr(), err
}

// Backup writes a backup of the database. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Backup(ctx context.Context, br *proto.BackupRequest, dst io.Writer, creds *clstrPB.Credentials,
	timeout time.Duration, noForward bool) (string, error) {

	err := p.store.Backup(ctx, br, dst)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return "", addrErr
		}
		err = p.cluster.Backup(ctx, br, addr, creds, timeout, dst)
		if err != nil {
			return "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteBackups, 1)
		return addr, nil
	}
	return p.GetAPIAddr(), err
}

// BackupSessionCreate creates a resumable backup session. If the local store
// returns ErrNotLeader and noForward is false, the request is forwarded to the
// current leader.
func (p *Proxy) BackupSessionCreate(ctx context.Context, br *proto.BackupRequest,
	creds *clstrPB.Credentials, timeout time.Duration, noForward bool) (*proto.BackupSessionInfo, string, error) {
	info, err := p.store.BackupSessionCreate(ctx, br)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return nil, "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, "", addrErr
		}
		bsr := &proto.BackupSessionRequest{
			Op:            proto.BackupSessionRequest_BACKUP_SESSION_OP_CREATE,
			BackupRequest: br,
		}
		info, _, err = p.cluster.BackupSession(ctx, bsr, addr, creds, timeout, nil)
		if err != nil {
			return nil, "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteBackups, 1)
		return info, addr, nil
	}
	return info, p.GetAPIAddr(), err
}

// BackupSessionPrepareRead validates a byte range of a backup session. When
// leaderOnly is true (the default for leader-backed backups), a session which
// is unknown to the local node is treated as "served by the leader" and, if
// noForward is false, forwarded there.
func (p *Proxy) BackupSessionPrepareRead(ctx context.Context, id string, offset, length int64,
	leaderOnly bool, creds *clstrPB.Credentials, timeout time.Duration,
	noForward bool) (*proto.BackupSessionInfo, int64, string, error) {
	info, n, err := p.store.BackupSessionPrepareRead(ctx, id, offset, length)
	if errors.Is(err, store.ErrNotLeader) || (leaderOnly && errors.Is(err, store.ErrBackupSessionNotFound)) {
		if noForward {
			return nil, 0, "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, 0, "", addrErr
		}
		bsr := &proto.BackupSessionRequest{
			Op:        proto.BackupSessionRequest_BACKUP_SESSION_OP_PREPARE_READ,
			SessionId: id,
			Offset:    offset,
			Length:    length,
		}
		info, n, err = p.cluster.BackupSession(ctx, bsr, addr, creds, timeout, nil)
		if err != nil {
			return nil, 0, "", wrapIfUnauthorized(err)
		}
		return info, n, addr, nil
	}
	return info, n, p.GetAPIAddr(), err
}

// BackupSessionRead streams a byte range of a backup session to dst. See
// BackupSessionPrepareRead for the meaning of leaderOnly and forwarding.
func (p *Proxy) BackupSessionRead(ctx context.Context, id string, offset, length int64,
	leaderOnly bool, dst io.Writer, creds *clstrPB.Credentials, timeout time.Duration,
	noForward bool) (int64, string, error) {
	n, err := p.store.BackupSessionRead(ctx, id, offset, length, dst)
	if errors.Is(err, store.ErrNotLeader) || (leaderOnly && errors.Is(err, store.ErrBackupSessionNotFound)) {
		if noForward {
			return 0, "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return 0, "", addrErr
		}
		bsr := &proto.BackupSessionRequest{
			Op:        proto.BackupSessionRequest_BACKUP_SESSION_OP_READ,
			SessionId: id,
			Offset:    offset,
			Length:    length,
		}
		_, n, err = p.cluster.BackupSession(ctx, bsr, addr, creds, timeout, dst)
		if err != nil {
			return 0, "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteBackups, 1)
		return n, addr, nil
	}
	return n, p.GetAPIAddr(), err
}

// BackupSessionRelease releases a backup session. Leader-only sessions unknown
// to the local node are forwarded to the leader when forwarding is enabled.
func (p *Proxy) BackupSessionRelease(ctx context.Context, id string, leaderOnly bool,
	creds *clstrPB.Credentials, timeout time.Duration, noForward bool) (string, error) {
	err := p.store.BackupSessionRelease(ctx, id)
	if errors.Is(err, store.ErrNotLeader) || (leaderOnly && errors.Is(err, store.ErrBackupSessionNotFound)) {
		if noForward {
			return "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return "", addrErr
		}
		bsr := &proto.BackupSessionRequest{
			Op:        proto.BackupSessionRequest_BACKUP_SESSION_OP_RELEASE,
			SessionId: id,
		}
		if _, _, err := p.cluster.BackupSession(ctx, bsr, addr, creds, timeout, nil); err != nil {
			return "", wrapIfUnauthorized(err)
		}
		return addr, nil
	}
	return p.GetAPIAddr(), err
}

// Load loads a SQLite file into the cluster. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Load(ctx context.Context, lr *proto.LoadRequest, creds *clstrPB.Credentials,
	timeout time.Duration, retries int, noForward bool) (string, error) {

	err := p.store.Load(ctx, lr)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return "", addrErr
		}
		err = p.cluster.Load(ctx, lr, addr, creds, timeout, retries)
		if err != nil {
			return "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteLoads, 1)
		return addr, nil
	}
	return p.GetAPIAddr(), err
}

// Remove removes a node from the cluster. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Remove(ctx context.Context, rn *proto.RemoveNodeRequest, creds *clstrPB.Credentials,
	timeout time.Duration, noForward bool) (string, error) {

	err := p.store.Remove(ctx, rn)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return "", addrErr
		}
		err = p.cluster.RemoveNode(ctx, rn, addr, creds, timeout)
		if err != nil {
			return "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteRemoveNode, 1)
		return addr, nil
	}
	return p.GetAPIAddr(), err
}

// Stepdown triggers leader stepdown. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Stepdown(ctx context.Context, wait bool, id string, creds *clstrPB.Credentials,
	timeout time.Duration, noForward bool) (string, error) {

	err := p.store.Stepdown(wait, id)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return "", ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return "", addrErr
		}
		sr := &proto.StepdownRequest{
			Id:   id,
			Wait: wait,
		}
		err = p.cluster.Stepdown(ctx, sr, addr, creds, timeout)
		if err != nil {
			return "", wrapIfUnauthorized(err)
		}
		stats.Add(numRemoteStepdowns, 1)
		return addr, nil
	}
	return p.GetAPIAddr(), err
}

// leaderAddr returns the Raft address of the current leader. Returns
// ErrLeaderNotFound if the address is empty.
func (p *Proxy) leaderAddr() (string, error) {
	addr, err := p.store.LeaderAddr()
	if err != nil {
		return "", err
	}
	if addr == "" {
		return "", ErrLeaderNotFound
	}
	return addr, nil
}

// wrapIfUnauthorized wraps an error as ErrUnauthorized if the error
// message is "unauthorized", allowing callers to use errors.Is().
func wrapIfUnauthorized(err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == "unauthorized" {
		return ErrUnauthorized
	}
	return err
}
