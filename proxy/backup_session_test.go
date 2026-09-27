package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	clstrPB "github.com/rqlite/rqlite/v10/cluster/proto"
	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/store"
)

const testLeaderAddr = "leader:4002"

func testSessionInfo() *proto.BackupSessionInfo {
	return &proto.BackupSessionInfo{
		SessionId: "sess-1",
		Size:      1234,
		Checksum:  "abcdef",
		RaftIndex: 99,
	}
}

func Test_BackupSessionCreate_LocalSuccess(t *testing.T) {
	t.Parallel()
	info := testSessionInfo()
	s := &mockStore{
		backupSessionCreateFn: func(ctx context.Context, br *proto.BackupRequest) (*proto.BackupSessionInfo, error) {
			return info, nil
		},
	}
	p := newTestProxy(s, &mockCluster{})
	p.SetAPIAddr("follower:4001")

	got, addr, err := p.BackupSessionCreate(context.Background(), &proto.BackupRequest{}, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if got.GetSessionId() != info.SessionId {
		t.Fatalf("unexpected info: %+v", got)
	}
	if addr != "follower:4001" {
		t.Fatalf("expected local addr, got %s", addr)
	}
}

func Test_BackupSessionCreate_Forward(t *testing.T) {
	t.Parallel()
	info := testSessionInfo()
	s := &mockStore{
		backupSessionCreateFn: func(ctx context.Context, br *proto.BackupRequest) (*proto.BackupSessionInfo, error) {
			return nil, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) { return testLeaderAddr, nil },
	}
	var gotOp proto.BackupSessionRequest_Operation
	c := &mockCluster{
		backupSessionFn: func(ctx context.Context, bsr *proto.BackupSessionRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) (*proto.BackupSessionInfo, int64, error) {
			gotOp = bsr.Op
			if nodeAddr != testLeaderAddr {
				t.Fatalf("expected forwarding to %s, got %s", testLeaderAddr, nodeAddr)
			}
			return info, 0, nil
		},
	}
	p := newTestProxy(s, c)

	got, addr, err := p.BackupSessionCreate(context.Background(), &proto.BackupRequest{}, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if got.GetSessionId() != info.SessionId || addr != testLeaderAddr {
		t.Fatalf("unexpected result: info=%+v addr=%s", got, addr)
	}
	if gotOp != proto.BackupSessionRequest_BACKUP_SESSION_OP_CREATE {
		t.Fatalf("expected CREATE op, got %v", gotOp)
	}

	// Redirect mode: no forwarding.
	if _, _, err := p.BackupSessionCreate(context.Background(), &proto.BackupRequest{}, nil, time.Second, true); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_BackupSessionPrepareRead_LeaderOnlyForwarding(t *testing.T) {
	t.Parallel()
	info := testSessionInfo()
	localCalls := 0
	s := &mockStore{
		backupSessionPrepareReadFn: func(ctx context.Context, id string, offset, length int64) (*proto.BackupSessionInfo, int64, error) {
			localCalls++
			return nil, 0, store.ErrBackupSessionNotFound
		},
		leaderAddrFn: func() (string, error) { return testLeaderAddr, nil },
	}
	c := &mockCluster{
		backupSessionFn: func(ctx context.Context, bsr *proto.BackupSessionRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) (*proto.BackupSessionInfo, int64, error) {
			if bsr.Op != proto.BackupSessionRequest_BACKUP_SESSION_OP_PREPARE_READ {
				t.Fatalf("expected PREPARE_READ op, got %v", bsr.Op)
			}
			if bsr.SessionId != info.SessionId || bsr.Offset != 100 || bsr.Length != 50 {
				t.Fatalf("unexpected forwarded request: %+v", bsr)
			}
			return info, 50, nil
		},
	}
	p := newTestProxy(s, c)

	got, n, addr, err := p.BackupSessionPrepareRead(context.Background(), info.SessionId, 100, 50, true, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != 50 || got.Checksum != info.Checksum || addr != testLeaderAddr {
		t.Fatalf("unexpected result: n=%d info=%+v addr=%s", n, got, addr)
	}

	// A follower-local session (leaderOnly=false) must not be forwarded.
	if _, _, _, err := p.BackupSessionPrepareRead(context.Background(), info.SessionId, 0, 10, false, nil, time.Second, false); !errors.Is(err, store.ErrBackupSessionNotFound) {
		t.Fatalf("expected NotFound without forwarding, got %v", err)
	}
}

func Test_BackupSessionRead_ForwardStreams(t *testing.T) {
	t.Parallel()
	payload := []byte("the missing chunk")
	s := &mockStore{
		backupSessionReadFn: func(ctx context.Context, id string, offset, length int64, dst io.Writer) (int64, error) {
			return 0, store.ErrBackupSessionNotFound
		},
		leaderAddrFn: func() (string, error) { return testLeaderAddr, nil },
	}
	c := &mockCluster{
		backupSessionFn: func(ctx context.Context, bsr *proto.BackupSessionRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) (*proto.BackupSessionInfo, int64, error) {
			if bsr.Op != proto.BackupSessionRequest_BACKUP_SESSION_OP_READ {
				t.Fatalf("expected READ op, got %v", bsr.Op)
			}
			if n, err := w.Write(payload); err != nil || n != len(payload) {
				t.Fatalf("failed to write payload: %v n=%d", err, n)
			}
			return testSessionInfo(), int64(len(payload)), nil
		},
	}
	p := newTestProxy(s, c)

	var buf bytes.Buffer
	n, addr, err := p.BackupSessionRead(context.Background(), "sess-1", 50, int64(len(payload)), true, &buf, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if n != int64(len(payload)) || addr != testLeaderAddr || !bytes.Equal(buf.Bytes(), payload) {
		t.Fatalf("unexpected read result: n=%d addr=%s bytes=%q", n, addr, buf.Bytes())
	}
}

func Test_BackupSessionRelease_Forward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		backupSessionReleaseFn: func(ctx context.Context, id string) error {
			return store.ErrBackupSessionNotFound
		},
		leaderAddrFn: func() (string, error) { return testLeaderAddr, nil },
	}
	var releasedID string
	c := &mockCluster{
		backupSessionFn: func(ctx context.Context, bsr *proto.BackupSessionRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) (*proto.BackupSessionInfo, int64, error) {
			if bsr.Op != proto.BackupSessionRequest_BACKUP_SESSION_OP_RELEASE {
				t.Fatalf("expected RELEASE op, got %v", bsr.Op)
			}
			releasedID = bsr.SessionId
			return nil, 0, nil
		},
	}
	p := newTestProxy(s, c)

	addr, err := p.BackupSessionRelease(context.Background(), "sess-1", true, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if addr != testLeaderAddr || releasedID != "sess-1" {
		t.Fatalf("unexpected release result: addr=%s id=%s", addr, releasedID)
	}
}
