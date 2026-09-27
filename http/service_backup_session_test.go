package http

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/store"
)

type backupSessionHarness struct {
	s        *Service
	m        *MockStore
	c        *mockClusterService
	artifact []byte
	info     *command.BackupSessionInfo
}

func newBackupSessionHarness(t *testing.T) *backupSessionHarness {
	t.Helper()
	artifact := bytes.Repeat([]byte("rqlite-backup-session-0123456789"), 4) // 128 bytes
	sum := sha256.Sum256(artifact)
	info := &command.BackupSessionInfo{
		SessionId: "sess-abc",
		Size:      int64(len(artifact)),
		Checksum:  hex.EncodeToString(sum[:]),
		RaftIndex: 77,
	}
	m := &MockStore{leaderAddr: "leader:4002"}
	c := &mockClusterService{apiAddr: "http://leader:4002"}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	t.Cleanup(s.Close)

	// By default a session unknown locally is forwarded to the leader, which
	// confirms it does not know the session either.
	c.backupSessionFn = func(bsr *command.BackupSessionRequest, addr string, _ time.Duration, w io.Writer) (*command.BackupSessionInfo, int64, error) {
		return nil, 0, store.ErrBackupSessionNotFound
	}

	m.backupSessionCreateFn = func(br *command.BackupRequest) (*command.BackupSessionInfo, error) {
		return info, nil
	}
	m.backupSessionPrepareReadFn = func(id string, offset, length int64) (*command.BackupSessionInfo, int64, error) {
		if id != info.SessionId {
			return nil, 0, store.ErrBackupSessionNotFound
		}
		n := info.Size - offset
		if length > 0 && length < n {
			n = length
		}
		return info, n, nil
	}
	m.backupSessionReadFn = func(id string, offset, length int64, dst io.Writer) (int64, error) {
		if id != info.SessionId {
			return 0, store.ErrBackupSessionNotFound
		}
		n := info.Size - offset
		if length > 0 && length < n {
			n = length
		}
		_, err := dst.Write(artifact[offset : offset+n])
		return n, err
	}
	return &backupSessionHarness{s: s, m: m, c: c, artifact: artifact, info: info}
}

func (h *backupSessionHarness) url(path string) string {
	return "http://" + h.s.Addr().String() + path
}

func Test_BackupSession_Create(t *testing.T) {
	h := newBackupSessionHarness(t)
	resp, err := http.Get(h.url("/db/backup?session=new"))
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, mustReadBody(t, resp))
	}
	if body := mustReadBody(t, resp); body != "" {
		t.Fatalf("expected empty body, got %q", body)
	}
	if got := resp.Header.Get(BackupSessionHeader); got != h.info.SessionId {
		t.Fatalf("session header mismatch: %q", got)
	}
	if got := resp.Header.Get(BackupSessionSizeHeader); got != "128" {
		t.Fatalf("size header mismatch: %q", got)
	}
	if got := resp.Header.Get(BackupSessionSumHeader); got != "sha256:"+h.info.Checksum {
		t.Fatalf("checksum header mismatch: %q", got)
	}
	if got := resp.Header.Get(BackupSessionIndexHeader); got != "77" {
		t.Fatalf("raft index header mismatch: %q", got)
	}
	if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("accept-ranges header mismatch: %q", got)
	}
}

func Test_BackupSession_ReadFull(t *testing.T) {
	h := newBackupSessionHarness(t)
	req, _ := http.NewRequest(http.MethodGet, h.url("/db/backup?session=sess-abc"), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if body := mustReadBody(t, resp); body != string(h.artifact) {
		t.Fatalf("full read body mismatch, len=%d", len(body))
	}
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		t.Fatalf("unexpected Content-Range on full read: %q", cr)
	}
	if resp.Header.Get(BackupSessionHeader) != h.info.SessionId {
		t.Fatal("missing session metadata headers")
	}
}

func Test_BackupSession_ReadRange(t *testing.T) {
	h := newBackupSessionHarness(t)
	req, _ := http.NewRequest(http.MethodGet, h.url("/db/backup?session=sess-abc"), nil)
	req.Header.Set("Range", "bytes=10-29")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d: %s", resp.StatusCode, mustReadBody(t, resp))
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes 10-29/128" {
		t.Fatalf("content-range mismatch: %q", got)
	}
	if resp.ContentLength != 20 {
		t.Fatalf("content-length mismatch: %d", resp.ContentLength)
	}
	if body := mustReadBody(t, resp); body != string(h.artifact[10:30]) {
		t.Fatalf("ranged body mismatch: %q", body)
	}

	// Suffix range: last 5 bytes.
	req2, _ := http.NewRequest(http.MethodGet, h.url("/db/backup?session=sess-abc"), nil)
	req2.Header.Set("Range", "bytes=-5")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("suffix request failed: %s", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d", resp2.StatusCode)
	}
	if got := resp2.Header.Get("Content-Range"); got != "bytes 123-127/128" {
		t.Fatalf("suffix content-range mismatch: %q", got)
	}
	if body := mustReadBody(t, resp2); body != string(h.artifact[123:128]) {
		t.Fatalf("suffix body mismatch: %q", body)
	}
}

func Test_BackupSession_IfMatchMismatch(t *testing.T) {
	h := newBackupSessionHarness(t)
	req, _ := http.NewRequest(http.MethodGet, h.url("/db/backup?session=sess-abc"), nil)
	req.Header.Set("If-Match", `"sha256:`+strings.Repeat("0", 64)+`"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
}

func Test_BackupSession_UnknownSession(t *testing.T) {
	h := newBackupSessionHarness(t)
	resp, err := http.Get(h.url("/db/backup?session=ghost"))
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	// Unsatisfiable range against an existing session.
	req, _ := http.NewRequest(http.MethodGet, h.url("/db/backup?session=sess-abc"), nil)
	req.Header.Set("Range", "bytes=1000-")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("range request failed: %s", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("expected 416, got %d", resp2.StatusCode)
	}
}

func Test_BackupSession_Release(t *testing.T) {
	h := newBackupSessionHarness(t)
	released := 0
	h.m.backupSessionReleaseFn = func(id string) error {
		if id != h.info.SessionId {
			return store.ErrBackupSessionNotFound
		}
		released++
		return nil
	}

	req, _ := http.NewRequest(http.MethodDelete, h.url("/db/backup?session=sess-abc"), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("release request failed: %s", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
	if released != 1 {
		t.Fatalf("expected one release, got %d", released)
	}

	req2, _ := http.NewRequest(http.MethodDelete, h.url("/db/backup?session=ghost"), nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second release request failed: %s", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown release, got %d", resp2.StatusCode)
	}
}

func Test_BackupSession_MethodNotAllowed(t *testing.T) {
	h := newBackupSessionHarness(t)
	req, _ := http.NewRequest(http.MethodPost, h.url("/db/backup?session=new"), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

// Test_BackupSession_FollowerForward verifies a follower that does not hold the
// session transparently prepares and streams ranged reads from the leader.
func Test_BackupSession_FollowerForward(t *testing.T) {
	artifact := bytes.Repeat([]byte("follower-forwarded-bytes!"), 8) // 200 bytes
	sum := sha256.Sum256(artifact)
	info := &command.BackupSessionInfo{
		SessionId: "leader-sess",
		Size:      int64(len(artifact)),
		Checksum:  hex.EncodeToString(sum[:]),
		RaftIndex: 5,
	}
	m := &MockStore{leaderAddr: "leader:4002"}
	c := &mockClusterService{apiAddr: "http://leader:4002"}
	srv := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	defer srv.Close()

	m.backupSessionCreateFn = func(br *command.BackupRequest) (*command.BackupSessionInfo, error) {
		return info, nil
	}
	// Every session operation is unknown locally and must be forwarded.
	m.backupSessionPrepareReadFn = func(id string, offset, length int64) (*command.BackupSessionInfo, int64, error) {
		return nil, 0, store.ErrBackupSessionNotFound
	}
	m.backupSessionReadFn = func(id string, offset, length int64, dst io.Writer) (int64, error) {
		return 0, store.ErrBackupSessionNotFound
	}
	c.backupSessionFn = func(bsr *command.BackupSessionRequest, addr string, _ time.Duration, w io.Writer) (*command.BackupSessionInfo, int64, error) {
		switch bsr.Op {
		case command.BackupSessionRequest_BACKUP_SESSION_OP_PREPARE_READ:
			n := info.Size - bsr.Offset
			if bsr.Length > 0 && bsr.Length < n {
				n = bsr.Length
			}
			return info, n, nil
		case command.BackupSessionRequest_BACKUP_SESSION_OP_READ:
			n := info.Size - bsr.Offset
			if bsr.Length > 0 && bsr.Length < n {
				n = bsr.Length
			}
			if _, err := w.Write(artifact[bsr.Offset : bsr.Offset+n]); err != nil {
				return nil, 0, err
			}
			return info, n, nil
		default:
			t.Fatalf("unexpected forwarded op: %v", bsr.Op)
			return nil, 0, nil
		}
	}

	req, _ := http.NewRequest(http.MethodGet, "http://"+srv.Addr().String()+"/db/backup?session=leader-sess", nil)
	req.Header.Set("Range", "bytes=160-199")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("forwarded request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d: %s", resp.StatusCode, mustReadBody(t, resp))
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes 160-199/200" {
		t.Fatalf("content-range mismatch: %q", got)
	}
	if body := mustReadBody(t, resp); body != string(artifact[160:200]) {
		t.Fatalf("forwarded body mismatch: %q", body)
	}
}
