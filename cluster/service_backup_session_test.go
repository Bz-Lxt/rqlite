package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

func Test_ServiceBackupSession(t *testing.T) {
	ln, mux := mustNewMux()
	defer mux.Close()
	go mux.Serve()
	tn := mux.Listen(1)
	db := mustNewMockDatabase()
	mgr := mustNewMockManager()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, mgr, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}
	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}
	defer func() {
		if err := ln.Close(); err != nil {
			t.Fatalf("failed to close listener: %s", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("failed to close cluster service")
		}
	}()

	artifact := bytes.Repeat([]byte("rqlite-resume!"), 5000) // 65,000 bytes
	info := &command.BackupSessionInfo{
		SessionId: "sess-1",
		Size:      int64(len(artifact)),
		Checksum:  strings.Repeat("a", 64),
		RaftIndex: 42,
	}

	db.backupSessionCreateFn = func(br *command.BackupRequest) (*command.BackupSessionInfo, error) {
		if br.Format != command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY {
			t.Fatalf("expected binary backup request")
		}
		return info, nil
	}
	db.backupSessionPrepareReadFn = func(id string, offset, length int64) (*command.BackupSessionInfo, int64, error) {
		if id != info.SessionId {
			return nil, 0, errors.New("backup session not found")
		}
		n := info.Size - offset
		if length > 0 && length < n {
			n = length
		}
		return info, n, nil
	}
	db.backupSessionReadFn = func(id string, offset, length int64, dst io.Writer) (int64, error) {
		if id != info.SessionId {
			return 0, errors.New("backup session not found")
		}
		n := info.Size - offset
		if length > 0 && length < n {
			n = length
		}
		if _, err := dst.Write(artifact[offset : offset+n]); err != nil {
			return 0, err
		}
		return n, nil
	}
	released := false
	db.backupSessionReleaseFn = func(id string) error {
		if id != info.SessionId {
			return errors.New("backup session not found")
		}
		released = true
		return nil
	}

	ctx := context.Background()

	// CREATE
	createReq := &command.BackupSessionRequest{
		Op:            command.BackupSessionRequest_BACKUP_SESSION_OP_CREATE,
		BackupRequest: backupRequestBinary(true),
	}
	gotInfo, written, err := c.BackupSession(ctx, createReq, s.Addr(), NO_CREDS, longWait, nil)
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	if written != 0 || gotInfo.GetSessionId() != info.SessionId || gotInfo.Size != info.Size ||
		gotInfo.Checksum != info.Checksum || gotInfo.RaftIndex != info.RaftIndex {
		t.Fatalf("unexpected create response: %+v written=%d", gotInfo, written)
	}

	// PREPARE_READ: bounded range.
	prepReq := &command.BackupSessionRequest{
		Op:        command.BackupSessionRequest_BACKUP_SESSION_OP_PREPARE_READ,
		SessionId: info.SessionId,
		Offset:    100,
		Length:    50,
	}
	gotInfo, n, err := c.BackupSession(ctx, prepReq, s.Addr(), NO_CREDS, longWait, nil)
	if err != nil {
		t.Fatalf("prepare failed: %s", err)
	}
	if n != 50 || gotInfo.Checksum != info.Checksum {
		t.Fatalf("unexpected prepare result: n=%d info=%+v", n, gotInfo)
	}

	// READ: bounded middle range.
	var buf bytes.Buffer
	readReq := &command.BackupSessionRequest{
		Op:        command.BackupSessionRequest_BACKUP_SESSION_OP_READ,
		SessionId: info.SessionId,
		Offset:    100,
		Length:    50,
	}
	gotInfo, n, err = c.BackupSession(ctx, readReq, s.Addr(), NO_CREDS, longWait, &buf)
	if err != nil {
		t.Fatalf("read failed: %s", err)
	}
	if n != 50 || buf.Len() != 50 {
		t.Fatalf("expected 50 bytes, got n=%d buffer=%d", n, buf.Len())
	}
	if !bytes.Equal(buf.Bytes(), artifact[100:150]) {
		t.Fatal("read bytes do not match pinned artifact range")
	}

	// READ: open-ended range (zero length) to the end of the artifact, on a
	// freshly pooled connection following the previous streamed read.
	buf.Reset()
	tailReq := &command.BackupSessionRequest{
		Op:        command.BackupSessionRequest_BACKUP_SESSION_OP_READ,
		SessionId: info.SessionId,
		Offset:    info.Size - 10,
	}
	_, n, err = c.BackupSession(ctx, tailReq, s.Addr(), NO_CREDS, longWait, &buf)
	if err != nil {
		t.Fatalf("tail read failed: %s", err)
	}
	if n != 10 || !bytes.Equal(buf.Bytes(), artifact[info.Size-10:]) {
		t.Fatalf("tail read mismatch: n=%d bytes=%v", n, buf.Bytes())
	}

	// READ against an unknown session: explicit error, no stream.
	var junk bytes.Buffer
	unknownReq := &command.BackupSessionRequest{
		Op:        command.BackupSessionRequest_BACKUP_SESSION_OP_READ,
		SessionId: "ghost",
		Offset:    0,
		Length:    10,
	}
	if _, _, err := c.BackupSession(ctx, unknownReq, s.Addr(), NO_CREDS, longWait, &junk); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
	if junk.Len() != 0 {
		t.Fatalf("expected no bytes for failed read, got %d", junk.Len())
	}

	// RELEASE, plus explicit failure for an unknown session.
	relReq := &command.BackupSessionRequest{
		Op:        command.BackupSessionRequest_BACKUP_SESSION_OP_RELEASE,
		SessionId: info.SessionId,
	}
	if _, _, err := c.BackupSession(ctx, relReq, s.Addr(), NO_CREDS, longWait, nil); err != nil {
		t.Fatalf("release failed: %s", err)
	}
	if !released {
		t.Fatal("release did not reach database layer")
	}
	relReq.SessionId = "ghost"
	if _, _, err := c.BackupSession(ctx, relReq, s.Addr(), NO_CREDS, longWait, nil); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error on release, got %v", err)
	}
}
