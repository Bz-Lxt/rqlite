package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// newLeaderStore opens, bootstraps and populates a single-node store and
// returns it ready for backup-session tests.
func newBackupTestStore(t *testing.T) *Store {
	t.Helper()
	s, ln := mustNewStore(t)
	t.Cleanup(func() { ln.Close() })
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	t.Cleanup(func() { s.Close(true) })
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}
	dump := `CREATE TABLE foo (id integer not null primary key, name text);
INSERT INTO "foo" VALUES(1,'fiona');
INSERT INTO "foo" VALUES(2,'declan');
`
	if _, _, err := s.Execute(context.Background(), executeRequestFromString(dump, false, false)); err != nil {
		t.Fatalf("failed to load test dump: %s", err.Error())
	}
	return s
}

func binarySessionBackupRequest() *proto.BackupRequest {
	return &proto.BackupRequest{
		Format: proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Leader: true,
	}
}

func sha256Hex(t *testing.T, b []byte) string {
	t.Helper()
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Test_BackupSession_CreateAndRead verifies a created session pins an artifact
// whose checksum matches a regular binary backup, and that reading it whole or
// in arbitrary chunks yields the same bytes.
func Test_BackupSession_CreateAndRead(t *testing.T) {
	s := newBackupTestStore(t)
	ctx := context.Background()

	var direct bytes.Buffer
	if err := s.Backup(ctx, backupRequestBinary(true, false, false), &direct); err != nil {
		t.Fatalf("direct backup failed: %s", err.Error())
	}
	wantSum := sha256Hex(t, direct.Bytes())

	info, err := s.BackupSessionCreate(ctx, binarySessionBackupRequest())
	if err != nil {
		t.Fatalf("BackupSessionCreate failed: %s", err.Error())
	}
	if info.SessionId == "" {
		t.Fatal("expected non-empty session ID")
	}
	if info.Size != int64(direct.Len()) {
		t.Fatalf("session size %d does not match direct backup size %d", info.Size, direct.Len())
	}
	if info.Checksum != wantSum {
		t.Fatalf("session checksum %s does not match direct backup checksum %s", info.Checksum, wantSum)
	}
	if info.RaftIndex == 0 {
		t.Fatal("expected non-zero raft index")
	}

	// Whole-artifact read.
	gotInfo, n, err := s.BackupSessionPrepareRead(ctx, info.SessionId, 0, 0)
	if err != nil {
		t.Fatalf("prepare whole read failed: %s", err)
	}
	if gotInfo.String() != info.String() {
		t.Fatalf("prepare info mismatch: %v != %v", gotInfo, info)
	}
	if n != info.Size {
		t.Fatalf("expected %d bytes, got %d", info.Size, n)
	}
	var whole bytes.Buffer
	if _, err := s.BackupSessionRead(ctx, info.SessionId, 0, 0, &whole); err != nil {
		t.Fatalf("whole read failed: %s", err)
	}
	if !bytes.Equal(whole.Bytes(), direct.Bytes()) {
		t.Fatal("whole session read does not match direct backup")
	}

	// Reassemble through small, overlapping-style chunks.
	var rebuilt bytes.Buffer
	const chunk = 4096
	for off := int64(0); off < info.Size; off += chunk {
		pi, pn, err := s.BackupSessionPrepareRead(ctx, info.SessionId, off, chunk)
		if err != nil {
			t.Fatalf("prepare chunk at %d failed: %s", off, err)
		}
		if pi.Checksum != info.Checksum {
			t.Fatal("checksum changed between chunks")
		}
		var part bytes.Buffer
		rn, err := s.BackupSessionRead(ctx, info.SessionId, off, chunk, &part)
		if err != nil {
			t.Fatalf("read chunk at %d failed: %s", off, err)
		}
		if rn != pn || int64(part.Len()) != pn {
			t.Fatalf("chunk length mismatch: prepare=%d read=%d actual=%d", pn, rn, part.Len())
		}
		rebuilt.Write(part.Bytes())
	}
	if rebuilt.Len() != int(info.Size) {
		t.Fatalf("rebuilt size %d != %d", rebuilt.Len(), info.Size)
	}
	if sha256Hex(t, rebuilt.Bytes()) != wantSum {
		t.Fatal("reassembled chunks checksum mismatch")
	}
}

// Test_BackupSession_InvalidRanges verifies out-of-bounds ranges are rejected
// explicitly.
func Test_BackupSession_InvalidRanges(t *testing.T) {
	s := newBackupTestStore(t)
	ctx := context.Background()
	info, err := s.BackupSessionCreate(ctx, binarySessionBackupRequest())
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}

	for _, tc := range []struct {
		name           string
		offset, length int64
	}{
		{"negative offset", -1, 10},
		{"offset at size", info.Size, 10},
		{"offset past size", info.Size + 100, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := s.BackupSessionPrepareRead(ctx, info.SessionId, tc.offset, tc.length); !errors.Is(err, ErrBackupSessionInvalidRange) {
				t.Fatalf("expected ErrBackupSessionInvalidRange, got %v", err)
			}
			var b bytes.Buffer
			if _, err := s.BackupSessionRead(ctx, info.SessionId, tc.offset, tc.length, &b); !errors.Is(err, ErrBackupSessionInvalidRange) {
				t.Fatalf("expected ErrBackupSessionInvalidRange, got %v", err)
			}
		})
	}

	// A length stretching past the end is clamped to the artifact end.
	_, n, err := s.BackupSessionPrepareRead(ctx, info.SessionId, info.Size-4, 100)
	if err != nil {
		t.Fatalf("oversubscribed length rejected: %s", err)
	}
	if n != 4 {
		t.Fatalf("expected 4 bytes, got %d", n)
	}
}

// Test_BackupSession_UnknownAndRelease verifies unknown sessions are rejected
// and release is single-shot.
func Test_BackupSession_UnknownAndRelease(t *testing.T) {
	s := newBackupTestStore(t)
	ctx := context.Background()

	if _, err := s.BackupSessionCreate(ctx, binarySessionBackupRequest()); err != nil {
		t.Fatalf("create failed: %s", err)
	}
	if _, _, err := s.BackupSessionPrepareRead(ctx, "does-not-exist", 0, 10); !errors.Is(err, ErrBackupSessionNotFound) {
		t.Fatalf("expected NotFound prepare, got %v", err)
	}
	var b bytes.Buffer
	if _, err := s.BackupSessionRead(ctx, "does-not-exist", 0, 10, &b); !errors.Is(err, ErrBackupSessionNotFound) {
		t.Fatalf("expected NotFound read, got %v", err)
	}
	if err := s.BackupSessionRelease(ctx, "does-not-exist"); !errors.Is(err, ErrBackupSessionNotFound) {
		t.Fatalf("expected NotFound release, got %v", err)
	}

	info, err := s.BackupSessionCreate(ctx, binarySessionBackupRequest())
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	sess := s.backupSessions[info.SessionId]
	if _, err := os.Stat(sess.path); err != nil {
		t.Fatalf("artifact file missing: %s", err)
	}
	if err := s.BackupSessionRelease(ctx, info.SessionId); err != nil {
		t.Fatalf("release failed: %s", err)
	}
	if _, err := os.Stat(sess.path); !os.IsNotExist(err) {
		t.Fatalf("artifact file not removed after release, stat err: %v", err)
	}
	if err := s.BackupSessionRelease(ctx, info.SessionId); !errors.Is(err, ErrBackupSessionNotFound) {
		t.Fatalf("second release expected NotFound, got %v", err)
	}
}

// Test_BackupSession_Reap verifies absolute TTL and idle-timeout reaping and
// that busy sessions are spared.
func Test_BackupSession_Reap(t *testing.T) {
	s := newBackupTestStore(t)
	ctx := context.Background()
	info, err := s.BackupSessionCreate(ctx, binarySessionBackupRequest())
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	sess := s.backupSessions[info.SessionId]
	artifactPath := sess.path

	now := time.Now()
	s.reapBackupSessions(now)
	if _, err := s.lookupBackupSession(info.SessionId); err != nil {
		t.Fatalf("fresh session reaped: %s", err)
	}

	// Busy and long-idle: the reaper must skip it.
	sess.mu.Lock()
	sess.reading = 1
	sess.mu.Unlock()
	s.reapBackupSessions(now.Add(backupSessionIdleTimeout + time.Minute))
	if _, err := s.lookupBackupSession(info.SessionId); err != nil {
		t.Fatalf("busy session reaped while reading: %s", err)
	}
	sess.mu.Lock()
	sess.reading = 0
	sess.mu.Unlock()

	// Idle past the timeout once idle: reaped and file removed.
	s.reapBackupSessions(now.Add(backupSessionIdleTimeout + time.Minute))
	if _, err := s.lookupBackupSession(info.SessionId); !errors.Is(err, ErrBackupSessionNotFound) {
		t.Fatalf("idle session not reaped: %v", err)
	}
	if _, err := os.Stat(artifactPath); !os.IsNotExist(err) {
		t.Fatal("reaped artifact file not removed")
	}

	// Absolute TTL expiry.
	info2, err := s.BackupSessionCreate(ctx, binarySessionBackupRequest())
	if err != nil {
		t.Fatalf("second create failed: %s", err)
	}
	sess2 := s.backupSessions[info2.SessionId]
	path2 := sess2.path
	// Recently accessed but older than the absolute TTL.
	sess2.mu.Lock()
	sess2.createdAt = now.Add(-backupSessionTTL - time.Minute)
	sess2.lastAccess = now
	sess2.mu.Unlock()
	s.reapBackupSessions(now)
	if _, err := s.lookupBackupSession(info2.SessionId); !errors.Is(err, ErrBackupSessionNotFound) {
		t.Fatalf("TTL-expired session not reaped: %v", err)
	}
	if _, err := os.Stat(path2); !os.IsNotExist(err) {
		t.Fatal("TTL-reaped artifact file not removed")
	}
}

// Test_BackupSession_EvictionAndLimit verifies LRU-idle eviction when the
// session cap is reached, and a hard limit error while every session is busy.
func Test_BackupSession_EvictionAndLimit(t *testing.T) {
	s := newBackupTestStore(t)

	mkSess := func(i int, idleFor time.Duration, busy bool) *backupSession {
		f, err := os.CreateTemp(s.dbDir, backupSessionScratchPattern)
		if err != nil {
			t.Fatalf("scratch create failed: %s", err)
		}
		if _, err := f.WriteString(fmt.Sprintf("artifact-%d", i)); err != nil {
			t.Fatalf("scratch write failed: %s", err)
		}
		f.Close()
		sess := &backupSession{
			id:        fmt.Sprintf("synthetic-%d", i),
			path:      f.Name(),
			size:      int64(10 + i),
			checksum:  fmt.Sprintf("%064x", i),
			createdAt: time.Now(),
		}
		sess.lastAccess = time.Now().Add(-idleFor)
		if busy {
			sess.reading = 1
		}
		return sess
	}

	oldest := mkSess(0, time.Hour, false)
	if err := s.registerBackupSession(oldest); err != nil {
		t.Fatalf("register oldest failed: %s", err)
	}
	for i := 1; i < backupSessionMax; i++ {
		if err := s.registerBackupSession(mkSess(i, time.Minute, false)); err != nil {
			t.Fatalf("register %d failed: %s", i, err)
		}
	}
	if _, err := s.lookupBackupSession(oldest.id); err != nil {
		t.Fatalf("oldest vanished before cap: %s", err)
	}

	// One more: the longest-idle session must be evicted.
	extra := mkSess(backupSessionMax, time.Second, false)
	if err := s.registerBackupSession(extra); err != nil {
		t.Fatalf("register extra failed: %s", err)
	}
	if _, err := s.lookupBackupSession(oldest.id); !errors.Is(err, ErrBackupSessionNotFound) {
		t.Fatalf("expected oldest session evicted, got %v", err)
	}
	if _, err := os.Stat(oldest.path); !os.IsNotExist(err) {
		t.Fatal("evicted artifact not removed")
	}
	if _, err := s.lookupBackupSession(extra.id); err != nil {
		t.Fatalf("new session missing: %v", err)
	}

	// Fill to capacity with busy sessions, then eviction must fail.
	s.backupSessionMu.Lock()
	for _, sess := range s.backupSessions {
		sess.mu.Lock()
		sess.reading = 1
		sess.mu.Unlock()
	}
	s.backupSessionMu.Unlock()
	if err := s.registerBackupSession(mkSess(999, 0, false)); !errors.Is(err, ErrBackupSessionLimit) {
		t.Fatalf("expected ErrBackupSessionLimit, got %v", err)
	}
}

// Test_BackupSession_InvalidFormats verifies only BINARY backups are allowed.
func Test_BackupSession_InvalidFormats(t *testing.T) {
	s := newBackupTestStore(t)
	for _, f := range []proto.BackupRequest_Format{
		proto.BackupRequest_BACKUP_REQUEST_FORMAT_NONE,
		proto.BackupRequest_BACKUP_REQUEST_FORMAT_SQL,
	} {
		br := binarySessionBackupRequest()
		br.Format = f
		if _, err := s.BackupSessionCreate(context.Background(), br); !errors.Is(err, ErrInvalidBackupFormat) {
			t.Fatalf("format %v: expected ErrInvalidBackupFormat, got %v", f, err)
		}
	}
}

// Test_BackupSession_LeaderRequired verifies a non-leader rejects a
// leader-pinned create.
func Test_BackupSession_LeaderRequired(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	defer s.Close(true)
	// An unbootstrapped node never becomes leader.
	if _, err := s.BackupSessionCreate(context.Background(), binarySessionBackupRequest()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

// Test_BackupSession_CloseCleanup verifies closing the store reaps sessions and
// deletes their artifacts.
func Test_BackupSession_CloseCleanup(t *testing.T) {
	s := newBackupTestStore(t)
	info, err := s.BackupSessionCreate(context.Background(), binarySessionBackupRequest())
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	sess := s.backupSessions[info.SessionId]
	path := sess.path
	if err := s.Close(true); err != nil {
		t.Fatalf("close failed: %s", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("artifact survived Close: %v", err)
	}
	leftovers, err := filepath.Glob(filepath.Join(s.dbDir, backupSessionScratchPattern))
	if err != nil {
		t.Fatalf("glob failed: %s", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("scratch artifacts left behind after Close: %v", leftovers)
	}
}
