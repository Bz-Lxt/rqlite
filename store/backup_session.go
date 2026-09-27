package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/random"
)

var (
	// ErrBackupSessionNotFound is returned when a backup session does not
	// exist, either because it was never created or because it has expired
	// and been reaped.
	ErrBackupSessionNotFound = errors.New("backup session not found")

	// ErrBackupSessionInvalidRange is returned when a read is requested for a
	// byte range outside the pinned backup artifact.
	ErrBackupSessionInvalidRange = errors.New("invalid backup session byte range")

	// ErrBackupSessionLimit is returned when no more backup sessions can be
	// created until existing sessions expire or are released.
	ErrBackupSessionLimit = errors.New("backup session limit reached")
)

const (
	backupSessionScratchPattern = "rqlite-backup-session-*"

	backupSessionTTL          = 30 * time.Minute
	backupSessionIdleTimeout  = 10 * time.Minute
	backupSessionReapInterval = time.Minute
	backupSessionMax          = 64
)

// backupSession pins a single, immutable binary backup artifact on disk. All
// reads are served from the same file, so resuming an interrupted transfer can
// never mix bytes from different database states.
type backupSession struct {
	id        string
	path      string
	size      int64
	checksum  string // SHA-256 of the complete artifact, hex encoded.
	raftIndex uint64

	createdAt time.Time

	mu         sync.Mutex
	lastAccess time.Time
	reading    int // Number of in-flight reads.
	closed     bool
}

func (sess *backupSession) info() *proto.BackupSessionInfo {
	return &proto.BackupSessionInfo{
		SessionId: sess.id,
		Size:      sess.size,
		Checksum:  sess.checksum,
		RaftIndex: sess.raftIndex,
	}
}

// initBackupSessions initializes the backup session map and starts the
// background reaper. It must be called once, during a successful Open.
func (s *Store) initBackupSessions() {
	s.backupSessionMu.Lock()
	defer s.backupSessionMu.Unlock()
	s.backupSessions = make(map[string]*backupSession)
	s.backupSessionStopCh = make(chan struct{})
	s.backupSessionDoneCh = make(chan struct{})
	go s.runBackupSessionReaper(s.backupSessionStopCh, s.backupSessionDoneCh)
}

// closeBackupSessions stops the reaper and removes every session and its
// artifact file. In-flight reads are allowed to finish against the already
// open file descriptors; the files are unlogged immediately and their storage
// is reclaimed once those reads complete.
func (s *Store) closeBackupSessions() {
	s.backupSessionMu.RLock()
	stopCh := s.backupSessionStopCh
	doneCh := s.backupSessionDoneCh
	s.backupSessionMu.RUnlock()
	if stopCh != nil {
		close(stopCh)
		<-doneCh
	}

	s.backupSessionMu.Lock()
	defer s.backupSessionMu.Unlock()
	for _, sess := range s.backupSessions {
		s.deleteBackupSessionLocked(sess)
	}
	s.backupSessions = nil
	s.backupSessionStopCh = nil
	s.backupSessionDoneCh = nil
}

func (s *Store) runBackupSessionReaper(stopCh, doneCh chan struct{}) {
	defer close(doneCh)
	ticker := time.NewTicker(backupSessionReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case now := <-ticker.C:
			s.reapBackupSessions(now)
		}
	}
}

// reapBackupSessions removes sessions which have exceeded their absolute TTL or
// their idle timeout. Sessions with in-flight reads are skipped; they become
// eligible on the next sweep once their reads complete.
func (s *Store) reapBackupSessions(now time.Time) {
	s.backupSessionMu.Lock()
	defer s.backupSessionMu.Unlock()
	for _, sess := range s.backupSessions {
		sess.mu.Lock()
		expired := now.Sub(sess.createdAt) >= backupSessionTTL ||
			now.Sub(sess.lastAccess) >= backupSessionIdleTimeout
		busy := sess.reading > 0
		sess.mu.Unlock()
		if expired && !busy {
			s.deleteBackupSessionLocked(sess)
		}
	}
}

// BackupSessionCreate pins an immutable binary backup of the current database
// state and returns metadata identifying the new session. Only BINARY format
// backups are supported.
func (s *Store) BackupSessionCreate(ctx context.Context,
	br *proto.BackupRequest) (*proto.BackupSessionInfo, error) {
	if !s.open.Is() {
		return nil, ErrNotOpen
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if br == nil {
		return nil, errors.New("BackupRequest is nil")
	}
	if br.Format != proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY {
		return nil, ErrInvalidBackupFormat
	}
	if br.Leader && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	path, size, checksum, raftIdx, err := s.createBackupArtifact(ctx, br)
	if err != nil {
		return nil, err
	}
	sess := &backupSession{
		id:        random.String(),
		path:      path,
		size:      size,
		checksum:  checksum,
		raftIndex: raftIdx,
		createdAt: time.Now(),
	}
	sess.mu.Lock()
	sess.lastAccess = sess.createdAt
	sess.mu.Unlock()

	if err := s.registerBackupSession(sess); err != nil {
		os.Remove(path)
		return nil, err
	}
	stats.Add(numBackups, 1)
	return sess.info(), nil
}

// BackupSessionPrepareRead validates a byte range against a pinned backup
// session and returns the session metadata together with the exact number of
// bytes a subsequent BackupSessionRead will write. It performs no data
// transfer and allows callers to send response headers before streaming the
// body.
func (s *Store) BackupSessionPrepareRead(ctx context.Context, id string,
	offset, length int64) (*proto.BackupSessionInfo, int64, error) {
	if !s.open.Is() {
		return nil, 0, ErrNotOpen
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	sess, err := s.lookupBackupSession(id)
	if err != nil {
		return nil, 0, err
	}
	if _, n, err := sess.resolveRange(offset, length); err != nil {
		return nil, 0, err
	} else {
		return sess.info(), n, nil
	}
}

// BackupSessionRead writes the requested byte range of the pinned backup
// artifact to dst. A range length of zero or less reads from offset to the end
// of the artifact. The range must first be validated with
// BackupSessionPrepareRead.
func (s *Store) BackupSessionRead(ctx context.Context, id string, offset, length int64,
	dst io.Writer) (int64, error) {
	if !s.open.Is() {
		return 0, ErrNotOpen
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sess, err := s.lookupBackupSession(id)
	if err != nil {
		return 0, err
	}
	_, n, err := sess.resolveRange(offset, length)
	if err != nil {
		return 0, err
	}

	sess.mu.Lock()
	if sess.closed {
		sess.mu.Unlock()
		return 0, ErrBackupSessionNotFound
	}
	sess.reading++
	sess.lastAccess = time.Now()
	sess.mu.Unlock()
	defer func() {
		sess.mu.Lock()
		sess.reading--
		sess.mu.Unlock()
	}()

	fd, err := os.Open(sess.path)
	if err != nil {
		return 0, err
	}
	defer fd.Close()
	if _, err := fd.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	return io.CopyN(dst, fd, n)
}

// BackupSessionRelease terminates a backup session and removes its pinned
// artifact. It returns ErrBackupSessionNotFound if the session does not exist.
func (s *Store) BackupSessionRelease(ctx context.Context, id string) error {
	if !s.open.Is() {
		return ErrNotOpen
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return ErrBackupSessionNotFound
	}
	s.backupSessionMu.Lock()
	defer s.backupSessionMu.Unlock()
	sess, ok := s.backupSessions[id]
	if !ok {
		return ErrBackupSessionNotFound
	}
	s.deleteBackupSessionLocked(sess)
	return nil
}

func (s *Store) lookupBackupSession(id string) (*backupSession, error) {
	if id == "" {
		return nil, ErrBackupSessionNotFound
	}
	s.backupSessionMu.RLock()
	sess, ok := s.backupSessions[id]
	s.backupSessionMu.RUnlock()
	if !ok {
		return nil, ErrBackupSessionNotFound
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.closed {
		return nil, ErrBackupSessionNotFound
	}
	return sess, nil
}

// resolveRange validates [offset, length) against the artifact and returns the
// inclusive end offset and the number of bytes the range spans. A non-positive
// length means "to the end of the artifact". Artifact size is immutable after
// session creation, so no locking is required.
func (sess *backupSession) resolveRange(offset, length int64) (int64, int64, error) {
	if offset < 0 || offset >= sess.size {
		return 0, 0, ErrBackupSessionInvalidRange
	}
	end := sess.size - 1
	if length > 0 && offset+length-1 < end {
		end = offset + length - 1
	}
	return end, end - offset + 1, nil
}

// registerBackupSession adds a session, evicting the session which has been
// idle for the longest when the limit is reached. If all sessions are busy
// with in-flight reads, ErrBackupSessionLimit is returned.
func (s *Store) registerBackupSession(sess *backupSession) error {
	s.backupSessionMu.Lock()
	defer s.backupSessionMu.Unlock()

	if len(s.backupSessions) >= backupSessionMax {
		var oldest *backupSession
		var oldestAccess time.Time
		for _, cand := range s.backupSessions {
			cand.mu.Lock()
			if cand.reading == 0 && (oldest == nil || cand.lastAccess.Before(oldestAccess)) {
				oldest = cand
				oldestAccess = cand.lastAccess
			}
			cand.mu.Unlock()
		}
		if oldest == nil {
			return ErrBackupSessionLimit
		}
		s.deleteBackupSessionLocked(oldest)
	}
	s.backupSessions[sess.id] = sess
	return nil
}

// deleteBackupSessionLocked removes the session from the map, marks it closed so
// in-flight read handling cannot reuse it, and unlinks its artifact file.
// Callers must hold s.backupSessionMu.
func (s *Store) deleteBackupSessionLocked(sess *backupSession) {
	delete(s.backupSessions, sess.id)
	sess.mu.Lock()
	sess.closed = true
	sess.mu.Unlock()
	if err := os.Remove(sess.path); err != nil && !os.IsNotExist(err) {
		s.logger.Printf("failed to remove backup session artifact %s: %s", sess.path, err.Error())
	}
}

// createBackupArtifact writes a binary backup of the current database state to
// a new scratch file and returns its path, size and SHA-256 checksum. The
// returned file is immutable; the caller is responsible for removing it once
// the backing session is released or reaped.
func (s *Store) createBackupArtifact(ctx context.Context,
	br *proto.BackupRequest) (path string, size int64, checksum string, raftIdx uint64, retErr error) {
	if br.Vacuum {
		return s.createVacuumBackupArtifact()
	}
	return s.createLiveBackupArtifact(ctx)
}

// createVacuumBackupArtifact renders the backup through the SQLite online
// backup API into a scratch file, then hashes the result.
func (s *Store) createVacuumBackupArtifact() (path string, size int64, checksum string, raftIdx uint64, retErr error) {
	tmp, err := createTemp(s.dbDir, backupSessionScratchPattern)
	if err != nil {
		return "", 0, "", 0, err
	}
	path = tmp.Name()
	if err := tmp.Close(); err != nil {
		os.Remove(path)
		return "", 0, "", 0, err
	}
	defer func() {
		if retErr != nil {
			os.Remove(path)
		}
	}()

	if err := s.db.Backup(path, true); err != nil {
		return "", 0, "", 0, err
	}
	size, checksum, err = hashFile(path)
	if err != nil {
		return "", 0, "", 0, err
	}
	return path, size, checksum, s.dbAppliedIdx.Load(), nil
}

// createLiveBackupArtifact checkpoints the WAL, briefly blocks snapshotting so
// the main database file cannot change, and copies it into a scratch file
// while computing its checksum. The snapshot lock is released as soon as the
// copy completes -- unlike a streaming backup, it is not held for the whole
// transfer.
func (s *Store) createLiveBackupArtifact(ctx context.Context) (path string, size int64, checksum string, raftIdx uint64, retErr error) {
	// If there is data in the WAL we need to do a snapshot to ensure that the
	// backup we take of the main database file is up-to-date.
	sz, err := s.db.WALSize()
	if err != nil {
		return "", 0, "", 0, err
	}
	if sz > 0 {
		if err := s.Snapshot(0); err != nil {
			if !errors.Is(err, ErrNothingNewToSnapshot) &&
				!strings.Contains(err.Error(), "wait until the configuration entry at") {
				return "", 0, "", 0, fmt.Errorf("pre-backup snapshot failed: %s", err.Error())
			}
		}
	}

	if err := s.snapshotCAS.BeginWithRetry("backup-session", backupCASTimeout, backupCASRetryDelay); err != nil {
		return "", 0, "", 0, ErrBackupCASFailed
	}
	defer s.snapshotCAS.End()

	if err := ctx.Err(); err != nil {
		return "", 0, "", 0, err
	}

	srcFD, err := os.Open(s.dbPath)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("failed to open database file: %s", err.Error())
	}
	defer srcFD.Close()

	tmp, err := createTemp(s.dbDir, backupSessionScratchPattern)
	if err != nil {
		return "", 0, "", 0, err
	}
	path = tmp.Name()
	defer func() {
		tmp.Close()
		if retErr != nil {
			os.Remove(path)
		}
	}()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), srcFD)
	if err != nil {
		return "", 0, "", 0, err
	}
	if err := tmp.Sync(); err != nil {
		return "", 0, "", 0, err
	}
	return path, n, hex.EncodeToString(h.Sum(nil)), s.dbAppliedIdx.Load(), nil
}

// hashFile returns the size and hex-encoded SHA-256 checksum of the file at
// the given path.
func hashFile(path string) (int64, string, error) {
	fd, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer fd.Close()

	h := sha256.New()
	n, err := io.Copy(h, fd)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}
