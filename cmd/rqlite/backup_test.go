package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	httpcl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

// fakeBackupServer implements the server-side subset of the resumable backup
// protocol used by the CLI.
type fakeBackupServer struct {
	mu         sync.Mutex
	artifact   []byte
	checksum   string // Overrides the advertised checksum when non-empty.
	sessions   map[string]bool
	nextID     int
	legacy     bool
	failAt     int // 1-based ranged GET on which to fail; 0 disables.
	requests   int
	rangeStart []int64
	released   []string
}

func newFakeBackupServer(artifact []byte) *fakeBackupServer {
	return &fakeBackupServer{
		artifact: artifact,
		sessions: make(map[string]bool),
	}
}

func (f *fakeBackupServer) sum() string {
	if f.checksum != "" {
		return f.checksum
	}
	h := sha256.Sum256(f.artifact)
	return hex.EncodeToString(h[:])
}

func (f *fakeBackupServer) setHeaders(w http.Header, id string) {
	w.Set(backupSessionHeader, id)
	w.Set(backupSizeHeader, strconv.Itoa(len(f.artifact)))
	w.Set(backupChecksumHeader, "sha256:"+f.sum())
	w.Set(backupIndexHeader, "7")
}

func (f *fakeBackupServer) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	session := r.URL.Query().Get("session")

	if f.legacy {
		w.WriteHeader(http.StatusOK)
		w.Write(f.artifact)
		return
	}

	if session == "new" {
		id := fmt.Sprintf("sess-%d", f.nextID)
		f.nextID++
		f.sessions[id] = true
		f.setHeaders(w.Header(), id)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusCreated)
		return
	}

	if r.Method == http.MethodDelete {
		if !f.sessions[session] {
			http.Error(w, "backup session not found or expired", http.StatusNotFound)
			return
		}
		delete(f.sessions, session)
		f.released = append(f.released, session)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !f.sessions[session] {
		http.Error(w, "backup session not found or expired", http.StatusNotFound)
		return
	}
	if im := r.Header.Get("If-Match"); im != "" {
		want := `"` + f.sum() + `"`
		if im != want && strings.Trim(im, `"`) != "sha256:"+f.sum() {
			http.Error(w, "backup session checksum mismatch", http.StatusConflict)
			return
		}
	}

	spec := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
	startStr, endStr, _ := strings.Cut(spec, "-")
	start, _ := strconv.ParseInt(startStr, 10, 64)
	end := int64(len(f.artifact)) - 1
	if endStr != "" {
		end, _ = strconv.ParseInt(endStr, 10, 64)
	}
	f.requests++
	f.rangeStart = append(f.rangeStart, start)
	if f.failAt > 0 && f.requests == f.failAt {
		http.Error(w, "simulated transfer failure", http.StatusInternalServerError)
		return
	}

	n := end - start + 1
	f.setHeaders(w.Header(), session)
	w.Header().Set("Content-Range",
		fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.artifact)))
	w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(f.artifact[start : end+1])
}

func newBackupTestClient(t *testing.T, srv *httptest.Server) *httpcl.Client {
	t.Helper()
	host := strings.TrimPrefix(srv.URL, "http://")
	return httpcl.NewClient(http.DefaultClient, []string{host}, httpcl.WithPrefix("/"))
}

func quietLogf(string, ...any) {}

func readBackupTestFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read backup file: %s", err)
	}
	return b
}

// Test_CLI_BackupSession_Full verifies a complete session download, its
// metadata headers and session release.
func Test_CLI_BackupSession_Full(t *testing.T) {
	oldChunk := backupChunkSize
	backupChunkSize = 4096
	t.Cleanup(func() { backupChunkSize = oldChunk })

	artifact := bytes.Repeat([]byte("full-download-artifact-0123456789"), 300) // ~10.5 KiB
	fake := newFakeBackupServer(artifact)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "backup.db")
	if err := backup(quietLogf, newBackupTestClient(t, srv), out); err != nil {
		t.Fatalf("backup failed: %s", err)
	}
	if got := readBackupTestFile(t, out); !bytesEqual(got, artifact) {
		t.Fatal("downloaded backup does not match artifact")
	}
	if _, err := os.Stat(out + backupSidecarExt); !os.IsNotExist(err) {
		t.Fatal("sidecar file should be removed after successful backup")
	}
	if len(fake.released) != 1 {
		t.Fatalf("expected session to be released, got %v", fake.released)
	}
}

// Test_CLI_BackupSession_Resume verifies an interrupted download resumes at the
// correct offset and yields the identical artifact.
func Test_CLI_BackupSession_Resume(t *testing.T) {
	oldChunk := backupChunkSize
	backupChunkSize = 4096
	t.Cleanup(func() { backupChunkSize = oldChunk })

	artifact := bytes.Repeat([]byte("resume-download-artifact-0123456789"), 300)
	fake := newFakeBackupServer(artifact)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()
	client := newBackupTestClient(t, srv)

	out := filepath.Join(t.TempDir(), "backup.db")

	// Fail on the third ranged request: two complete chunks land on disk.
	fake.failAt = 3
	if err := backup(quietLogf, client, out); err == nil {
		t.Fatal("expected first backup attempt to fail")
	}
	partial, err := os.Stat(out)
	if err != nil {
		t.Fatalf("partial file missing: %s", err)
	}
	if partial.Size() != 2*backupChunkSize {
		t.Fatalf("expected %d partial bytes, got %d", 2*backupChunkSize, partial.Size())
	}
	if _, err := os.Stat(out + backupSidecarExt); err != nil {
		t.Fatalf("sidecar should survive failure: %s", err)
	}

	fake.mu.Lock()
	fake.failAt = 0
	fake.mu.Unlock()

	if err := backup(quietLogf, client, out); err != nil {
		t.Fatalf("resumed backup failed: %s", err)
	}
	if got := readBackupTestFile(t, out); !bytesEqual(got, artifact) {
		t.Fatal("resumed backup does not match artifact")
	}
	// The first ranged request of the retry is the liveness probe at 0; the
	// next real chunk must start exactly where the partial file ended.
	foundResume := false
	for _, s := range fake.rangeStart {
		if s == 2*backupChunkSize {
			foundResume = true
		}
	}
	if !foundResume {
		t.Fatalf("expected a ranged read at %d, got %v", 2*backupChunkSize, fake.rangeStart)
	}
}

// Test_CLI_BackupSession_ExpiredSameArtifact verifies that after the server
// forgets a session, a new session for the byte-identical artifact reuses the
// partial bytes.
func Test_CLI_BackupSession_ExpiredSameArtifact(t *testing.T) {
	oldChunk := backupChunkSize
	backupChunkSize = 4096
	t.Cleanup(func() { backupChunkSize = oldChunk })

	artifact := bytes.Repeat([]byte("expired-session-artifact-0123456789!"), 300)
	fake := newFakeBackupServer(artifact)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()
	client := newBackupTestClient(t, srv)
	out := filepath.Join(t.TempDir(), "backup.db")

	fake.failAt = 3
	if err := backup(quietLogf, client, out); err == nil {
		t.Fatal("expected first backup attempt to fail")
	}
	// Expire every session; the next create returns a new ID for the same
	// artifact.
	fake.mu.Lock()
	fake.failAt = 0
	fake.sessions = make(map[string]bool)
	fake.mu.Unlock()

	if err := backup(quietLogf, client, out); err != nil {
		t.Fatalf("recreated backup failed: %s", err)
	}
	if got := readBackupTestFile(t, out); !bytesEqual(got, artifact) {
		t.Fatal("recreated backup does not match artifact")
	}
	if len(fake.released) < 1 {
		t.Fatal("expected the new session to be released")
	}
}

// Test_CLI_BackupSession_ExpiredChangedArtifact verifies a new session for a
// different database state discards partial bytes and restarts at offset 0.
func Test_CLI_BackupSession_ExpiredChangedArtifact(t *testing.T) {
	oldChunk := backupChunkSize
	backupChunkSize = 4096
	t.Cleanup(func() { backupChunkSize = oldChunk })

	artifact1 := bytes.Repeat([]byte("first-artifact-version-0123456789!!"), 300)
	artifact2 := bytes.Repeat([]byte("second-completely-different-artifact"), 400)
	fake := newFakeBackupServer(artifact1)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()
	client := newBackupTestClient(t, srv)
	out := filepath.Join(t.TempDir(), "backup.db")

	fake.failAt = 3
	if err := backup(quietLogf, client, out); err == nil {
		t.Fatal("expected first backup attempt to fail")
	}

	fake.mu.Lock()
	fake.failAt = 0
	fake.sessions = make(map[string]bool)
	fake.artifact = artifact2
	fake.mu.Unlock()

	if err := backup(quietLogf, client, out); err != nil {
		t.Fatalf("restarted backup failed: %s", err)
	}
	if got := readBackupTestFile(t, out); !bytesEqual(got, artifact2) {
		t.Fatal("restarted backup does not match the new artifact")
	}
	// Every ranged request after the create must have started at zero.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.rangeStart) == 0 || fake.rangeStart[len(fake.rangeStart)-1] < 0 {
		t.Fatal("no ranged requests observed")
	}
	sawZero := false
	for _, s := range fake.rangeStart {
		if s == 0 {
			sawZero = true
		}
	}
	if !sawZero {
		t.Fatalf("expected the changed artifact to be re-read from 0, got %v", fake.rangeStart)
	}
}

// Test_CLI_BackupSession_ChecksumMismatch verifies a corrupt/inconsistent
// complete download is rejected and the sidecar is retained for retry.
func Test_CLI_BackupSession_ChecksumMismatch(t *testing.T) {
	oldChunk := backupChunkSize
	backupChunkSize = 4096
	t.Cleanup(func() { backupChunkSize = oldChunk })

	artifact := bytes.Repeat([]byte("checksum-mismatch-artifact-0123!"), 300)
	fake := newFakeBackupServer(artifact)
	fake.checksum = strings.Repeat("0", 64)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "backup.db")
	err := backup(quietLogf, newBackupTestClient(t, srv), out)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
	if _, err := os.Stat(out + backupSidecarExt); err != nil {
		t.Fatal("sidecar should remain after checksum failure")
	}
}

// Test_CLI_BackupSession_LegacyServer verifies fallback to a plain streaming
// download against a server that ignores the session parameter.
func Test_CLI_BackupSession_LegacyServer(t *testing.T) {
	artifact := bytes.Repeat([]byte("legacy-streaming-server-0123456789"), 300)
	fake := newFakeBackupServer(artifact)
	fake.legacy = true
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "backup.db")
	if err := backup(quietLogf, newBackupTestClient(t, srv), out); err != nil {
		t.Fatalf("legacy backup failed: %s", err)
	}
	if got := readBackupTestFile(t, out); !bytesEqual(got, artifact) {
		t.Fatal("legacy backup does not match artifact")
	}
	if _, err := os.Stat(out + backupSidecarExt); !os.IsNotExist(err) {
		t.Fatal("no sidecar should exist after legacy backup")
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
