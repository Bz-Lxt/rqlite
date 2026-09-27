package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/mkideal/cli"
	httpcl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

type restoreResponse struct {
	Results []*Result `json:"results"`
}

type sqliteStatus struct {
	FkConstraint string `json:"fk_constraints"`
}

type store struct {
	SqliteStatus sqliteStatus `json:"sqlite3"`
}

type statusResponse struct {
	Store *store `json:"store"`
}

// Binary backups are downloaded through a resumable, server-pinned session.
// A sidecar file next to the backup records the session ID, the full artifact
// size, its SHA-256 checksum and the Raft index the backup was taken from. An
// interrupted invocation resumes with byte-range reads against the same
// immutable artifact; if the server can no longer serve that exact artifact
// the download restarts from scratch. Servers without session support fall
// back to a single streaming download.
var (
	// backupChunkSize is the maximum byte range fetched per request.
	backupChunkSize = int64(32 << 20)
)

const (
	backupSidecarExt = ".rqlite-backup"

	backupSessionHeader  = "X-RQLITE-BACKUP-SESSION"
	backupSizeHeader     = "X-RQLITE-BACKUP-SIZE"
	backupChecksumHeader = "X-RQLITE-BACKUP-CHECKSUM"
	backupIndexHeader    = "X-RQLITE-BACKUP-RAFT-INDEX"
)

type backupSessionMeta struct {
	ID        string `json:"id"`
	Size      int64  `json:"size"`
	Checksum  string `json:"checksum"`
	RaftIndex uint64 `json:"raft_index"`
}

func backup(logf func(string, ...any), client *httpcl.Client, filename string) error {
	u := fmt.Sprintf("%sdb/backup", client.Prefix)
	// Backups can be much larger than a normal response and are read in many
	// chunks, so never apply the client's overall request timeout.
	client = client.WithoutTimeout()

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	sidecarPath := filename + backupSidecarExt
	prevMeta, _ := loadBackupSidecar(sidecarPath)
	if prevMeta != nil {
		logf("resuming backup session %s\n", prevMeta.ID)
	}

	meta, resp, err := establishBackupSession(client, u, prevMeta)
	if err != nil {
		return err
	}
	if meta == nil {
		// The server does not support backup sessions and has already started
		// streaming the complete backup in resp.
		defer removeBackupSidecar(sidecarPath)
		return streamLegacyBackup(fd, resp)
	}

	// A new session may describe a different database state than a previous
	// interrupted attempt; only already-downloaded bytes of the exact same
	// artifact can be reused.
	written, err := alignBackupFile(fd, prevMeta, meta)
	if err != nil {
		return err
	}
	if err := storeBackupSidecar(sidecarPath, meta); err != nil {
		return err
	}

	if written < meta.Size {
		// If establishBackupSession returned a pre-opened ranged response (an
		// existing session was still valid) use it; otherwise fetch the first
		// chunk now.
		if resp == nil {
			resp, err = requestBackupChunk(client, u, meta, written)
			if err != nil {
				return err
			}
		}
		written, err = downloadBackupChunks(logf, client, u, fd, meta,
			sidecarPath, written, resp)
		if err != nil {
			return err
		}
	} else if resp != nil {
		resp.Body.Close()
	}

	if written != meta.Size {
		return fmt.Errorf("backup size mismatch: got %d bytes, expected %d", written, meta.Size)
	}
	sum, err := hashBackupFile(fd, meta.Size)
	if err != nil {
		return err
	}
	if sum != meta.Checksum {
		return fmt.Errorf("backup checksum mismatch: got %s, expected %s", sum, meta.Checksum)
	}

	releaseBackupSession(client, u, meta.ID)
	if err := os.Remove(sidecarPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	logf("backup file written successfully\n")
	return nil
}

// establishBackupSession returns a usable session and, when a previous session
// is still valid, an already-opened 206 chunk response for the next byte. If
// the server has no session support it returns a nil meta with a 200 streaming
// response instead.
func establishBackupSession(client *httpcl.Client, u string,
	prev *backupSessionMeta) (*backupSessionMeta, *http.Response, error) {

	if prev != nil {
		resp, err := requestBackupChunk(client, u, prev, 0)
		if err != nil {
			return nil, nil, err
		}
		switch resp.StatusCode {
		case http.StatusPartialContent:
			// Session is alive, but the requested range starts at 0; the caller
			// aligns the file and then requests the real next chunk.
			resp.Body.Close()
			return prev, nil, nil
		case http.StatusOK:
			// Server ignores sessions: legacy full stream.
			return nil, resp, nil
		case http.StatusNotFound, http.StatusConflict, http.StatusRequestedRangeNotSatisfiable:
			resp.Body.Close()
			// Expired/invalid session or stale state: create a fresh one.
		case http.StatusUnauthorized:
			resp.Body.Close()
			return nil, nil, fmt.Errorf("unauthorized")
		default:
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			return nil, nil, fmt.Errorf("server responded with: %s %s", resp.Status, strings.TrimSpace(string(body)))
		}
	}

	resp, err := client.DoRequest(http.MethodGet, u+"?session=new", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("unauthorized")
	}
	if resp.StatusCode == http.StatusOK {
		// Legacy server that does not understand the session parameter: it has
		// answered with the full backup.
		return nil, resp, nil
	}
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, nil, fmt.Errorf("failed to create backup session: %s %s", resp.Status, strings.TrimSpace(string(body)))
	}
	meta, err := readBackupSessionMeta(resp)
	resp.Body.Close()
	if err != nil {
		return nil, nil, err
	}
	return meta, nil, nil
}

// downloadBackupChunks streams the remaining artifact bytes in ranges. A
// session that expires or is rejected mid-download is transparently recreated;
// already-downloaded bytes are reused only when the new artifact is
// byte-identical to the old one. It returns the total number of bytes written.
func downloadBackupChunks(logf func(string, ...any), client *httpcl.Client, u string,
	fd *os.File, meta *backupSessionMeta, sidecarPath string,
	offset int64, resp *http.Response) (int64, error) {

	for {
		switch resp.StatusCode {
		case http.StatusNotFound, http.StatusConflict,
			http.StatusRequestedRangeNotSatisfiable:
			// The session expired or was rejected as stale. Recreate it.
			resp.Body.Close()
			newMeta, err := createBackupSession(client, u)
			if err != nil {
				return 0, err
			}
			if newMeta.Checksum != meta.Checksum || newMeta.Size != meta.Size {
				// The database moved on: the new artifact is different, so the
				// partial bytes are unusable and the download restarts.
				if err := fd.Truncate(0); err != nil {
					return 0, err
				}
				offset = 0
			}
			meta = newMeta
			if err := storeBackupSidecar(sidecarPath, meta); err != nil {
				return 0, err
			}
			resp, err = requestBackupChunk(client, u, meta, offset)
			if err != nil {
				return 0, err
			}
			continue
		case http.StatusPartialContent:
			// Fall through to stream this chunk.
		case http.StatusUnauthorized:
			resp.Body.Close()
			return 0, fmt.Errorf("unauthorized")
		default:
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			return 0, fmt.Errorf("server responded with: %s %s", resp.Status, strings.TrimSpace(string(body)))
		}

		start, err := parseContentRange(resp.Header.Get("Content-Range"), meta.Size)
		if err != nil {
			resp.Body.Close()
			return 0, err
		}
		if start != offset {
			resp.Body.Close()
			return 0, fmt.Errorf("backup range mismatch: got start %d, expected %d", start, offset)
		}
		if err := verifyResponseMeta(resp, meta); err != nil {
			resp.Body.Close()
			return 0, err
		}
		chunkLen := resp.ContentLength
		if chunkLen <= 0 {
			resp.Body.Close()
			return 0, fmt.Errorf("backup chunk response has invalid Content-Length: %d", chunkLen)
		}

		n, err := io.CopyN(fd, resp.Body, chunkLen)
		resp.Body.Close()
		if err != nil {
			return 0, err
		}
		if n != chunkLen {
			return 0, fmt.Errorf("backup chunk short read: got %d bytes, expected %d", n, chunkLen)
		}
		if err := fd.Sync(); err != nil {
			return 0, err
		}
		offset += n
		logf("downloaded %d of %d bytes\n", offset, meta.Size)
		if offset == meta.Size {
			return offset, nil
		}
		if offset > meta.Size {
			return 0, fmt.Errorf("backup overflow: %d bytes for %d-byte artifact", offset, meta.Size)
		}

		resp, err = requestBackupChunk(client, u, meta, offset)
		if err != nil {
			return 0, err
		}
	}
}

// createBackupSession asks the server to pin a new backup artifact and returns
// its metadata.
func createBackupSession(client *httpcl.Client, u string) (*backupSessionMeta, error) {
	resp, err := client.DoRequest(http.MethodGet, u+"?session=new", nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("failed to create backup session: %s %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return readBackupSessionMeta(resp)
}

// requestBackupChunk issues a ranged GET for up to backupChunkSize bytes
// starting at offset, pinning the response to the session's checksum.
func requestBackupChunk(client *httpcl.Client, u string, meta *backupSessionMeta,
	offset int64) (*http.Response, error) {

	end := offset + backupChunkSize - 1
	if end >= meta.Size {
		end = meta.Size - 1
	}
	hdr := http.Header{
		"Range":    {fmt.Sprintf("bytes=%d-%d", offset, end)},
		"If-Match": {`"` + meta.Checksum + `"`},
	}
	qs := url.Values{}
	qs.Set("session", meta.ID)
	return client.DoRequest(http.MethodGet, u+"?"+qs.Encode(), nil, hdr)
}

// alignBackupFile keeps only the prefix shared with the pinned artifact. It
// returns the usable byte count.
func alignBackupFile(fd *os.File, prev, meta *backupSessionMeta) (int64, error) {
	st, err := fd.Stat()
	if err != nil {
		return 0, err
	}
	n := st.Size()
	same := prev != nil && prev.Checksum == meta.Checksum && prev.Size == meta.Size
	if !same || n > meta.Size {
		if err := fd.Truncate(0); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if _, err := fd.Seek(n, io.SeekStart); err != nil {
		return 0, err
	}
	return n, nil
}

// streamLegacyBackup copies a non-session full-backup response into the file,
// replacing any partial content.
func streamLegacyBackup(fd *os.File, resp *http.Response) error {
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with: %s", resp.Status)
	}
	return streamLegacyBackupTo(fd, resp.Body)
}

func streamLegacyBackupTo(fd *os.File, body io.Reader) error {
	if err := fd.Truncate(0); err != nil {
		return err
	}
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(fd, body); err != nil {
		return err
	}
	return nil
}

// readBackupSessionMeta parses the session metadata headers on a CREATE or
// ranged READ response.
func readBackupSessionMeta(resp *http.Response) (*backupSessionMeta, error) {
	h := resp.Header
	id := h.Get(backupSessionHeader)
	if id == "" {
		return nil, fmt.Errorf("backup session response missing %s header", backupSessionHeader)
	}
	size, err := strconv.ParseInt(h.Get(backupSizeHeader), 10, 64)
	if err != nil || size < 0 {
		return nil, fmt.Errorf("invalid backup session size header: %q", h.Get(backupSizeHeader))
	}
	checksum := strings.TrimPrefix(strings.TrimSpace(h.Get(backupChecksumHeader)), "sha256:")
	if len(checksum) != sha256.Size*2 {
		return nil, fmt.Errorf("invalid backup session checksum header: %q", h.Get(backupChecksumHeader))
	}
	idx, err := strconv.ParseUint(h.Get(backupIndexHeader), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid backup session raft index header: %q", h.Get(backupIndexHeader))
	}
	return &backupSessionMeta{ID: id, Size: size, Checksum: checksum, RaftIndex: idx}, nil
}

// verifyResponseMeta ensures a chunk still belongs to the pinned artifact.
func verifyResponseMeta(resp *http.Response, meta *backupSessionMeta) error {
	got, err := readBackupSessionMeta(resp)
	if err != nil {
		return err
	}
	if got.ID != meta.ID || got.Size != meta.Size || got.Checksum != meta.Checksum ||
		got.RaftIndex != meta.RaftIndex {
		return fmt.Errorf("backup session metadata changed mid-download")
	}
	return nil
}

// parseContentRange parses a "bytes start-end/total" Content-Range value and
// returns its start offset, checking the total matches.
func parseContentRange(v string, total int64) (int64, error) {
	const prefix = "bytes "
	if !strings.HasPrefix(v, prefix) {
		return 0, fmt.Errorf("invalid Content-Range header: %q", v)
	}
	rangePart, totalPart, ok := strings.Cut(strings.TrimPrefix(v, prefix), "/")
	if !ok {
		return 0, fmt.Errorf("invalid Content-Range header: %q", v)
	}
	startStr, _, ok := strings.Cut(rangePart, "-")
	if !ok {
		return 0, fmt.Errorf("invalid Content-Range header: %q", v)
	}
	start, err := strconv.ParseInt(strings.TrimSpace(startStr), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Content-Range header: %q", v)
	}
	if got, err := strconv.ParseInt(strings.TrimSpace(totalPart), 10, 64); err != nil || got != total {
		return 0, fmt.Errorf("Content-Range total mismatch: %q", v)
	}
	return start, nil
}

// hashBackupFile returns the lowercase SHA-256 hex digest of the first size
// bytes of the file.
func hashBackupFile(fd *os.File, size int64) (string, error) {
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.CopyN(h, fd, size); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func loadBackupSidecar(path string) (*backupSessionMeta, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := &backupSessionMeta{}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, err
	}
	if m.ID == "" || m.Checksum == "" || m.Size <= 0 {
		return nil, fmt.Errorf("invalid backup sidecar file %s", path)
	}
	return m, nil
}

func storeBackupSidecar(path string, meta *backupSessionMeta) error {
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func removeBackupSidecar(path string) {
	_ = os.Remove(path)
}

// releaseBackupSession best-effort releases the server-side session; the
// server reaps abandoned sessions automatically.
func releaseBackupSession(client *httpcl.Client, u, id string) {
	qs := url.Values{}
	qs.Set("session", id)
	resp, err := client.DoRequest(http.MethodDelete, u+"?"+qs.Encode(), nil, nil)
	if err == nil {
		resp.Body.Close()
	}
}

func dump(ctx *cli.Context, client *httpcl.Client, filename string, tables []string) error {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	queryStr := url.Values{}
	queryStr.Set("fmt", "sql")
	if len(tables) > 0 {
		queryStr.Set("tables", strings.Join(tables, ","))
	}
	u := fmt.Sprintf("%sdb/backup?%s", client.Prefix, queryStr.Encode())

	resp, err := client.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with: %s", resp.Status)
	}

	if _, err := io.Copy(fd, resp.Body); err != nil {
		return err
	}

	ctx.String("SQL text file written successfully\n")
	return nil
}

func validSQLiteFile(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	b := make([]byte, 16)
	_, err = file.Read(b)
	if err != nil {
		return false
	}
	return validSQLiteData(b)
}

func validSQLiteData(b []byte) bool {
	return len(b) > 13 && string(b[0:13]) == "SQLite format"
}

func restore(ctx *cli.Context, client *httpcl.Client, filename string) error {
	statusRet, err := checkStatus(client)
	if err != nil {
		return err
	}

	restoreFile, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	if !validSQLiteData(restoreFile) {
		fkEnabled := statusRet.Store.SqliteStatus.FkConstraint == "enabled"
		if fkEnabled {
			restoreFile = append(restoreFile, []byte("PRAGMA foreign_keys=ON;")...)
		} else {
			restoreFile = append(restoreFile, []byte("PRAGMA foreign_keys=OFF;")...)
		}
	}

	contentType := "text/plain"
	if validSQLiteData(restoreFile) {
		contentType = "application/octet-stream"
	}

	// Restoring uploads the whole file and waits for the node to load it, which
	// can take far longer than a normal request, so use a client without an
	// overall request timeout to avoid aborting the upload mid-transfer.
	u := fmt.Sprintf("%sdb/load", client.Prefix)
	headers := http.Header{"Content-Type": {contentType}}
	resp, err := client.WithoutTimeout().PostWithHeaders(u, bytes.NewReader(restoreFile), headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with: %s", resp.Status)
	}

	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	restoreRet := &restoreResponse{}
	if err := parseResponse(response, &restoreRet); err != nil {
		return err
	}
	if !validSQLiteData(restoreFile) {
		if len(restoreRet.Results) < 1 {
			return fmt.Errorf("unexpected results length: %d", len(restoreRet.Results))
		}
		if resultError := restoreRet.Results[0].Error; resultError != "" {
			ctx.String("Error: %s\n", resultError)
			return nil
		}
		ctx.String("last inserted ID: %d\n", restoreRet.Results[0].LastInsertID)
		ctx.String("rows affected: %d\n", restoreRet.Results[0].RowsAffected)
	}

	ctx.String("Database restored successfully.\n")
	return nil
}

func boot(ctx *cli.Context, client *httpcl.Client, filename string) error {
	if _, err := checkStatus(client); err != nil {
		return err
	}

	if !validSQLiteFile(filename) {
		return fmt.Errorf("%s is not a valid SQLite file", filename)
	}

	fd, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	// Booting streams a potentially large SQLite file, and the node must finish
	// loading it before responding, so this can take far longer than a normal
	// request. Use a client without an overall request timeout to avoid aborting
	// the upload mid-transfer.
	u := fmt.Sprintf("%sboot", client.Prefix)
	resp, err := client.WithoutTimeout().Post(u, fd)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("boot failed, status code: %s", resp.Status)
		if len(body) > 0 {
			errMsg += fmt.Sprintf(", %s", string(body))
		}
		return errors.New(errMsg)
	}

	ctx.String("Node booted successfully.\n")
	return nil
}

func checkStatus(client *httpcl.Client) (*statusResponse, error) {
	u := fmt.Sprintf("%sstatus", client.Prefix)
	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("unauthorized")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	statusRet := &statusResponse{}
	if err := parseResponse(body, &statusRet); err != nil {
		return nil, err
	}
	if statusRet.Store == nil {
		return nil, fmt.Errorf("unexpected server response: store status not found")
	}

	return statusRet, nil
}
