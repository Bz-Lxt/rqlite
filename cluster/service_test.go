package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/cluster/proto"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/limit"
	"github.com/rqlite/rqlite/v10/testdata/x509"
	pb "google.golang.org/protobuf/proto"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	if ml.Addr().String() != s.Addr() {
		t.Fatalf("service returned incorrect address")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetAPIAddr(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")
	if exp, got := "foo", s.GetAPIAddr(); exp != got {
		t.Fatalf("got incorrect API address, exp %s, got %s", exp, got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeMeta(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")
	s.SetVersion("1.0.0")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	c.SetLocalVersion("2.0.0")
	if v := c.GetLocalVersion(); v != "2.0.0" {
		t.Fatalf("failed to get correct local version, exp %s, got %s", "2.0.0", v)
	}
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "http://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "http://foo", meta.Url)
	}
	if meta.Version != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "2.0.0", meta.Version)
	}

	s.EnableHTTPS(true)

	// Test fetch via network.
	meta, err = c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", meta.Url)
	}
	if meta.Version != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "1.0.0", meta.Version)
	}

	// Test fetch via local call.
	addr := s.GetNodeAPIURL()
	if addr != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", addr)
	}
	ver := s.GetVersion()
	if ver != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "1.0.0", ver)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeMetaLocal(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Check stats to confirm no local request yet.
	if stats.Get(numGetNodeAPIRequestLocal).String() != "0" {
		t.Fatalf("failed to confirm request served locally")
	}

	// Test by enabling local answering
	c := NewClient(ml, 30*time.Second)
	if err := c.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address locally: %s", err)
	}
	if meta.Url != "http://foo" {
		t.Fatalf("failed to get correct node API address locally, exp %s, got %s", "http://foo", meta.Url)
	}

	// Check stats to confirm local response.
	if stats.Get(numGetNodeAPIRequestLocal).String() != "1" {
		t.Fatalf("failed to confirm request served locally")
	}
}

func Test_NewServiceSetGetNodeMetaTLS(t *testing.T) {
	ml := mustNewMockTLSTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	exp := "http://foo"
	if meta.Url != exp {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", exp, meta.Url)
	}

	s.EnableHTTPS(true)
	meta, err = c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", meta.Url)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceGetCommitIndex(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	s.EnableHTTPS(true)

	// Test fetch via network.
	mgr.commitIndex = 1234
	c := NewClient(ml, 30*time.Second)
	idx, err := c.GetCommitIndex(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if idx != 1234 {
		t.Fatalf("failed to get correct node commit index, exp %d, got %d", 1234, idx)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceTestExecuteQueryAuthNoCredentials(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	// Test that for a cluster with no credential store configured
	// all users are authed for both operations
	var c CredentialStore = nil
	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, _, err := cl.Execute(context.Background(), er, s.Addr(), nil, 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal(err)
	}
	qr := &command.QueryRequest{}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), nil, 5*time.Second, noRetries)
	if err != nil {
		t.Fatal(err)
	}
}

// Test_NewServiceTestExecuteQueryAuth tests that for a cluster with a credential
// store, configured users with execute permissions can execute and users with
// query permissions can query, and can't if they don't have those permissions.
func Test_NewServiceTestExecuteQueryAuth(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	f := func(username string, password string, perm string) bool {
		if username == "alice" && password == "secret1" && perm == "execute" {
			return true
		} else if username == "bob" && password == "secret1" && perm == "query" {
			return true
		}
		return false
	}
	c := &mockCredentialStore{aaFunc: f}

	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, _, err := cl.Execute(context.Background(), er, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal("alice improperly unauthorized to execute")
	}
	_, _, err = cl.Execute(context.Background(), er, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second, defaultMaxRetries)
	if err == nil {
		t.Fatal("bob improperly authorized to execute")
	}
	qr := &command.QueryRequest{}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second, noRetries)
	if err != nil && err.Error() != "unauthorized" {
		fmt.Println(err)
		t.Fatal("bob improperly unauthorized to query")
	}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second, noRetries)
	if err != nil && err.Error() != "unauthorized" {
		t.Fatal("alice improperly authorized to query")
	}
}

func Test_NewServiceNotify(t *testing.T) {
	ml := mustNewMockTransport()
	mm := mustNewMockManager()
	var wg sync.WaitGroup

	wg.Add(1)
	mm.notifyFn = func(n *command.NotifyRequest) error {
		defer wg.Done()
		if n.Id != "foo" {
			t.Fatalf("failed to get correct node ID, exp %s, got %s", "foo", n.Id)
		}
		if n.Address != "localhost" {
			t.Fatalf("failed to get correct node address, exp %s, got %s", "localhost", n.Address)
		}
		return nil
	}

	credStr := mustNewMockCredentialStore()
	s := New(ml, mustNewMockDatabase(), mm, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	// Create a notify request.
	nr := &command.NotifyRequest{
		Id:      "foo",
		Address: "localhost",
	}

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	err := c.Notify(context.Background(), nr, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to notify node: %s", err)
	}

	// Ensure that the notify function was called.
	wg.Wait()

	// Test when auth is enabled
	credStr.HasPermOK = false
	err = c.Notify(context.Background(), nr, s.Addr(), nil, 5*time.Second)
	if err == nil {
		t.Fatal("should have failed to notify node due to lack of auth")
	}
	if err.Error() != "unauthorized" {
		t.Fatalf("failed to get correct error, exp %s, got %s", "unauthorized", err.Error())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceJoin(t *testing.T) {
	ml := mustNewMockTransport()
	mm := mustNewMockManager()
	var wg sync.WaitGroup

	wg.Add(1)
	mm.joinFn = func(j *command.JoinRequest) error {
		defer wg.Done()
		if j.Id != "foo" {
			t.Fatalf("failed to get correct node ID, exp %s, got %s", "foo", j.Id)
		}
		if j.Address != "localhost" {
			t.Fatalf("failed to get correct node address, exp %s, got %s", "localhost", j.Address)
		}
		if !j.Voter {
			t.Fatalf("failed to get correct voter setting, exp %t, got %t", true, j.Voter)
		}
		return nil
	}

	credStr := mustNewMockCredentialStore()
	s := New(ml, mustNewMockDatabase(), mm, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	// Create a Join request.
	jr := &command.JoinRequest{
		Id:      "foo",
		Address: "localhost",
		Voter:   true,
	}

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	err := c.Join(context.Background(), jr, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to join node: %s", err)
	}

	// Ensure the join function was called.
	wg.Wait()

	// Test when auth is enabled
	credStr.HasPermOK = false
	err = c.Join(context.Background(), jr, s.Addr(), nil, 5*time.Second)
	if err == nil {
		t.Fatal("should have failed to join node due to lack of auth")
	}
	if err.Error() != "unauthorized" {
		t.Fatalf("failed to get correct error, exp %s, got %s", "unauthorized", err.Error())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_ServiceHandleHighwaterMarkUpdate(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	responses, err := c.BroadcastHWM(context.Background(), 987654, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}
}

func Test_ServiceRegisterHWMUpdate(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a channel to receive highwater mark updates
	hwmCh := make(chan uint64, 1)
	s.RegisterHWMUpdate(hwmCh)

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	testHWM := uint64(123456)
	responses, err := c.BroadcastHWM(context.Background(), testHWM, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}

	// Check that we received the update on the channel
	select {
	case hwmUpdate := <-hwmCh:
		if hwmUpdate != testHWM {
			t.Fatalf("expected highwater_mark to be %d, got: %d", testHWM, hwmUpdate)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for highwater mark update on channel")
	}
}

func Test_ServiceClosesIdleConnection(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}
	s.connTimeout = 500 * time.Millisecond

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Connect but send nothing. The service should close the connection
	// after ConnReadTimeout.
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("failed to connect to service: %s", err)
	}
	defer conn.Close()

	// The server should close the connection after the read timeout.
	one := make([]byte, 1)
	_, err = conn.Read(one)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got: %v", err)
	}
}

func Test_ServiceSetConnectionLimitOpen(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	if err := s.SetConnectionLimit(1); !errors.Is(err, ErrServiceOpen) {
		t.Fatalf("expected ErrServiceOpen, got: %v", err)
	}
}

func Test_ServiceConnLimiterRejectAndRelease(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if err := s.SetConnectionLimit(1); err != nil {
		t.Fatalf("failed to set connection limit: %s", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	md := &mockDialer{Dialer: ml}
	t.Cleanup(md.CloseAll)

	c1 := NewClient(md, 30*time.Second)
	_, err := c1.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node metadata on first connection: %s", err)
	}

	// The first client's pooled connection remains open and holds the limiter slot.
	c2 := NewClient(md, 30*time.Second)
	_, err = c2.GetNodeMeta(context.Background(), s.Addr(), noRetries, 500*time.Millisecond)
	if err == nil {
		t.Fatalf("expected request on rejected connection to fail")
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		t.Fatalf("expected connection to be rejected without timing out, got: %v", err)
	}

	// Releasing the idle connection should free its limiter slot.
	if err := md.CloseConn(0); err != nil {
		t.Fatalf("failed to close first connection: %s", err)
	}

	// A new client request should be accepted once capacity is available again.
	c3 := NewClient(md, 30*time.Second)
	_, err = c3.GetNodeMeta(context.Background(), s.Addr(), noRetries, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to get node metadata after release: %s", err)
	}
}

type mockDialer struct {
	Dialer
	mu    sync.Mutex
	conns []net.Conn
}

func (d *mockDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := d.Dialer.Dial(address, timeout)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()

	return conn, nil
}

func (d *mockDialer) CloseConn(index int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if index < 0 || index >= len(d.conns) {
		return fmt.Errorf("connection %d not tracked", index)
	}
	return d.conns[index].Close()
}

func (d *mockDialer) CloseAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
}

type mockTransport struct {
	tn              net.Listener
	remoteEncrypted bool
}

func (ml *mockTransport) Accept() (c net.Conn, err error) {
	return ml.tn.Accept()
}

func (ml *mockTransport) Addr() net.Addr {
	return ml.tn.Addr()
}

func (ml *mockTransport) Close() (err error) {
	return ml.tn.Close()
}

func (ml *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var err error
	var conn net.Conn
	if ml.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}

	return conn, err
}

func mustNewMockTransport() *mockTransport {
	tn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}
	return &mockTransport{
		tn: tn,
	}
}

func mustNewMockTLSTransport() *mockTransport {
	tn := mustNewMockTransport()
	return &mockTransport{
		tn:              tls.NewListener(tn, mustCreateTLSConfig()),
		remoteEncrypted: true,
	}
}

type mockDatabase struct {
	executeFn func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error)
	queryFn   func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error)
	requestFn func(rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error)
	backupFn  func(br *command.BackupRequest, dst io.Writer) error
	loadFn    func(lr *command.LoadRequest) error
}

func (m *mockDatabase) Execute(ctx context.Context, er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
	return m.executeFn(er)
}

func (m *mockDatabase) Query(ctx context.Context, qr *command.QueryRequest) ([]*command.QueryRows, command.ConsistencyLevel, uint64, error) {
	rows, idx, err := m.queryFn(qr)
	return rows, command.ConsistencyLevel_NONE, idx, err
}

func (m *mockDatabase) Request(ctx context.Context, rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
	if m.requestFn == nil {
		return []*command.ExecuteQueryResponse{}, 0, 0, nil
	}
	return m.requestFn(rr)
}

func (m *mockDatabase) Backup(ctx context.Context, br *command.BackupRequest, dst io.Writer) error {
	if m.backupFn == nil {
		return nil
	}
	return m.backupFn(br, dst)
}

func (m *mockDatabase) Load(ctx context.Context, lr *command.LoadRequest) error {
	if m.loadFn == nil {
		return nil
	}
	return m.loadFn(lr)
}

func mustNewMockDatabase() *mockDatabase {
	e := func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		return []*command.ExecuteQueryResponse{}, 0, nil
	}
	q := func(er *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return []*command.QueryRows{}, 0, nil
	}
	return &mockDatabase{executeFn: e, queryFn: q}
}

type MockManager struct {
	removeNodeFn func(rn *command.RemoveNodeRequest) error
	notifyFn     func(n *command.NotifyRequest) error
	joinFn       func(j *command.JoinRequest) error
	leaderAddrFn func() (string, error)
	stepdownFn   func(wait bool, id string) error
	commitIndex  uint64
}

func (m *MockManager) Remove(ctx context.Context, rn *command.RemoveNodeRequest) error {
	if m.removeNodeFn == nil {
		return nil
	}
	return m.removeNodeFn(rn)
}

func (m *MockManager) Notify(n *command.NotifyRequest) error {
	if m.notifyFn == nil {
		return nil
	}
	return m.notifyFn(n)
}

func (m *MockManager) Join(j *command.JoinRequest) error {
	if m.joinFn == nil {
		return nil
	}
	return m.joinFn(j)
}

func (m *MockManager) Stepdown(wait bool, id string) error {
	if m.stepdownFn == nil {
		return nil
	}
	return m.stepdownFn(wait, id)
}

func (m *MockManager) LeaderAddr() (string, error) {
	if m.leaderAddrFn == nil {
		return "", nil
	}
	return m.leaderAddrFn()
}

func (m *MockManager) CommitIndex() (uint64, error) {
	return m.commitIndex, nil
}

func mustNewMockManager() *MockManager {
	return &MockManager{}
}

func mustCreateTLSConfig() *tls.Config {
	var err error

	certFile := x509.CertExampleDotComFile("")
	defer os.Remove(certFile)
	keyFile := x509.KeyExampleDotComFile("")
	defer os.Remove(keyFile)

	config := &tls.Config{
		InsecureSkipVerify: true,
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic("failed to create TLS config")
	}

	return config
}

type mockCredentialStore struct {
	HasPermOK bool
	aaFunc    func(username, password, perm string) bool
}

func (m *mockCredentialStore) AA(username, password, perm string) bool {
	if m == nil {
		return true
	}

	if m.aaFunc != nil {
		return m.aaFunc(username, password, perm)
	}
	return m.HasPermOK
}

func mustNewMockCredentialStore() *mockCredentialStore {
	return &mockCredentialStore{HasPermOK: true}
}

func makeCredentials(username, password string) *proto.Credentials {
	return &proto.Credentials{
		Username: username,
		Password: password,
	}
}

// mockClassifier is a Classifier driven by a function.
type mockClassifier func(stmts []*command.Statement) limit.Class

func (f mockClassifier) Class(stmts []*command.Statement) limit.Class { return f(stmts) }

// newAdmissionClusterService opens a cluster service gated by the given
// limiter configuration and returns it with its backing database.
func newAdmissionClusterService(t *testing.T, cfg limit.Config) (*Service, *mockDatabase, *limit.Limiter) {
	t.Helper()
	limit.ResetStats()
	db := mustNewMockDatabase()
	l := limit.New(cfg)
	s := New(mustNewMockTransport(), db, mustNewMockManager(), mustNewMockCredentialStore())
	s.SetAdmissionControl(l, nil)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, db, l
}

// blockExecute makes the database Execute call block until the hold
// channel closes. It returns a signal channel closed once a call has
// entered, the hold channel, and a counter of invocations.
func blockExecute(db *mockDatabase) (chan struct{}, chan struct{}, *int32) {
	started := make(chan struct{})
	hold := make(chan struct{})
	var invocations int32
	db.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		atomic.AddInt32(&invocations, 1)
		select {
		case <-started:
		default:
			close(started)
		}
		<-hold
		return []*command.ExecuteQueryResponse{}, 1, nil
	}
	return started, hold, &invocations
}

func blockQuery(db *mockDatabase) (chan struct{}, chan struct{}) {
	started := make(chan struct{})
	hold := make(chan struct{})
	db.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-hold
		return []*command.QueryRows{}, 1, nil
	}
	return started, hold
}

func clientExecute(t *testing.T, s *Service, cl *Client, timeout time.Duration) error {
	t.Helper()
	_, _, err := cl.Execute(context.Background(), &command.ExecuteRequest{}, s.Addr(), nil, timeout, noRetries)
	return err
}

// Test_ClusterExecuteQueryClassPools (TR-4.1): executes occupy the
// write pool, queries the read pool; saturation of one never affects
// the other.
func Test_ClusterExecuteQueryClassPools(t *testing.T) {
	s, db, _ := newAdmissionClusterService(t, limit.Config{
		WriteConcurrency: 1, ReadConcurrency: 1,
	})

	// Write pool: second execute rejected, query still succeeds.
	started, hold, _ := blockExecute(db)
	c1 := NewClient(mustNewMockTransport(), 30*time.Second)
	go func() {
		if err := clientExecute(t, s, c1, 10*time.Second); err != nil {
			t.Errorf("first execute: %v", err)
		}
	}()
	<-started

	c2 := NewClient(mustNewMockTransport(), 30*time.Second)
	err := clientExecute(t, s, c2, 5*time.Second)
	if err == nil || err.Error() != limit.ErrOverloaded.Error() {
		t.Fatalf("second execute: got %v want %q", err, limit.ErrOverloaded.Error())
	}
	cl := NewClient(mustNewMockTransport(), 30*time.Second)
	if _, _, err := cl.Query(context.Background(), &command.QueryRequest{}, s.Addr(), nil, 5*time.Second, noRetries); err != nil {
		t.Fatalf("query with write full: %v", err)
	}

	close(hold)

	// Read pool: second query rejected, execute still succeeds.
	qStarted, qHold := blockQuery(db)
	cq1 := NewClient(mustNewMockTransport(), 30*time.Second)
	go func() {
		if _, _, err := cq1.Query(context.Background(), &command.QueryRequest{}, s.Addr(), nil, 10*time.Second, noRetries); err != nil {
			t.Errorf("first query: %v", err)
		}
	}()
	<-qStarted

	cq2 := NewClient(mustNewMockTransport(), 30*time.Second)
	_, _, err = cq2.Query(context.Background(), &command.QueryRequest{}, s.Addr(), nil, 5*time.Second, noRetries)
	if err == nil || err.Error() != limit.ErrOverloaded.Error() {
		t.Fatalf("second query: got %v want %q", err, limit.ErrOverloaded.Error())
	}
	if err := clientExecute(t, s, NewClient(mustNewMockTransport(), 30*time.Second), 5*time.Second); err != nil {
		t.Fatalf("execute with read full: %v", err)
	}

	close(qHold)
}

// Test_ClusterRequestClassifiesByStatements (TR-4.1 REQUEST): a
// unified request takes the class reported by the classifier.
func Test_ClusterRequestClassifiesByStatements(t *testing.T) {
	s, db, _ := newAdmissionClusterService(t, limit.Config{
		WriteConcurrency: 1, ReadConcurrency: 1,
	})
	s.classifier = mockClassifier(func(stmts []*command.Statement) limit.Class {
		for _, st := range stmts {
			if !strings.HasPrefix(strings.ToUpper(st.Sql), "SELECT") {
				return limit.Write
			}
		}
		return limit.Read
	})

	// Fill the read pool.
	qStarted, qHold := blockQuery(db)
	blocking := NewClient(mustNewMockTransport(), 30*time.Second)
	go func() {
		blocking.Query(context.Background(), &command.QueryRequest{}, s.Addr(), nil, 10*time.Second, noRetries)
	}()
	<-qStarted

	cl := NewClient(mustNewMockTransport(), 30*time.Second)
	readReq := &command.ExecuteQueryRequest{
		Request: &command.Request{Statements: []*command.Statement{{Sql: "SELECT * FROM foo"}}},
	}
	_, _, _, err := cl.Request(context.Background(), readReq, s.Addr(), nil, 5*time.Second, noRetries)
	if err == nil || err.Error() != limit.ErrOverloaded.Error() {
		t.Fatalf("read-only request with read full: got %v want overload", err)
	}

	writeReq := &command.ExecuteQueryRequest{
		Request: &command.Request{Statements: []*command.Statement{{Sql: "INSERT INTO foo VALUES(1)"}}},
	}
	if _, _, _, err := cl.Request(context.Background(), writeReq, s.Addr(), nil, 5*time.Second, noRetries); err != nil {
		t.Fatalf("write request with read full: %v", err)
	}

	close(qHold)
}

// Test_ClusterQueueAndOverloadError (TR-4.2): saturation queues up to
// the configured depth; beyond it the overload error is carried back
// through the command response.
func Test_ClusterQueueAndOverloadError(t *testing.T) {
	s, db, l := newAdmissionClusterService(t, limit.Config{
		WriteConcurrency: 1, WriteQueue: 1,
	})

	started, hold, _ := blockExecute(db)
	c1 := NewClient(mustNewMockTransport(), 30*time.Second)
	c1Done := make(chan error, 1)
	go func() {
		c1Done <- clientExecute(t, s, c1, 10*time.Second)
	}()
	<-started

	c2 := NewClient(mustNewMockTransport(), 30*time.Second)
	c2Done := make(chan error, 1)
	go func() {
		c2Done <- clientExecute(t, s, c2, 10*time.Second)
	}()
	waitForGauge(l, "write", "queued", 1)

	c3 := NewClient(mustNewMockTransport(), 30*time.Second)
	start := time.Now()
	err := clientExecute(t, s, c3, 5*time.Second)
	if err == nil || err.Error() != limit.ErrOverloaded.Error() {
		t.Fatalf("third execute: got %v want overload", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("overload error not immediate: %s", time.Since(start))
	}

	close(hold)
	if err := <-c1Done; err != nil {
		t.Errorf("first execute: %v", err)
	}
	if err := <-c2Done; err != nil {
		t.Errorf("queued execute: %v", err)
	}
}

// Test_ClusterQueueCancelOnConnClose (TR-4.3): a queued command is
// canceled when its connection closes; it is never executed and the
// capacity is recovered.
func Test_ClusterQueueCancelOnConnClose(t *testing.T) {
	s, db, l := newAdmissionClusterService(t, limit.Config{
		WriteConcurrency: 1, WriteQueue: 1,
	})

	started, hold, invocations := blockExecute(db)
	c1 := NewClient(mustNewMockTransport(), 30*time.Second)
	go func() {
		clientExecute(t, s, c1, 10*time.Second)
	}()
	<-started

	// Send a second command over a raw connection and leave it queued.
	raw, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cmd := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_EXECUTE,
		Request: &proto.Command_ExecuteRequest{
			ExecuteRequest: &command.ExecuteRequest{},
		},
	}
	p, err := pb.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := writeBytesWithLength(raw, p); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitForGauge(l, "write", "queued", 1)

	if err := raw.Close(); err != nil {
		t.Fatalf("close raw conn: %v", err)
	}
	waitForExpvarInt("num_queue_canceled", 1)
	waitForGauge(l, "write", "queued", 0)

	close(hold)

	// The queued command must never have executed.
	if got := atomic.LoadInt32(invocations); got != 1 {
		t.Fatalf("db execute invocations: got %d want 1", got)
	}
	// Capacity is fully recovered.
	if err := clientExecute(t, s, NewClient(mustNewMockTransport(), 30*time.Second), 5*time.Second); err != nil {
		t.Fatalf("execute after cancel: %v", err)
	}
}

// Test_ClusterSharedPoolAcrossConnections (TR-4.4): commands arriving
// on independent connections share the same class pool.
func Test_ClusterSharedPoolAcrossConnections(t *testing.T) {
	s, db, l := newAdmissionClusterService(t, limit.Config{WriteConcurrency: 2})

	started, hold, _ := blockExecute(db)
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		cl := NewClient(mustNewMockTransport(), 30*time.Second)
		go func() {
			done <- clientExecute(t, s, cl, 10*time.Second)
		}()
	}
	<-started // Same fn closes started once; wait for gauge 2 below.
	waitForGauge(l, "write", "in_flight", 2)

	c3 := NewClient(mustNewMockTransport(), 30*time.Second)
	err := clientExecute(t, s, c3, 5*time.Second)
	if err == nil || err.Error() != limit.ErrOverloaded.Error() {
		t.Fatalf("third execute: got %v want overload", err)
	}

	close(hold)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Errorf("in-flight execute %d: %v", i, err)
		}
	}
}

func waitForGauge(l *limit.Limiter, class, key string, want any) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if l.Stats()[class].(map[string]any)[key] == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic(fmt.Sprintf("timed out waiting for %s.%s == %v (got %v)", class, key, want, l.Stats()[class]))
}

func waitForExpvarInt(key string, want int64) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m := expvar.Get("limit")
		if m != nil && m.(*expvar.Map).Get(key).(*expvar.Int).Value() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic(fmt.Sprintf("timed out waiting for expvar %s == %d", key, want))
}

// Test_ServiceClosesIdleAfterCommand (TR-I-1.1): with admission
// control unset (nil limiter), a connection that has sent one command
// and then goes idle is still reaped at the connection read timeout,
// exactly as before admission control existed.
func Test_ServiceClosesIdleAfterCommand(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	s.connTimeout = 500 * time.Millisecond

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send one EXECUTE; it takes the nil-limiter fast path, so no
	// watcher goroutine is started and no deadline is touched.
	cmd := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_EXECUTE,
		Request: &proto.Command_ExecuteRequest{
			ExecuteRequest: &command.ExecuteRequest{},
		},
	}
	p, err := pb.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := writeBytesWithLength(conn, p); err != nil {
		t.Fatalf("write: %v", err)
	}
	respBytes, err := readResponse(conn, 5*time.Second)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp := &proto.CommandExecuteResponse{}
	if err := pb.Unmarshal(respBytes, resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected response error: %s", resp.Error)
	}

	// Idle: the server must close the conn at about connTimeout.
	start := time.Now()
	one := make([]byte, 1)
	if _, err = conn.Read(one); err != io.EOF {
		t.Fatalf("expected io.EOF, got: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 350*time.Millisecond {
		t.Fatalf("idle connection closed too soon: %s", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("idle connection not reaped at connTimeout: %s", elapsed)
	}
}

// Test_ClientRemoteOverloadSentinel (TR-I-2.1): when the executing
// database reports admission overload, the error returned through the
// client still satisfies errors.Is(err, limit.ErrOverloaded), so entry
// nodes can detect it and map it to HTTP 503.
func Test_ClientRemoteOverloadSentinel(t *testing.T) {
	s, db, _ := newAdmissionClusterService(t, limit.Config{WriteConcurrency: 1})
	db.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		return nil, 0, limit.ErrOverloaded
	}

	cl := NewClient(mustNewMockTransport(), 30*time.Second)
	_, _, err := cl.Execute(context.Background(), &command.ExecuteRequest{}, s.Addr(), nil, 5*time.Second, noRetries)
	if !errors.Is(err, limit.ErrOverloaded) {
		t.Fatalf("expected errors.Is ErrOverloaded, got: %v", err)
	}
}

// Test_AdmitPromoteCloseRace (TR-I-3.1/3.2): stress the window in
// which a queued request is promoted by a slot release at the same
// moment its client disconnects. An abandoned request must never
// reach the database, and the class slot capacity must never decay.
func Test_AdmitPromoteCloseRace(t *testing.T) {
	const (
		N      = 2
		rounds = 300
	)
	limit.ResetStats()
	db := mustNewMockDatabase()
	lmt := limit.New(limit.Config{WriteConcurrency: N, WriteQueue: 1})
	ml := mustNewMockTransport()
	defer ml.Close()
	s := New(mustNewMockTransport(), db, mustNewMockManager(), mustNewMockCredentialStore())
	s.SetAdmissionControl(lmt, nil)
	if err := s.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	var invocations int64
	var extraTotal int64 // queued requests admitted before close observed

	for round := 0; round < rounds; round++ {
		holds := make([]chan struct{}, N)
		for i := range holds {
			holds[i] = make(chan struct{})
		}
		started := make(chan struct{}, N)
		var callIdx int64
		db.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
			i := atomic.AddInt64(&callIdx, 1) - 1
			atomic.AddInt64(&invocations, 1)
			if int(i) >= N {
				// X was promoted before the disconnect was observed:
				// it holds a real slot and returns immediately.
				atomic.AddInt64(&extraTotal, 1)
				return []*command.ExecuteQueryResponse{}, 1, nil
			}
			started <- struct{}{}
			<-holds[i]
			return []*command.ExecuteQueryResponse{}, 1, nil
		}

		holderDone := make(chan error, N)
		for i := 0; i < N; i++ {
			cl := NewClient(ml, 30*time.Second)
			go func() {
				holderDone <- clientExecute(t, s, cl, 10*time.Second)
			}()
		}
		for i := 0; i < N; i++ {
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatalf("round %d: holder %d did not start", round, i)
			}
		}

		// Park one request in the queue on a raw connection.
		raw, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatalf("round %d dial: %v", round, err)
		}
		cmd := &proto.Command{
			Type: proto.Command_COMMAND_TYPE_EXECUTE,
			Request: &proto.Command_ExecuteRequest{
				ExecuteRequest: &command.ExecuteRequest{},
			},
		}
		p, err := pb.Marshal(cmd)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := writeBytesWithLength(raw, p); err != nil {
			t.Fatalf("write: %v", err)
		}
		waitForGauge(lmt, "write", "queued", 1)

		// Race the disconnect against a slot release.
		closed := make(chan struct{})
		go func() {
			raw.Close()
			close(closed)
		}()
		go close(holds[0])
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: raw close hung", round)
		}

		// Release the other holders and drain them.
		for i := 1; i < N; i++ {
			close(holds[i])
		}
		for i := 0; i < N; i++ {
			select {
			case err := <-holderDone:
				if err != nil {
					t.Fatalf("round %d holder: %v", round, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("round %d: holder %d hung", round, i)
			}
		}

		// Settle: no queued waiters, no in-flight work. If X was
		// admitted its fn returns instantly, so in_flight reaching 0
		// implies every fn this round has completed.
		waitForGauge(lmt, "write", "queued", 0)
		waitForGauge(lmt, "write", "in_flight", 0)

		// At most one extra execution per round: the single X when it
		// was promoted before the disconnect became observable (then
		// it is an admitted, in-flight request the spec leaves alone).
		// A closed-observed X can never invoke the fn.
		got := atomic.LoadInt64(&invocations)
		wantMin := int64(N * (round + 1))
		wantMax := wantMin + int64(round+1)
		if got < wantMin || got > wantMax {
			t.Fatalf("round %d: invocations got %d want in [%d,%d]", round, got, wantMin, wantMax)
		}

		// All N slots must remain claimable: capacity did not decay.
		for i := 0; i < N; i++ {
			if !lmt.TryAcquire(limit.Write) {
				t.Fatalf("round %d: slot %d not claimable, capacity decayed", round, i)
			}
		}
		for i := 0; i < N; i++ {
			lmt.Release(limit.Write)
		}
	}
	t.Logf("admitted-before-close requests: %d / %d rounds", atomic.LoadInt64(&extraTotal), rounds)
}

// Test_ReconcileClose (TR-I-3.1): the promotion/disconnect race
// branch — Acquire returned nil but the connection had broken — must
// return the slot and report canceled instead of success.
func Test_ReconcileClose(t *testing.T) {
	l := limit.New(limit.Config{WriteConcurrency: 1})

	// Simulate Acquire having claimed the only slot.
	if !l.TryAcquire(limit.Write) {
		t.Fatal("setup: expected to claim the slot")
	}
	if err := reconcileClose(l, limit.Write, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcileClose nil: got %v want context.Canceled", err)
	}
	// The slot was handed back.
	if !l.TryAcquire(limit.Write) {
		t.Fatal("slot not claimable: capacity decayed")
	}
	l.Release(limit.Write)

	// Non-nil errors (including overload) pass through untouched.
	sentinel := errors.New("some acquire error")
	if err := reconcileClose(l, limit.Write, sentinel); !errors.Is(err, sentinel) {
		t.Fatalf("reconcileClose error: got %v want %v", err, sentinel)
	}
}

// Test_AdmitQueuedDisconnect (TR-I-3.1): a client that disconnects
// while its request is queued never executes; the promoted slot goes
// to no one and capacity stays intact.
func Test_AdmitQueuedDisconnect(t *testing.T) {
	l := limit.New(limit.Config{WriteConcurrency: 1, WriteQueue: 1})
	s := &Service{limiter: l}
	server, client := net.Pipe()
	defer server.Close()

	// Hold the only slot.
	if !l.TryAcquire(limit.Write) {
		t.Fatal("setup: expected to claim the only slot")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &connCloseWatcher{Conn: server}
	result := make(chan error, 1)
	go func() {
		result <- s.admit(ctx, cancel, w, limit.Write)
	}()

	// Disconnect while the request is still queued.
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("admit: got %v want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admit hung")
	}

	// Releasing the held slot leaves exactly one claimable slot.
	l.Release(limit.Write)
	if !l.TryAcquire(limit.Write) {
		t.Fatal("slot not claimable: capacity decayed")
	}
	l.Release(limit.Write)
}
