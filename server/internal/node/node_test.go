package node

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgmembership "github.com/premchandkpc/FlowRulZ/server/pkg/membership"
	pkgplandist "github.com/premchandkpc/FlowRulZ/server/pkg/plandist"
)

//
// Config tests
//

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.NodeID != DefaultNodeID {
		t.Errorf("NodeID=%s", cfg.NodeID)
	}
	if cfg.HTTPAddr != DefaultHTTPAddr {
		t.Errorf("HTTPAddr=%s", cfg.HTTPAddr)
	}
	if cfg.GRPCAddr != DefaultGRPCAddr {
		t.Errorf("GRPCAddr=%s", cfg.GRPCAddr)
	}
	if cfg.Topic != DefaultTopic {
		t.Errorf("Topic=%s", cfg.Topic)
	}
}

func TestConfigExecDir(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ExecDir() != filepath.Join(os.TempDir(), "flowrulz-execstate") {
		t.Errorf("unexpected ExecDir: %s", cfg.ExecDir())
	}
	cfg.ExecStateDir = "/custom/path"
	if cfg.ExecDir() != "/custom/path" {
		t.Errorf("unexpected ExecDir: %s", cfg.ExecDir())
	}
}

func TestConfigDLQDir(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.DLQDir() != filepath.Join(os.TempDir(), "flowrulz-dlq") {
		t.Errorf("unexpected DLQDir: %s", cfg.DLQDir())
	}
}

func TestConfigListenAddrs(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.GRPCListenAddr() != DefaultGRPCAddr {
		t.Errorf("GRPCListenAddr=%s", cfg.GRPCListenAddr())
	}
	if cfg.HTTPListenAddr() != DefaultHTTPAddr {
		t.Errorf("HTTPListenAddr=%s", cfg.HTTPListenAddr())
	}
	cfg.GRPCAddr = ":10000"
	cfg.HTTPAddr = ":20000"
	if cfg.GRPCListenAddr() != ":10000" {
		t.Errorf("GRPCListenAddr=%s", cfg.GRPCListenAddr())
	}
	if cfg.HTTPListenAddr() != ":20000" {
		t.Errorf("HTTPListenAddr=%s", cfg.HTTPListenAddr())
	}
}

func TestConfigDerivedValues(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ReplyRouterCleanupInterval() != time.Second {
		t.Errorf("ReplyRouterCleanupInterval=%v", cfg.ReplyRouterCleanupInterval())
	}
	if cfg.ReplyRouterMaxPending() != 10000 {
		t.Errorf("ReplyRouterMaxPending=%d", cfg.ReplyRouterMaxPending())
	}
	if cfg.DedupCapacity() != 10000 {
		t.Errorf("DedupCapacity=%d", cfg.DedupCapacity())
	}
	if cfg.DedupTTL() != 5*time.Minute {
		t.Errorf("DedupTTL=%v", cfg.DedupTTL())
	}
	if cfg.DLQMaxEntries() != 10000 {
		t.Errorf("DLQMaxEntries=%d", cfg.DLQMaxEntries())
	}
	if cfg.RegistryHeartbeatTimeout() != 30*time.Second {
		t.Errorf("RegistryHeartbeatTimeout=%v", cfg.RegistryHeartbeatTimeout())
	}
	if cfg.NumPartitions() != 64 {
		t.Errorf("NumPartitions=%d", cfg.NumPartitions())
	}
}

//
// ExecRegistry tests
//

func TestNewExecRegistry(t *testing.T) {
	er := NewExecRegistry()
	if er.Len() != 0 {
		t.Errorf("expected empty, Len=%d", er.Len())
	}
}

func TestExecRegistryRegisterAndCancel(t *testing.T) {
	er := NewExecRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	er.Register("exec-1", cancel, "test-plan")

	if er.Len() != 1 {
		t.Errorf("Len=%d", er.Len())
	}

	cancelled := er.Cancel("exec-1")
	if !cancelled {
		t.Error("Cancel returned false for existing execution")
	}
	// After cancel, context should be done
	select {
	case <-ctx.Done():
	default:
		t.Error("context should be cancelled after Cancel()")
	}
}

func TestExecRegistryCancelNonexistent(t *testing.T) {
	er := NewExecRegistry()
	if er.Cancel("nonexistent") {
		t.Error("Cancel should return false for nonexistent ID")
	}
}

func TestExecRegistryUnregister(t *testing.T) {
	er := NewExecRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	er.Register("exec-1", cancel, "test")
	er.Unregister("exec-1")
	if er.Len() != 0 {
		t.Errorf("Len=%d after unregister", er.Len())
	}
	// Context should still be valid (not cancelled by us)
	if ctx.Err() != nil {
		t.Error("Unregister should not cancel the context")
	}
}

func TestExecRegistryCancelAll(t *testing.T) {
	er := NewExecRegistry()
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	er.Register("a", cancel1, "")
	er.Register("b", cancel2, "")
	er.CancelAll()
	if ctx1.Err() == nil {
		t.Error("ctx1 should be cancelled")
	}
	if ctx2.Err() == nil {
		t.Error("ctx2 should be cancelled")
	}
}

func TestExecRegistryList(t *testing.T) {
	er := NewExecRegistry()
	_, cancel := context.WithCancel(context.Background())
	er.Register("id-1", cancel, "plan-a")
	list := er.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(list))
	}
	if _, ok := list["id-1"]; !ok {
		t.Error("id-1 not in list")
	}
}

func TestExecRegistryConcurrentAccess(t *testing.T) {
	er := NewExecRegistry()
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			_, cancel := context.WithCancel(context.Background())
			er.Register("x", cancel, "")
			er.Len()
			er.List()
			er.CancelAll()
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

//
// ProdNode basics tests
//

type mockRaftCluster struct {
	leader    bool
	term      uint64
	leaderID  pkgcluster.MemberID
	leaderFn  func(bool)
}

func (m *mockRaftCluster) ID() pkgcluster.MemberID                   { return "mock" }
func (m *mockRaftCluster) Addr() string                              { return ":9090" }
func (m *mockRaftCluster) Start(ctx context.Context) error            { return nil }
func (m *mockRaftCluster) Stop(ctx context.Context) error             { return nil }
func (m *mockRaftCluster) State() pkgcluster.ClusterState             { return pkgcluster.Leader }
func (m *mockRaftCluster) IsLeader() bool                            { return m.leader }
func (m *mockRaftCluster) CurrentTerm() uint64                       { return m.term }
func (m *mockRaftCluster) LeaderID() pkgcluster.MemberID             { return m.leaderID }
func (m *mockRaftCluster) LeaderAddr() string                        { return string(m.leaderID) }
func (m *mockRaftCluster) SubscribeLeaderChanges(fn func(bool)) pkgcluster.CancelFunc {
	m.leaderFn = fn
	return func() {}
}
func (m *mockRaftCluster) SubscribeTermChanges(fn func(uint64)) pkgcluster.CancelFunc { return func() {} }
func (m *mockRaftCluster) Join(memberID pkgcluster.MemberID, addr string) error       { return nil }
func (m *mockRaftCluster) Remove(memberID pkgcluster.MemberID) error                  { return nil }
func (m *mockRaftCluster) BootstrapCluster() error                                     { return nil }
func (m *mockRaftCluster) CaptureLeadershipToken() pkgcluster.LeadershipToken {
	return pkgcluster.LeadershipToken{Leader: m.leader, Term: m.term}
}
func (m *mockRaftCluster) ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool {
	return token.Leader == m.leader && token.Term == m.term
}

type mockMembership struct {
	leaderID string
}

func (m *mockMembership) Add(id, address string)           {}
func (m *mockMembership) Remove(id string)                 {}
func (m *mockMembership) Heartbeat(id, address string)     {}
func (m *mockMembership) MarkDead(id string)               {}
func (m *mockMembership) MarkAlive(id string)              {}
func (m *mockMembership) AliveCount() int                  { return 0 }
func (m *mockMembership) AliveNodes() []string             { return nil }
func (m *mockMembership) LeaderID() string                 { return m.leaderID }
func (m *mockMembership) Snapshot() []pkgmembership.NodeInfo { return nil }
func (m *mockMembership) Lookup(id string) *pkgmembership.NodeInfo { return nil }
func (m *mockMembership) LeaderLastSeen() time.Time        { return time.Time{} }
func (m *mockMembership) SetLeaderLease(d time.Duration)   {}
func (m *mockMembership) OnLeaseExpiry(cb func(leaderID string)) pkgmembership.CancelFunc { return func() {} }
func (m *mockMembership) StartEviction(ctx context.Context, interval time.Duration) {}
func (m *mockMembership) StartLeaderLeaseChecker(ctx context.Context, interval time.Duration) {}

func minimalProdNode() *ProdNode {
	return &ProdNode{
		nodeID:     "test-node",
		httpAddr:   ":8080",
		httpClient: &http.Client{Timeout: 10 * time.Second},
		Execs:      NewExecRegistry(),
		Membership: &mockMembership{},
	}
}

func TestProdNodeID(t *testing.T) {
	n := minimalProdNode()
	if n.ID() != "test-node" {
		t.Errorf("ID=%s", n.ID())
	}
}

func TestProdNodeAddr(t *testing.T) {
	n := minimalProdNode()
	if n.Addr() != ":8080" {
		t.Errorf("Addr=%s", n.Addr())
	}
}

func TestProdNodeIsLeaderNoRaft(t *testing.T) {
	n := minimalProdNode()
	if !n.IsLeader() {
		t.Error("expected leader when RaftCluster is nil")
	}
}

func TestProdNodeIsLeaderWithRaft(t *testing.T) {
	n := minimalProdNode()
	n.RaftCluster = &mockRaftCluster{leader: true}
	if !n.IsLeader() {
		t.Error("expected leader")
	}
	n.RaftCluster = &mockRaftCluster{leader: false}
	if n.IsLeader() {
		t.Error("expected not leader")
	}
}

func TestProdNodeCurrentTermWithRaft(t *testing.T) {
	n := minimalProdNode()
	n.RaftCluster = &mockRaftCluster{term: 42}
	if n.CurrentTerm() != 42 {
		t.Errorf("CurrentTerm=%d", n.CurrentTerm())
	}
}

func TestProdNodeCurrentTermWithoutRaft(t *testing.T) {
	n := minimalProdNode()
	pd := plandist.New("test")
	pd.SetTerm(7)
	n.PlanDist = pd
	if n.CurrentTerm() != 7 {
		t.Errorf("CurrentTerm=%d", n.CurrentTerm())
	}
}

func TestProdNodeLeaderIDWithRaft(t *testing.T) {
	n := minimalProdNode()
	n.RaftCluster = &mockRaftCluster{leader: true, leaderID: "leader-1"}
	n.nodeID = "leader-1"
	if n.LeaderID() != "leader-1" {
		t.Errorf("LeaderID=%s", n.LeaderID())
	}
}

func TestProdNodeLeaderIDWithoutRaft(t *testing.T) {
	n := minimalProdNode()
	m := &mockMembership{leaderID: "mem-leader"}
	n.Membership = m
	if n.LeaderID() != "mem-leader" {
		t.Errorf("LeaderID=%s", n.LeaderID())
	}
}

func TestProdNodeReady(t *testing.T) {
	n := minimalProdNode()
	pd := plandist.New("test")
	n.PlanDist = pd

	// Without Raft, IsLeader() returns true.
	// If term is 0, Ready returns error.
	if err := n.Ready(context.Background()); err == nil {
		t.Error("expected error when leader with term=0")
	}

	pd.SetTerm(1)
	if err := n.Ready(context.Background()); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProdNodeNextDeployTermWithRaft(t *testing.T) {
	n := minimalProdNode()
	n.RaftCluster = &mockRaftCluster{term: 99}
	if term := n.nextDeployTerm(); term != 99 {
		t.Errorf("nextDeployTerm=%d", term)
	}
}

func TestProdNodeNextDeployTermWithoutRaft(t *testing.T) {
	n := minimalProdNode()
	pd := plandist.New("test")
	pd.SetTerm(5)
	n.PlanDist = pd
	if term := n.nextDeployTerm(); term != 6 {
		t.Errorf("nextDeployTerm=%d, expected 6", term)
	}
}

//
// Message handler tests
//

func TestHandleNodeDiscoveryMessage(t *testing.T) {
	n := minimalProdNode()
	m := &mockMembership{}
	n.Membership = m

	msg := NodeDiscoveryMessage{NodeID: "node-b", Address: "10.0.0.2"}
	data, _ := json.Marshal(msg)
	_, err := n.handleNodeDiscoveryMessage(context.Background(), data)
	if err != nil {
		t.Fatalf("handleNodeDiscoveryMessage: %v", err)
	}
}

func TestHandleNodeDiscoveryMessageSelf(t *testing.T) {
	n := minimalProdNode()
	n.nodeID = "node-a"
	msg := NodeDiscoveryMessage{NodeID: "node-a", Address: "10.0.0.1"}
	data, _ := json.Marshal(msg)
	_, err := n.handleNodeDiscoveryMessage(context.Background(), data)
	if err != nil {
		t.Fatalf("handleNodeDiscoveryMessage: %v", err)
	}
}

func TestHandleNodeDiscoveryMessageInvalidJSON(t *testing.T) {
	n := minimalProdNode()
	_, err := n.handleNodeDiscoveryMessage(context.Background(), []byte("{{{"))
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestHandleAckMessage(t *testing.T) {
	n := minimalProdNode()
	n.PlanDist = plandist.New("test")

	msg := pkgplandist.AckMessage{NodeID: "node-a", RuleID: "rule-1", Version: 1, Status: "ok"}
	data, _ := json.Marshal(msg)
	_, err := n.handleAckMessage(context.Background(), data)
	if err != nil {
		t.Fatalf("handleAckMessage: %v", err)
	}
}

func TestHandleAckMessageInvalidJSON(t *testing.T) {
	n := minimalProdNode()
	n.PlanDist = plandist.New("test")

	_, err := n.handleAckMessage(context.Background(), []byte("{{{"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

//
// NewNode tests
//

func TestNewNodeMinimal(t *testing.T) {
	cfg := Config{NodeID: "minimal"}
	deps := Dependencies{
		Engine:   engine.New(""),
		Registry: registry.New(),
	}
	n := NewNode(cfg, deps)
	if n == nil {
		t.Fatal("NewNode returned nil")
	}
	if n.ID() != "minimal" {
		t.Errorf("ID=%s", n.ID())
	}
	if n.Execs == nil {
		t.Error("Execs should be initialized")
	}
}

func TestNewProdNodeDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	n := NewNode(*cfg, DefaultDependencies(*cfg))
	if n == nil {
		t.Fatal("NewNode returned nil")
	}
	if n.ID() != DefaultNodeID {
		t.Errorf("ID=%s", n.ID())
	}
}

func TestDependenciesDefaultHasComponents(t *testing.T) {
	cfg := DefaultConfig()
	deps := DefaultDependencies(*cfg)
	if deps.Engine == nil {
		t.Error("Engine should not be nil")
	}
	if deps.Scheduler == nil {
		t.Error("Scheduler should not be nil")
	}
	if deps.ReplyRouter == nil {
		t.Error("ReplyRouter should not be nil")
	}
	if deps.Metrics == nil {
		t.Error("Metrics should not be nil")
	}
}

//
// Edge case tests
//

func TestExecRegistryRegisterAfterCancel(t *testing.T) {
	er := NewExecRegistry()
	_, cancel := context.WithCancel(context.Background())
	er.Register("x", cancel, "")
	er.Unregister("x")
	er.Cancel("x") // should return false, not panic
	_, cancel2 := context.WithCancel(context.Background())
	er.Register("x", cancel2, "") // re-register same ID
	if er.Len() != 1 {
		t.Errorf("Len=%d", er.Len())
	}
}

func TestProdNodeExecuteEmptyEngine(t *testing.T) {
	n := &ProdNode{
		nodeID:   "test",
		httpAddr: ":0",
		Engine:   engine.New(""),
	}
	out, err := n.executeAll(context.Background(), []byte(`{"test":1}`))
	if err != nil {
		t.Fatalf("executeAll: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected nil result, got %d entries", len(out))
	}
}

func TestConfigGRPCListenAddrEmpty(t *testing.T) {
	cfg := Config{}
	if addr := cfg.GRPCListenAddr(); addr != DefaultGRPCAddr {
		t.Errorf("expected default, got %s", addr)
	}
	if addr := cfg.HTTPListenAddr(); addr != DefaultHTTPAddr {
		t.Errorf("expected default, got %s", addr)
	}
}
