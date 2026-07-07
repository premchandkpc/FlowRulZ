package node

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/scheduler"
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
	er := execstate.NewExecRegistry()
	if er.Len() != 0 {
		t.Errorf("expected empty, Len=%d", er.Len())
	}
}

func TestExecRegistryRegisterAndCancel(t *testing.T) {
	er := execstate.NewExecRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	er.Register("exec-1", cancel, "test-plan")

	if er.Len() != 1 {
		t.Errorf("Len=%d", er.Len())
	}

	cancelled := er.Cancel("exec-1")
	if !cancelled {
		t.Error("Cancel returned false for existing execution")
	}
	select {
	case <-ctx.Done():
	default:
		t.Error("context should be cancelled after Cancel()")
	}
}

func TestExecRegistryCancelNonexistent(t *testing.T) {
	er := execstate.NewExecRegistry()
	if er.Cancel("nonexistent") {
		t.Error("Cancel should return false for nonexistent ID")
	}
}

func TestExecRegistryUnregister(t *testing.T) {
	er := execstate.NewExecRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	er.Register("exec-1", cancel, "test")
	er.Unregister("exec-1")
	if er.Len() != 0 {
		t.Errorf("Len=%d after unregister", er.Len())
	}
	if ctx.Err() != nil {
		t.Error("Unregister should not cancel the context")
	}
}

func TestExecRegistryCancelAll(t *testing.T) {
	er := execstate.NewExecRegistry()
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
	er := execstate.NewExecRegistry()
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
	er := execstate.NewExecRegistry()
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
	leader   bool
	term     uint64
	leaderID pkgcluster.MemberID
	leaderFn func(bool)
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

func (m *mockMembership) Add(id, address string)                      {}
func (m *mockMembership) Remove(id string)                            {}
func (m *mockMembership) Heartbeat(id, address string)                {}
func (m *mockMembership) MarkDead(id string)                          {}
func (m *mockMembership) MarkAlive(id string)                         {}
func (m *mockMembership) AliveCount() int                             { return 0 }
func (m *mockMembership) AliveNodes() []string                        { return nil }
func (m *mockMembership) LeaderID() string                            { return m.leaderID }
func (m *mockMembership) Snapshot() []pkgmembership.NodeInfo           { return nil }
func (m *mockMembership) Lookup(id string) *pkgmembership.NodeInfo     { return nil }
func (m *mockMembership) LeaderLastSeen() time.Time                   { return time.Time{} }
func (m *mockMembership) SetLeaderLease(d time.Duration)              {}
func (m *mockMembership) OnLeaseExpiry(cb func(leaderID string)) pkgmembership.CancelFunc { return func() {} }
func (m *mockMembership) StartEviction(ctx context.Context, interval time.Duration)       {}
func (m *mockMembership) StartLeaderLeaseChecker(ctx context.Context, interval time.Duration) {}

type mockEngine struct{}

func (e *mockEngine) ActivePlanBytes() [][]byte { return nil }
func (e *mockEngine) AddVersion(id, dsl string, plan []byte, version uint64) error { return nil }
func (e *mockEngine) Promote(id string, version uint64) error { return nil }
func (e *mockEngine) SetAfterDeploy(fn func(id, dsl string, plan []byte, version uint64)) {}
func (e *mockEngine) SetAfterPromote(fn func(id string, version uint64)) {}

func minimalProdNode() *ProdNode {
	planDist := plandist.New("test")
	return &ProdNode{
		config: NodeConfig{
			Config:     Config{NodeID: "test-node", HTTPAddr: ":8080"},
			httpClient: &http.Client{Timeout: 10 * time.Second},
		},
		leadership: NewSingleLeaderStrategy(planDist),
		cluster: ClusterDeps{
			Membership: &mockMembership{},
		},
		exec: ExecutionDeps{
			Execs: execstate.NewExecRegistry(),
		},
		part: PartitionDeps{
			PlanDist: planDist,
		},
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
	raft := &mockRaftCluster{leader: true}
	n.cluster.RaftCluster = raft
	n.leadership = NewRaftLeadershipStrategy(raft)
	if !n.IsLeader() {
		t.Error("expected leader")
	}
	raft.leader = false
	if n.IsLeader() {
		t.Error("expected not leader")
	}
}

func TestProdNodeCurrentTermWithRaft(t *testing.T) {
	n := minimalProdNode()
	raft := &mockRaftCluster{term: 42}
	n.cluster.RaftCluster = raft
	n.leadership = NewRaftLeadershipStrategy(raft)
	if n.CurrentTerm() != 42 {
		t.Errorf("CurrentTerm=%d", n.CurrentTerm())
	}
}

func TestProdNodeCurrentTermWithoutRaft(t *testing.T) {
	n := minimalProdNode()
	n.part.PlanDist.SetTerm(7)
	if n.CurrentTerm() != 7 {
		t.Errorf("CurrentTerm=%d", n.CurrentTerm())
	}
}

func TestProdNodeLeaderIDWithRaft(t *testing.T) {
	n := minimalProdNode()
	raft := &mockRaftCluster{leader: true, leaderID: "leader-1"}
	n.cluster.RaftCluster = raft
	n.leadership = NewRaftLeadershipStrategy(raft)
	n.config.NodeID = "leader-1"
	if n.LeaderID() != "leader-1" {
		t.Errorf("LeaderID=%s", n.LeaderID())
	}
}

func TestProdNodeLeaderIDWithoutRaft(t *testing.T) {
	n := minimalProdNode()
	m := &mockMembership{leaderID: "mem-leader"}
	n.cluster.Membership = m
	n.leadership.(*SingleLeaderStrategy).SetMembership(m)
	if n.LeaderID() != "mem-leader" {
		t.Errorf("LeaderID=%s", n.LeaderID())
	}
}

func TestProdNodeReady(t *testing.T) {
	n := minimalProdNode()
	n.part.PlanDist.SetTerm(1)

	if err := n.Ready(context.Background()); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

//
// MessageRouter handler tests
//

func newTestMessageRouter() *MessageRouter {
	return NewMessageRouter(
		"test-node",
		"test-topic",
		nil,
		&mockMembership{},
		nil,
		&mockEngine{},
		plandist.New("test"),
		nil,
	)
}

func TestHandleNodeDiscoveryMessage(t *testing.T) {
	r := newTestMessageRouter()
	m := &mockMembership{}
	r.membership = m

	msg := NodeDiscoveryMessage{NodeID: "node-b", Address: "10.0.0.2"}
	data, _ := json.Marshal(msg)
	_, err := r.handleNodeDiscoveryMessage(context.Background(), data)
	if err != nil {
		t.Fatalf("handleNodeDiscoveryMessage: %v", err)
	}
}

func TestHandleNodeDiscoveryMessageSelf(t *testing.T) {
	r := newTestMessageRouter()
	msg := NodeDiscoveryMessage{NodeID: "test-node", Address: "10.0.0.1"}
	data, _ := json.Marshal(msg)
	_, err := r.handleNodeDiscoveryMessage(context.Background(), data)
	if err != nil {
		t.Fatalf("handleNodeDiscoveryMessage: %v", err)
	}
}

func TestHandleNodeDiscoveryMessageInvalidJSON(t *testing.T) {
	r := newTestMessageRouter()
	_, err := r.handleNodeDiscoveryMessage(context.Background(), []byte("{{{"))
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestHandleAckMessage(t *testing.T) {
	r := newTestMessageRouter()

	msg := pkgplandist.AckMessage{NodeID: "node-a", RuleID: "rule-1", Version: 1, Status: "ok"}
	data, _ := json.Marshal(msg)
	_, err := r.handleAckMessage(context.Background(), data)
	if err != nil {
		t.Fatalf("handleAckMessage: %v", err)
	}
}

func TestHandleAckMessageInvalidJSON(t *testing.T) {
	r := newTestMessageRouter()

	_, err := r.handleAckMessage(context.Background(), []byte("{{{"))
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
		Engine:    engine.New(""),
		Registry:  registry.New(),
		Scheduler: scheduler.New(nil),
	}
	n := NewNode(cfg, deps)
	if n == nil {
		t.Fatal("NewNode returned nil")
	}
	if n.ID() != "minimal" {
		t.Errorf("ID=%s", n.ID())
	}
	if n.exec.Execs == nil {
		t.Error("Execs should be initialized")
	}
}

func TestNewProdNodeDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	n := NewProdNode(cfg)
	if n == nil {
		t.Fatal("NewProdNode returned nil")
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
	er := execstate.NewExecRegistry()
	_, cancel := context.WithCancel(context.Background())
	er.Register("x", cancel, "")
	er.Unregister("x")
	er.Cancel("x")
	_, cancel2 := context.WithCancel(context.Background())
	er.Register("x", cancel2, "")
	if er.Len() != 1 {
		t.Errorf("Len=%d", er.Len())
	}
}

func TestProdNodeExecuteEmptyEngine(t *testing.T) {
	n := &ProdNode{
		config: NodeConfig{
			Config: Config{NodeID: "test", HTTPAddr: ":0"},
		},
		exec: ExecutionDeps{
			Engine: engine.New(""),
		},
	}
	n.execution = NewExecutionEngine(n.exec.Engine, nil, nil, nil, nil, nil, nil)
	out, err := n.execution.ExecuteAll(context.Background(), []byte(`{"test":1}`))
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
