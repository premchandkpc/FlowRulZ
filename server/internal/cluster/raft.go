package cluster

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
)

const (
	DefaultRaftPort  = 9093
	raftTimeout      = 10 * time.Second
	raftLogCacheSize = 512
)

// RaftCluster provides Raft consensus for leader election and cluster coordination.
// Replaces the lowest-ID-based leader election with proper Raft consensus.
type RaftCluster struct {
	nodeID   string
	raftDir  string
	raftBind string

	raft      *raft.Raft
	transport *raft.NetworkTransport
	logStore  *raftboltdb.BoltStore
	stable    *raftboltdb.BoltStore
	snapStore raft.SnapshotStore
	fsm       *NoopFSM

	leaderNotifyCh chan bool
	isLeader       atomic.Bool
	leaderAddr     atomic.Value

	leaderSubsMu sync.RWMutex
	leaderSubs   []func(bool)

	mu      sync.Mutex
	started bool
	stopCh  chan struct{}
}

// NoopFSM implements raft.FSM with no state replication.
// Cluster state (rules, partitions) is still distributed via Kafka.
// Raft is used ONLY for leader election and term management.
type NoopFSM struct{}

func (n *NoopFSM) Apply(log *raft.Log) interface{}     { return nil }
func (n *NoopFSM) Snapshot() (raft.FSMSnapshot, error) { return &NoopSnapshot{}, nil }
func (n *NoopFSM) Restore(rc io.ReadCloser) error      { return nil }

type NoopSnapshot struct{}

func (n *NoopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (n *NoopSnapshot) Release()                             {}

func NewRaftCluster(nodeID, raftDir, raftBind string) *RaftCluster {
	return &RaftCluster{
		nodeID:         nodeID,
		raftDir:        raftDir,
		raftBind:       raftBind,
		fsm:            &NoopFSM{},
		leaderNotifyCh: make(chan bool, 1),
		stopCh:         make(chan struct{}),
	}
}

func (rc *RaftCluster) Start() error {
	rc.mu.Lock()
	if rc.started {
		rc.mu.Unlock()
		return nil
	}
	rc.started = true
	rc.mu.Unlock()

	if err := os.MkdirAll(rc.raftDir, 0755); err != nil {
		return fmt.Errorf("raft dir: %w", err)
	}

	logStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(rc.raftDir, "raft-log.db"),
	})
	if err != nil {
		return fmt.Errorf("raft log store: %w", err)
	}
	rc.logStore = logStore

	stable, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(rc.raftDir, "raft-stable.db"),
	})
	if err != nil {
		return fmt.Errorf("raft stable store: %w", err)
	}
	rc.stable = stable

	snapStore, err := raft.NewFileSnapshotStore(rc.raftDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft snapshot store: %w", err)
	}
	rc.snapStore = snapStore

	addr, err := net.ResolveTCPAddr("tcp", rc.raftBind)
	if err != nil {
		return fmt.Errorf("raft bind addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(rc.raftBind, addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft transport: %w", err)
	}
	rc.transport = transport

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(rc.nodeID)
	cfg.HeartbeatTimeout = 1 * time.Second
	cfg.ElectionTimeout = 1 * time.Second
	cfg.LeaderLeaseTimeout = 500 * time.Millisecond
	cfg.CommitTimeout = 50 * time.Millisecond
	cfg.SnapshotInterval = 30 * time.Second
	cfg.SnapshotThreshold = 1024
	cfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Info,
		Output: os.Stderr,
	})

	r, err := raft.NewRaft(cfg, rc.fsm, rc.logStore, rc.stable, rc.snapStore, rc.transport)
	if err != nil {
		return fmt.Errorf("raft new: %w", err)
	}
	rc.raft = r

	go rc.trackLeadership()

	slog.Info("raft cluster: started", "node_id", rc.nodeID, "bind", rc.raftBind)
	return nil
}

func (rc *RaftCluster) Stop() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if !rc.started {
		return
	}
	select {
	case <-rc.stopCh:
	default:
		close(rc.stopCh)
	}

	if rc.raft != nil {
		future := rc.raft.Shutdown()
		if err := future.Error(); err != nil {
			slog.Error("raft shutdown error", "error", err)
		}
	}
	if rc.transport != nil {
		rc.transport.Close()
	}
	if rc.logStore != nil {
		rc.logStore.Close()
	}
	if rc.stable != nil {
		rc.stable.Close()
	}
	rc.started = false
	slog.Info("raft cluster: stopped", "node_id", rc.nodeID)
}

// BootstrapCluster initializes the Raft cluster with this node as the sole voter.
// Called exactly once on the first node in a new cluster.
func (rc *RaftCluster) BootstrapCluster() error {
	if rc.raft == nil {
		return fmt.Errorf("raft not started")
	}
	hasState := true
	if _, err := os.Stat(filepath.Join(rc.raftDir, "raft-log.db")); os.IsNotExist(err) {
		hasState = false
	}
	if hasState {
		slog.Info("raft cluster: existing state found, skipping bootstrap", "node_id", rc.nodeID)
		return nil
	}
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(rc.nodeID),
				Address: raft.ServerAddress(rc.raftBind),
			},
		},
	}
	future := rc.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		return fmt.Errorf("bootstrap cluster: %w", err)
	}
	slog.Info("raft cluster: bootstrapped as initial leader candidate", "node_id", rc.nodeID)
	return nil
}

// Join adds a node as a voter in the Raft cluster. Must be called on the leader.
func (rc *RaftCluster) Join(nodeID, raftAddr string) error {
	if rc.raft == nil {
		return fmt.Errorf("raft not started")
	}
	if rc.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader, cannot add voter")
	}
	addFuture := rc.raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0, // previous index
		0, // timeout (0 = use default)
	)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("add voter %s at %s: %w", nodeID, raftAddr, err)
	}
	slog.Info("raft cluster: added voter", "node_id", rc.nodeID, "voter_id", nodeID, "addr", raftAddr)
	return nil
}

// Leave removes a node from the Raft cluster. Must be called on the leader.
func (rc *RaftCluster) Leave(nodeID string) error {
	if rc.raft == nil {
		return fmt.Errorf("raft not started")
	}
	if rc.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader, cannot remove server")
	}
	future := rc.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("remove server %s: %w", nodeID, err)
	}
	slog.Info("raft cluster: removed voter", "node_id", rc.nodeID, "voter_id", nodeID)
	return nil
}

// IsLeader returns true if this node is the current Raft leader.
func (rc *RaftCluster) IsLeader() bool {
	if rc.raft == nil {
		return false
	}
	return rc.raft.State() == raft.Leader
}

// LeaderAddr returns the Raft address of the current leader.
func (rc *RaftCluster) LeaderAddr() string {
	if rc.raft == nil {
		return ""
	}
	addr := rc.raft.Leader()
	return string(addr)
}

// CurrentTerm returns the current Raft term.
func (rc *RaftCluster) CurrentTerm() uint64 {
	if rc.raft == nil {
		return 0
	}
	stats := rc.raft.Stats()
	term, err := strconv.ParseUint(stats["term"], 10, 64)
	if err != nil {
		return 0
	}
	return term
}

// CaptureLeadershipToken captures the current leadership state.
// Use this to implement the fencing pattern:
//
//	token := node.CaptureLeadershipToken()
//	if !token.Valid() { return }
//	// ... do work ...
//	if !node.ValidateLeadershipToken(token) { return stale error }
//	// ... publish side effect ...
//
// This prevents split-brain: if leadership changed between capture and
// validate, the token will be invalid and the publish is skipped.
func (rc *RaftCluster) CaptureLeadershipToken() pkgcluster.LeadershipToken {
	return pkgcluster.LeadershipToken{
		Leader: rc.IsLeader(),
		Term:   rc.CurrentTerm(),
	}
}

// ValidateLeadershipToken checks if a previously captured token is still valid.
// Returns true if this node is still leader with the same term.
func (rc *RaftCluster) ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool {
	if !token.Valid() {
		return false
	}
	current := rc.CaptureLeadershipToken()
	return current.Leader && current.Term == token.Term
}

// ClusterSize returns the number of voters in the Raft cluster.
func (rc *RaftCluster) ClusterSize() int {
	if rc.raft == nil {
		return 0
	}
	future := rc.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0
	}
	return len(future.Configuration().Servers)
}

// SubscribeLeaderChanges registers a callback that fires when leadership changes.
// isLeader is true when this node becomes leader, false when it steps down.
func (rc *RaftCluster) SubscribeLeaderChanges(fn func(isLeader bool)) {
	rc.leaderSubsMu.Lock()
	defer rc.leaderSubsMu.Unlock()
	rc.leaderSubs = append(rc.leaderSubs, fn)
}

// LastContact returns the time since last contact with the leader.
func (rc *RaftCluster) LastContact() time.Duration {
	if rc.raft == nil {
		return time.Hour
	}
	return time.Since(rc.raft.LastContact())
}

// Raft returns the underlying *raft.Raft for advanced operations.
func (rc *RaftCluster) Raft() *raft.Raft {
	return rc.raft
}

func (rc *RaftCluster) trackLeadership() {
	leaderCh := rc.raft.LeaderCh()
	for {
		select {
		case isLeader := <-leaderCh:
			rc.isLeader.Store(isLeader)
			if isLeader {
				rc.leaderAddr.Store(string(rc.raft.Leader()))
			}
			slog.Info("raft cluster: leadership changed", "node_id", rc.nodeID, "is_leader", isLeader)

			rc.leaderSubsMu.RLock()
			for _, fn := range rc.leaderSubs {
				fn(isLeader)
			}
			rc.leaderSubsMu.RUnlock()

		case <-rc.stopCh:
			return
		}
	}
}
