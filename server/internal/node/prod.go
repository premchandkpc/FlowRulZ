package node

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/admin"
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/plugins"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
	grpctransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/grpc"
	kafkatransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/kafka"

	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgmembership "github.com/premchandkpc/FlowRulZ/server/pkg/membership"
	pkgnode "github.com/premchandkpc/FlowRulZ/server/pkg/node"
	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
	pkgreplyrouter "github.com/premchandkpc/FlowRulZ/server/pkg/replyrouter"
	pkgscheduler "github.com/premchandkpc/FlowRulZ/server/pkg/scheduler"
)

var _ pkgnode.Node = (*ProdNode)(nil)

type Dependencies struct {
	Engine       *engine.Engine
	Scheduler    pkgscheduler.Scheduler
	ReplyRouter  pkgreplyrouter.ReplyRouter
	PlanDist     *plandist.PlanDistributor
	Membership   pkgmembership.Membership
	Partitions   pkgpartition.PartitionManager
	Rebalancer   pkgpartition.RebalanceNotifier
	Registry     *registry.ServiceRegistry
	DLQ          *reliability.DLQ
	RateLimiter  *reliability.RateLimiter
	Dedup        *reliability.DedupTracker
	Saga         *reliability.SagaTracker
	StateStore   *execstate.FileStore
	Cluster      pkgcluster.ClusterMember
	ClusterNode  *cluster.ClusterNode
	GRPCBus      *grpctransport.GRPCBus
	AdminSrv     *admin.Server
	Metrics      *observability.MetricsCollector
	OtelExporter *observability.SpanExporter
}

type ProdNode struct {
	// Interface dependencies
	Scheduler   pkgscheduler.Scheduler
	ReplyRouter pkgreplyrouter.ReplyRouter
	Membership  pkgmembership.Membership
	Partitions  pkgpartition.PartitionManager
	Rebalancer  pkgpartition.RebalanceNotifier

	// Concrete dependencies
	PlanDist *plandist.PlanDistributor

	// Concrete dependencies
	Engine       *engine.Engine
	Registry     *registry.ServiceRegistry
	DLQ          *reliability.DLQ
	RateLimiter  *reliability.RateLimiter
	Dedup        *reliability.DedupTracker
	Saga         *reliability.SagaTracker
	StateStore   *execstate.FileStore
	Execs        *ExecRegistry
	GRPCBus      *grpctransport.GRPCBus
	RaftCluster  pkgcluster.ClusterMember
	ClusterNode  *cluster.ClusterNode
	AdminSrv     *admin.Server
	Metrics      *observability.MetricsCollector
	OtelExporter *observability.SpanExporter

	// Unexported internals
	httpServer      *http.Server
	consumers       []transport.MessageConsumer
	producers       []transport.MessageProducer
	config          Config
	nodeID          string
	httpAddr        string
	httpClient      *http.Client
	serviceCaller   *ServiceCaller
	circuitBreakers sync.Map
	execSem         chan struct{} // Node-wide concurrency limiter for executeAll
	mu              sync.Mutex
	distributeWg    sync.WaitGroup
}

func NewNode(cfg Config, deps Dependencies) *ProdNode {
	n := &ProdNode{
		nodeID:     cfg.NodeID,
		httpAddr:   cfg.HTTPListenAddr(),
		config:     cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		consumers:  make([]transport.MessageConsumer, 0),
		producers:  make([]transport.MessageProducer, 0),
		execSem:    make(chan struct{}, executeAllSemaphore),

		Engine:       deps.Engine,
		Scheduler:    deps.Scheduler,
		ReplyRouter:  deps.ReplyRouter,
		PlanDist:     deps.PlanDist,
		Membership:   deps.Membership,
		Partitions:   deps.Partitions,
		Rebalancer:   deps.Rebalancer,
		Registry:     deps.Registry,
		DLQ:          deps.DLQ,
		RateLimiter:  deps.RateLimiter,
		Dedup:        deps.Dedup,
		Saga:         deps.Saga,
		StateStore:   deps.StateStore,
		RaftCluster:  deps.Cluster,
		ClusterNode:  deps.ClusterNode,
		GRPCBus:      deps.GRPCBus,
		AdminSrv:     deps.AdminSrv,
		Metrics:      deps.Metrics,
		OtelExporter: deps.OtelExporter,
	}

	if cfg.HasTLS() {
		n.serviceCaller = NewServiceCallerWithTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	} else {
		n.serviceCaller = NewServiceCaller()
	}

	n.Execs = NewExecRegistry()

	n.Registry.SetHeartbeatTimeout(cfg.RegistryHeartbeatTimeout())

	n.configureEngineHooks()
	n.configureSagaCompensator()

	// Plugins
	if cfg.PluginDir != "" {
		if err := plugins.LoadDir(cfg.PluginDir); err != nil {
			slog.Warn("plugin load warning", "error", err)
		}
	} else if pd := os.Getenv("FLOWRULZ_PLUGIN_DIR"); pd != "" {
		if err := plugins.LoadDir(pd); err != nil {
			slog.Warn("plugin load warning", "error", err)
		}
	}

	return n
}

// --- pkg/node.Node interface compliance ---

func (n *ProdNode) ID() pkgnode.ID {
	return pkgnode.ID(n.nodeID)
}

func (n *ProdNode) Addr() string {
	return n.httpAddr
}

func (n *ProdNode) IsLeader() bool {
	if n.RaftCluster != nil {
		return n.RaftCluster.IsLeader()
	}
	// Single-node mode: no Raft configured, always leader.
	// WARNING: In multi-node deployments, RaftCluster MUST be configured.
	// Membership alone does NOT provide consensus — it uses lowest-ID election.
	return true
}

func (n *ProdNode) CurrentTerm() uint64 {
	if n.RaftCluster != nil {
		return n.RaftCluster.CurrentTerm()
	}
	return n.PlanDist.CurrentTerm()
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
func (n *ProdNode) CaptureLeadershipToken() pkgcluster.LeadershipToken {
	if n.RaftCluster != nil {
		return n.RaftCluster.CaptureLeadershipToken()
	}
	// No Raft configured — always leader with term 0.
	return pkgcluster.LeadershipToken{Leader: true, Term: 0}
}

// ValidateLeadershipToken checks if a previously captured token is still valid.
func (n *ProdNode) ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool {
	if n.RaftCluster != nil {
		return n.RaftCluster.ValidateLeadershipToken(token)
	}
	// No Raft configured — token is always valid.
	return token.Valid()
}

func (n *ProdNode) LeaderID() pkgnode.ID {
	if n.RaftCluster != nil {
		if n.RaftCluster.IsLeader() {
			return pkgnode.ID(n.nodeID)
		}
		// Raft is authoritative — return Raft's leader, not Membership's.
		if addr := n.RaftCluster.LeaderAddr(); addr != "" {
			return pkgnode.ID(addr)
		}
		return pkgnode.ID(n.nodeID)
	}
	// Single-node fallback: Membership's lowest-ID heuristic.
	return pkgnode.ID(n.Membership.LeaderID())
}

func (n *ProdNode) Ready(ctx context.Context) error {
	if n.IsLeader() && n.PlanDist.CurrentTerm() == 0 {
		return fmt.Errorf("leader not initialized")
	}
	return nil
}

func (n *ProdNode) Execute(ctx context.Context, req *pkgnode.ExecuteRequest) (*pkgnode.ExecuteResponse, error) {
	out, err := n.executeAll(ctx, req.Body)
	if err != nil {
		return &pkgnode.ExecuteResponse{Error: err.Error()}, err
	}
	if len(out) == 0 {
		return &pkgnode.ExecuteResponse{}, nil
	}
	return &pkgnode.ExecuteResponse{Body: out[0]}, nil
}

// --- Lifecycle ---

func (n *ProdNode) Start(ctx context.Context) error {
	if len(n.config.Seeds) > 0 && n.RaftCluster == nil {
		return fmt.Errorf("node: multi-node deployment requires Raft — set RaftDir and RaftPort, or remove Seeds config")
	}

	handler := n.handleIncomingMessage
	kafkaCfg := kafkatransport.Config{
		Brokers:    n.config.KafkaBrokers,
		GroupID:    n.config.KafkaGroupID,
		Acks:       kafkatransport.AcksLevelFromString(n.config.KafkaAcks),
		Idempotent: n.config.KafkaIdempotent,
	}

	n.startCluster(ctx)
	n.startConsumers(ctx, handler, kafkaCfg)
	n.startSubsystems(ctx)
	n.startGRPC()
	n.startOTel(ctx)
	n.serveHTTP(ctx)

	slog.Info("prodnode started", "node_id", n.nodeID)
	return nil
}

func (n *ProdNode) Shutdown(ctx context.Context) error {
	slog.Info("shutdown: starting", "node_id", n.nodeID)

	n.Execs.CancelAll()

	n.mu.Lock()
	consumers := n.consumers
	n.consumers = nil
	producers := n.producers
	n.producers = nil
	n.mu.Unlock()

	for _, c := range consumers {
		c.Stop()
	}

	n.PlanDist.Stop()
	n.distributeWg.Wait()
	n.Scheduler.Stop()
	n.ReplyRouter.StopCleanup()

	for _, p := range producers {
		p.Close()
	}

	if n.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.httpServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown error", "error", err)
		}
	}

	if n.ClusterNode != nil {
		n.ClusterNode.Stop()
	}
	if n.GRPCBus != nil {
		n.GRPCBus.Stop()
	}
	if n.OtelExporter != nil {
		n.OtelExporter.Stop()
	}
	if n.RaftCluster != nil {
		n.RaftCluster.Stop(ctx)
	}
	if n.StateStore != nil {
		n.StateStore.Close()
	}
	if n.serviceCaller != nil {
		n.serviceCaller.Close()
	}

	slog.Info("shutdown: complete", "node_id", n.nodeID)
	return nil
}

// --- Internal methods ---

func (n *ProdNode) makeConsumer(topic string, handler transport.MessageHandler, kc kafkatransport.Config) transport.MessageConsumer {
	if len(kc.Brokers) > 0 {
		return kafkatransport.NewConsumer(topic, handler, kc)
	}
	if n.ClusterNode != nil {
		return cluster.NewClusterConsumer(topic, handler, n.ClusterNode)
	}
	return transport.NewConsumer(topic, handler)
}
