package node

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/flow"
	"github.com/premchandkpc/FlowRulZ/server/internal/plugins"
	"github.com/premchandkpc/FlowRulZ/server/internal/scheduler"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgmembership "github.com/premchandkpc/FlowRulZ/server/pkg/membership"
	pkgnode "github.com/premchandkpc/FlowRulZ/server/pkg/node"
	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
	pkgreplyrouter "github.com/premchandkpc/FlowRulZ/server/pkg/replyrouter"
	pkgscheduler "github.com/premchandkpc/FlowRulZ/server/pkg/scheduler"
)

// Dependencies holds all dependencies for node construction.
type Dependencies struct {
	Engine           NodeEngine
	Scheduler        pkgscheduler.Scheduler
	ReplyRouter      pkgreplyrouter.ReplyRouter
	PlanDist         PlanDistributor
	Membership       pkgmembership.Membership
	Partitions       pkgpartition.PartitionManager
	Rebalancer       pkgpartition.RebalanceNotifier
	Registry         ServiceLookup
	DLQ              NodeDLQ
	RateLimiter      RateLimiter
	Dedup            DedupChecker
	Saga             NodeSagaTracker
	StateStore       StateStore
	Invoker          ServiceInvoker
	TransportFactory TransportFactory
	Cluster          pkgcluster.ClusterMember
	ClusterNode      ClusterTransport
	GRPCBus          GRPCService
	AdminSrv         AdminHandler
	Metrics          MetricsSnapshotProvider
	OtelExporter     SpanExporter
	FlowRegistry     *flow.Registry
	FlowDirs         []string
}

type ProdNode struct {
	execution  *ExecutionEngine
	ingress    *IngressPipeline
	msgRouter  *MessageRouter
	httpServer *AdminHTTPServer
	leadership LeadershipStrategy
	flowWatch  *flow.FileWatcher

	config    NodeConfig
	cluster   ClusterDeps
	transport TransportDeps
	exec      ExecutionDeps
	reliab    ReliabilityDeps
	api       APIDeps
	part      PartitionDeps
}

func NewNode(cfg Config, deps Dependencies) *ProdNode {
	var strategy LeadershipStrategy
	if deps.Cluster != nil {
		strategy = NewRaftLeadershipStrategy(deps.Cluster)
	} else {
		sls := NewSingleLeaderStrategy(deps.PlanDist)
		sls.SetMembership(deps.Membership)
		strategy = sls
	}

	n := &ProdNode{
		config: NodeConfig{
			Config:     cfg,
			httpClient: &http.Client{Timeout: 10 * time.Second},
		},
		leadership: strategy,
		cluster: ClusterDeps{
			RaftCluster: deps.Cluster,
			ClusterNode: deps.ClusterNode,
			Membership:  deps.Membership,
		},
		transport: TransportDeps{
			TransportFactory: deps.TransportFactory,
			GRPCBus:          deps.GRPCBus,
		},
		exec: ExecutionDeps{
			Engine:     deps.Engine,
			Scheduler:  deps.Scheduler,
			StateStore: deps.StateStore,
			Execs:      NewExecRegistry(),
			Saga:       deps.Saga,
			Invoker:    deps.Invoker,
		},
		reliab: ReliabilityDeps{
			DLQ:         deps.DLQ,
			RateLimiter: deps.RateLimiter,
			Dedup:       deps.Dedup,
		},
		api: APIDeps{
			AdminSrv:     deps.AdminSrv,
			Registry:     deps.Registry,
			ReplyRouter:  deps.ReplyRouter,
			Metrics:      deps.Metrics,
			OtelExporter: deps.OtelExporter,
		},
		part: PartitionDeps{
			Partitions: deps.Partitions,
			Rebalancer: deps.Rebalancer,
			PlanDist:   deps.PlanDist,
		},
	}

	n.execution = NewExecutionEngine(
		deps.Engine,
		deps.Scheduler.(*scheduler.Scheduler),
		deps.StateStore,
		n.exec.Execs,
		deps.Saga,
		deps.Invoker,
	)

	n.ingress = NewIngressPipeline(
		deps.RateLimiter,
		deps.Dedup,
		deps.DLQ,
		n.execution,
	)

	n.msgRouter = NewMessageRouter(
		cfg.NodeID,
		cfg.Topic,
		deps.TransportFactory,
		deps.Membership,
		deps.ClusterNode,
		deps.Engine,
		deps.PlanDist,
		deps.Partitions,
	)

	n.httpServer = NewAdminHTTPServer(
		cfg.HTTPListenAddr(),
		cfg.NodeID,
		deps.AdminSrv,
		deps.Registry.(HTTPRegistry),
		n,
		n.exec.Execs,
		deps.Partitions,
		deps.Membership,
		deps.Metrics,
		deps.DLQ,
		deps.PlanDist,
		deps.ReplyRouter,
		deps.Cluster,
	)

	n.api.Registry.SetHeartbeatTimeout(cfg.RegistryHeartbeatTimeout())

	// Flow registry and file watcher
	if deps.FlowRegistry != nil && len(deps.FlowDirs) > 0 {
		n.flowWatch = flow.NewFileWatcher(deps.FlowRegistry, deps.FlowDirs...)
	}

	n.configureEngineHooks()

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

func NewProdNode(cfg *Config) *ProdNode {
	return NewNode(*cfg, DefaultDependencies(*cfg))
}

// --- pkg/node.Node interface compliance ---

func (n *ProdNode) ID() pkgnode.ID {
	return pkgnode.ID(n.config.NodeID)
}

func (n *ProdNode) Addr() string {
	return n.config.HTTPListenAddr()
}

func (n *ProdNode) IsLeader() bool {
	return n.leadership.IsLeader()
}

func (n *ProdNode) CurrentTerm() uint64 {
	return n.leadership.CurrentTerm()
}

func (n *ProdNode) CaptureLeadershipToken() pkgcluster.LeadershipToken {
	return n.leadership.CaptureLeadershipToken()
}

func (n *ProdNode) ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool {
	return n.leadership.ValidateLeadershipToken(token)
}

func (n *ProdNode) LeaderID() pkgnode.ID {
	return pkgnode.ID(n.leadership.LeaderID(n.config.NodeID))
}

func (n *ProdNode) Ready(ctx context.Context) error {
	if n.IsLeader() && n.part.PlanDist.CurrentTerm() == 0 {
		return fmt.Errorf("leader not initialized")
	}
	return nil
}

func (n *ProdNode) Execute(ctx context.Context, req *pkgnode.ExecuteRequest) (*pkgnode.ExecuteResponse, error) {
	out, err := n.execution.ExecuteAll(ctx, req.Body)
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
	n.startCluster(ctx)
	n.msgRouter.StartConsumers(ctx, n.ingress.HandleMessage)
	n.startSubsystems(ctx)
	if n.transport.GRPCBus != nil {
		if err := n.transport.GRPCBus.Start(); err != nil {
			slog.Error("grpc: start error", "error", err)
		}
	}
	n.startOTel(ctx)
	n.httpServer.ServeHTTP(ctx)

	// Start flow file watcher
	if n.flowWatch != nil {
		if err := n.flowWatch.Start(ctx); err != nil {
			slog.Error("flow watcher: start error", "error", err)
		}
	}

	slog.Info("prodnode started", "node_id", n.config.NodeID)
	return nil
}

func (n *ProdNode) Shutdown(ctx context.Context) error {
	slog.Info("shutdown: starting", "node_id", n.config.NodeID)

	n.exec.Execs.CancelAll()
	n.msgRouter.StopConsumers()
	n.part.PlanDist.Stop()
	n.exec.Scheduler.Stop()
	n.api.ReplyRouter.StopCleanup()

	// Stop flow file watcher
	if n.flowWatch != nil {
		n.flowWatch.Stop()
	}

	if err := n.httpServer.Shutdown(ctx); err != nil {
		slog.Error("http shutdown error", "error", err)
	}
	if n.cluster.ClusterNode != nil {
		n.cluster.ClusterNode.Stop()
	}
	if n.transport.GRPCBus != nil {
		n.transport.GRPCBus.Stop()
	}
	if n.api.OtelExporter != nil {
		n.api.OtelExporter.Stop()
	}
	if n.cluster.RaftCluster != nil {
		n.cluster.RaftCluster.Stop(ctx)
	}
	if n.exec.StateStore != nil {
		n.exec.StateStore.Close()
	}

	slog.Info("shutdown: complete", "node_id", n.config.NodeID)
	return nil
}
