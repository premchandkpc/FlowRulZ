package node

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/bridge"
	"github.com/premchandkpc/FlowRulZ/go/internal/admin"
	"github.com/premchandkpc/FlowRulZ/go/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/go/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/go/internal/engine"
	"github.com/premchandkpc/FlowRulZ/go/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/go/internal/membership"
	"github.com/premchandkpc/FlowRulZ/go/internal/observability"
	"github.com/premchandkpc/FlowRulZ/go/internal/partition"
	"github.com/premchandkpc/FlowRulZ/go/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/go/internal/plugins"
	"github.com/premchandkpc/FlowRulZ/go/internal/registry"
	"github.com/premchandkpc/FlowRulZ/go/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/go/internal/replyrouter"
	"github.com/premchandkpc/FlowRulZ/go/internal/scheduler"
	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
	grpctransport "github.com/premchandkpc/FlowRulZ/go/internal/transport/grpc"

	pkgnode "github.com/premchandkpc/FlowRulZ/go/pkg/node"
)

type ProdNode struct {
	// Core subsystems
	Engine      *engine.Engine
	Scheduler   *scheduler.Scheduler
	ReplyRouter *replyrouter.ReplyRouter
	PlanDist    *plandist.PlanDistributor
	Membership  *membership.Membership
	Partitions  *partition.Manager
	Rebalancer  *partition.RebalanceNotifier
	Registry    *registry.ServiceRegistry
	DLQ         *reliability.DLQ
	RateLimiter *reliability.RateLimiter
	Dedup       *reliability.DedupTracker
	Metrics     *observability.MetricsCollector
	Saga        *reliability.SagaTracker
	StateStore  *execstate.FileStore
	Execs       *ExecRegistry

	// Transport
	GRPCBus      *grpctransport.GRPCBus
	consumers    []transport.MessageConsumer
	producers    []transport.MessageProducer

	// Cluster
	RaftCluster *cluster.RaftCluster
	ClusterNode *cluster.ClusterNode
	OtelExporter *observability.SpanExporter
	AdminSrv    *admin.Server

	// HTTP
	httpServer *http.Server

	// Identity
	nodeID   string
	httpAddr string

	// Config
	config Config

	// Dependencies
	httpClient      *http.Client
	circuitBreakers sync.Map
	mu              sync.Mutex
	shutdownCh      chan struct{}
}

func NewProdNode(cfg *Config) *ProdNode {
	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = DefaultNodeID
	}

	n := &ProdNode{
		nodeID:       nodeID,
		httpAddr:     cfg.HTTPListenAddr(),
		config:       *cfg,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		shutdownCh:   make(chan struct{}),
		consumers:    make([]transport.MessageConsumer, 0),
		producers:    make([]transport.MessageProducer, 0),
		Registry:     registry.New(),
	}

	// Engine (with optional remote compiler)
	if cfg.CompilerAddr != "" {
		n.Engine = engine.NewWithCompiler(cfg.PersistPath, compiler.NewRemote(cfg.CompilerAddr))
	} else {
		n.Engine = engine.New(cfg.PersistPath)
	}

	// Observability
	n.Metrics = observability.NewMetricsCollector()
	n.Scheduler = scheduler.New(nil)
	n.ReplyRouter = replyrouter.New(
		replyrouter.WithCleanupInterval(cfg.ReplyRouterCleanupInterval()),
		replyrouter.WithMaxPending(cfg.ReplyRouterMaxPending()),
	)
	n.Dedup = reliability.NewDedupTracker(cfg.DedupCapacity(), cfg.DedupTTL())
	n.RateLimiter = reliability.NewRateLimiter()

	// Cluster node (only if no Kafka brokers configured)
	if len(cfg.KafkaBrokers) == 0 {
		n.ClusterNode = cluster.NewClusterNode(nodeID, cfg.GRPCListenAddr())
	}

	// Transport config
	kafkaCfg := transport.KafkaConfig{
		Brokers:    cfg.KafkaBrokers,
		GroupID:    cfg.KafkaGroupID,
		Acks:       transport.AcksLevelFromString(cfg.KafkaAcks),
		Idempotent: cfg.KafkaIdempotent,
	}

	// DLQ
	dlqProducer := n.makeProducer(reliability.DefaultDLQTopic, kafkaCfg)
	dlqDir := cfg.DLQDir()
	os.MkdirAll(dlqDir, 0755)
	n.DLQ = reliability.NewDLQ(cfg.DLQMaxEntries(),
		reliability.WithDLQProducer(dlqProducer),
		reliability.WithDLQDir(dlqDir),
	)

	// Membership + Plan Distribution
	planProducer := n.makeProducer(plandist.DefaultPlanTopic, kafkaCfg)
	ackProducer := n.makeProducer(plandist.DefaultAckTopic, kafkaCfg)
	n.Membership = membership.New()
	n.PlanDist = plandist.New(nodeID,
		plandist.WithPlanProducer(planProducer),
		plandist.WithAckProducer(ackProducer),
		plandist.WithQuorumProvider(n.Membership),
	)

	// Partitioning
	n.Partitions = partition.New(partition.DefaultNumPartitions)
	partProducer := n.makeProducer(partition.PartitionTopic, kafkaCfg)
	n.Partitions.SetProducer(partProducer)
	n.Rebalancer = partition.NewRebalanceNotifier(n.Partitions,
		func() []string { return n.Membership.AliveNodes() },
		func() uint64 { return n.PlanDist.CurrentTerm() },
	)

	n.mu.Lock()
	n.producers = append(n.producers, partProducer)
	n.mu.Unlock()

	// Admin server
	if cfg.CompilerAddr != "" {
		n.AdminSrv = admin.NewWithCompiler(n.Engine, compiler.NewRemote(cfg.CompilerAddr))
	} else {
		n.AdminSrv = admin.New(n.Engine)
	}
	n.AdminSrv.RegisterDLQ(n.DLQ)

	// Wire engine hooks for plan distribution
	n.configureEngineHooks()

	n.mu.Lock()
	n.producers = append(n.producers, dlqProducer, planProducer, ackProducer)
	n.mu.Unlock()

	// Execution state store
	execDir := cfg.ExecDir()
	store, err := execstate.NewFileStore(execDir)
	if err != nil {
		slog.Warn("execstate: init warning", "error", err)
	}
	n.StateStore = store
	n.Execs = NewExecRegistry()

	// Saga tracker
	n.Saga = reliability.NewSagaTrackerWithDir(func(svc, method string, body []byte) error {
		_, err := n.callService(svc, method, body, 0)
		return err
	}, execDir)

	n.Registry.SetHeartbeatTimeout(cfg.RegistryHeartbeatTimeout())

	// Raft
	if cfg.RaftDir != "" && cfg.RaftPort > 0 {
		raftBind := fmt.Sprintf("localhost:%d", cfg.RaftPort)
		n.RaftCluster = cluster.NewRaftCluster(nodeID, cfg.RaftDir, raftBind)
	}

	// gRPC bus
	if cfg.GRPCAddr != "" {
		n.GRPCBus = grpctransport.NewGRPCBus(cfg.GRPCAddr)
	}

	// OpenTelemetry
	if ep := os.Getenv("FLOWRULZ_OTEL_ENDPOINT"); ep != "" {
		n.OtelExporter = observability.NewSpanExporter(ep)
	}

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
	return true
}

func (n *ProdNode) CurrentTerm() uint64 {
	if n.RaftCluster != nil {
		return n.RaftCluster.CurrentTerm()
	}
	return n.PlanDist.CurrentTerm()
}

func (n *ProdNode) LeaderID() pkgnode.ID {
	if n.RaftCluster != nil {
		return pkgnode.ID(n.RaftCluster.LeaderID())
	}
	return pkgnode.ID(n.nodeID)
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
	handler := n.handleIncomingMessage
	kafkaCfg := transport.KafkaConfig{
		Brokers:    n.config.KafkaBrokers,
		GroupID:    n.config.KafkaGroupID,
		Acks:       transport.AcksLevelFromString(n.config.KafkaAcks),
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

	for _, c := range n.consumers {
		c.Stop()
	}
	n.consumers = nil

	n.PlanDist.Stop()
	n.Scheduler.Stop()
	n.ReplyRouter.StopCleanup()

	for _, p := range n.producers {
		p.Close()
	}
	n.producers = nil

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
		n.RaftCluster.Stop()
	}
	if n.StateStore != nil {
		n.StateStore.Close()
	}

	close(n.shutdownCh)
	slog.Info("shutdown: complete", "node_id", n.nodeID)
	return nil
}

// --- HTTP server ---

func (n *ProdNode) serveHTTP(ctx context.Context) {
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", n.AdminSrv.Handler()))
	mux.HandleFunc("/register", n.Registry.RegisterHTTPHandler)
	mux.HandleFunc("/heartbeat", n.Registry.HeartbeatHTTPHandler)
	mux.HandleFunc("/services", n.Registry.ListServicesHTTPHandler)
	n.registerHandlers(mux)

	n.httpServer = &http.Server{Addr: n.httpAddr, Handler: mux}
	go func() {
		slog.Info("HTTP server started", "addr", n.httpAddr)
		if err := n.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()
}

// --- Internal methods (adapted from execnode) ---

func (n *ProdNode) makeProducer(topic string, kc transport.KafkaConfig) transport.MessageProducer {
	if len(kc.Brokers) > 0 {
		p := transport.NewKafkaProducer(topic, kc)
		n.mu.Lock()
		n.producers = append(n.producers, p)
		n.mu.Unlock()
		return p
	}
	if n.ClusterNode != nil {
		return cluster.NewClusterProducer(topic, n.ClusterNode)
	}
	return transport.NewProducer(topic)
}

func (n *ProdNode) makeConsumer(topic string, handler transport.MessageHandler, kc transport.KafkaConfig) transport.MessageConsumer {
	if len(kc.Brokers) > 0 {
		return transport.NewKafkaConsumer(topic, handler, kc)
	}
	if n.ClusterNode != nil {
		return cluster.NewClusterConsumer(topic, handler, n.ClusterNode)
	}
	return transport.NewConsumer(topic, handler)
}
