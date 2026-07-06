package node

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/admin"
	"github.com/premchandkpc/FlowRulZ/server/internal/cache"
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/flow"
	"github.com/premchandkpc/FlowRulZ/server/internal/membership"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/partition"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/server/internal/replyrouter"
	"github.com/premchandkpc/FlowRulZ/server/internal/scheduler"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
	grpctransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/grpc"
	kafkatransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/kafka"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
)

// --- Adapters (bridge concrete types → node interfaces) ---

// transportFactoryAdapter wraps *transport.TransportFactory.
type transportFactoryAdapter struct {
	factory *transport.TransportFactory
}

func (a *transportFactoryAdapter) NewConsumer(topic string, handler transport.MessageHandler) transport.MessageConsumer {
	return a.factory.NewConsumer(topic, handler)
}

func (a *transportFactoryAdapter) NewProducer(topic string) transport.MessageProducer {
	return a.factory.NewProducer(topic)
}

// serviceLookupAdapter wraps *registry.ServiceRegistry.
type serviceLookupAdapter struct {
	registry *registry.ServiceRegistry
}

func (a *serviceLookupAdapter) LookupInstance(serviceName, method string) (*registry.ServiceInstance, error) {
	return a.registry.LookupInstance(serviceName, method)
}

func (a *serviceLookupAdapter) MarkUnhealthy(serviceName, nodeID string) {
	a.registry.MarkUnhealthy(serviceName, nodeID)
}

func (a *serviceLookupAdapter) SetHeartbeatTimeout(timeout time.Duration) {
	a.registry.SetHeartbeatTimeout(timeout)
}

func (a *serviceLookupAdapter) RegisterHTTPHandler(w http.ResponseWriter, r *http.Request) {
	a.registry.RegisterHTTPHandler(w, r)
}

func (a *serviceLookupAdapter) HeartbeatHTTPHandler(w http.ResponseWriter, r *http.Request) {
	a.registry.HeartbeatHTTPHandler(w, r)
}

func (a *serviceLookupAdapter) ListServicesHTTPHandler(w http.ResponseWriter, r *http.Request) {
	a.registry.ListServicesHTTPHandler(w, r)
}

// rateLimiterAdapter wraps *reliability.RateLimiter.
type rateLimiterAdapter struct {
	limiter *reliability.RateLimiter
}

func (a *rateLimiterAdapter) Allow(key string) bool {
	return a.limiter.Allow(key)
}

// dedupCheckerAdapter wraps *reliability.DedupTracker.
type dedupCheckerAdapter struct {
	tracker *reliability.DedupTracker
}

func (a *dedupCheckerAdapter) CheckAndMark(key string) bool {
	return a.tracker.CheckAndMark(key)
}

func (a *dedupCheckerAdapter) StartCleanup(ctx context.Context, interval time.Duration) {
	a.tracker.StartCleanup(ctx, interval)
}

// planDistributorAdapter wraps *plandist.PlanDistributor.
type planDistributorAdapter struct {
	distributor *plandist.PlanDistributor
}

func (a *planDistributorAdapter) CurrentTerm() uint64 {
	return a.distributor.CurrentTerm()
}

func (a *planDistributorAdapter) SendAck(ctx context.Context, ruleID string, version uint64, status string) error {
	return a.distributor.SendAck(ctx, ruleID, version, status)
}

func (a *planDistributorAdapter) RecordAck(msg plandist.AckMessage) {
	a.distributor.RecordAck(msg)
}

func (a *planDistributorAdapter) SetTerm(term uint64) {
	a.distributor.SetTerm(term)
}

func (a *planDistributorAdapter) Start(ctx context.Context) error {
	return a.distributor.Start(ctx)
}

func (a *planDistributorAdapter) Stop() error {
	return a.distributor.Stop()
}

func (a *planDistributorAdapter) PublishPlan(ctx context.Context, id string, version uint64, plan []byte, dsl string) error {
	return a.distributor.PublishPlan(ctx, id, version, plan, dsl)
}

func (a *planDistributorAdapter) WaitForAcks(ctx context.Context, id string, version uint64, quorum int, timeout time.Duration) error {
	return a.distributor.WaitForAcks(ctx, id, version, quorum, timeout)
}

func (a *planDistributorAdapter) ActivatePlan(ctx context.Context, id string, version uint64) error {
	return a.distributor.ActivatePlan(ctx, id, version)
}

func DefaultDependencies(cfg Config) Dependencies {
	// Engine
	var eng *engine.Engine
	if cfg.CompilerAddr != "" {
		eng = engine.NewWithCompiler(cfg.PersistPath, compiler.NewRemote(cfg.CompilerAddr))
	} else {
		eng = engine.New(cfg.PersistPath)
	}

	// Metrics — use injectable adapter from new observability adapter
	metrics := observability.NewMetricsCollector()

	// Scheduler
	sched := scheduler.New(nil)

	// ReplyRouter
	replyRouter := replyrouter.New(
		replyrouter.WithCleanupInterval(cfg.ReplyRouterCleanupInterval()),
		replyrouter.WithMaxPending(cfg.ReplyRouterMaxPending()),
	)

	// Reliability basics
	dedup := reliability.NewDedupTracker(cfg.DedupCapacity(), cfg.DedupTTL())
	rateLimiter := reliability.NewRateLimiter()

	// Service registry
	svcRegistry := registry.New()
	svcRegistry.SetHeartbeatTimeout(cfg.RegistryHeartbeatTimeout())

	// Cluster node (only if no Kafka)
	var clusterNode ClusterTransport
	var rawClusterNode *cluster.ClusterNode
	if len(cfg.KafkaBrokers) == 0 {
		rawClusterNode = cluster.NewClusterNode(cfg.NodeID, cfg.GRPCListenAddr())
		clusterNode = NewClusterTransportAdapter(rawClusterNode)
	}

	// Transport factory — centralizes backend selection
	transportFactory := kafkatransport.NewTransportFactoryFromConfig(
		kafkatransport.RegistrationConfig{
			Brokers:    cfg.KafkaBrokers,
			GroupID:    cfg.KafkaGroupID,
			Acks:       cfg.KafkaAcks,
			Idempotent: cfg.KafkaIdempotent,
		},
	)

	// Register cluster backend if available
	if rawClusterNode != nil {
		cluster.RegisterClusterTransport(transportFactory, rawClusterNode)
		transportFactory.SetKind(transport.KindCluster)
	}

	// DLQ — stateless, Kafka-backed when available
	dlqProducer := transportFactory.NewProducer(reliability.DefaultDLQTopic)
	dlq := reliability.NewDLQ(cfg.DLQMaxEntries(),
		reliability.WithDLQProducer(dlqProducer),
	)

	// Membership + Plan Distribution
	members := membership.New()

	planProducer := transportFactory.NewProducer(plandist.DefaultPlanTopic)
	ackProducer := transportFactory.NewProducer(plandist.DefaultAckTopic)
	planDist := plandist.New(cfg.NodeID,
		plandist.WithPlanProducer(planProducer),
		plandist.WithAckProducer(ackProducer),
		plandist.WithQuorumProvider(members),
	)

	// Partitioning
	partitions := partition.New(partition.DefaultNumPartitions)
	partProducer := transportFactory.NewProducer(partition.PartitionTopic)
	partitions.SetProducer(partProducer)
	rebalancer := partition.NewRebalanceNotifier(partitions,
		func() []string { return members.AliveNodes() },
		func() uint64 { return planDist.CurrentTerm() },
	)

	// Admin server
	var adminSrv *admin.Server
	if cfg.CompilerAddr != "" {
		adminSrv = admin.NewWithCompiler(eng, compiler.NewRemote(cfg.CompilerAddr))
	} else {
		adminSrv = admin.New(eng)
	}
	adminSrv.RegisterDLQ(dlq)

	// Execution state store — in-memory only, ephemeral by design
	store := execstate.NewMemoryStore()

	// Saga tracker — in-memory, stateless
	saga := reliability.NewSagaTracker(func(svc, method string, body []byte) error {
		return nil
	})

	// Service invoker — protocol-aware dispatch
	invoker := NewProductionInvoker(&serviceLookupAdapter{registry: svcRegistry})

	// Raft
	var raftCluster pkgcluster.ClusterMember
	if cfg.RaftDir != "" && cfg.RaftPort > 0 {
		raftBind := fmt.Sprintf("localhost:%d", cfg.RaftPort)
		rc := cluster.NewRaftCluster(cfg.NodeID, cfg.RaftDir, raftBind)
		raftCluster = cluster.NewClusterMember(rc)
	}

	// gRPC bus
	var grpcBus *grpctransport.GRPCBus
	if cfg.GRPCAddr != "" {
		grpcBus = grpctransport.NewGRPCBus(cfg.GRPCAddr)
	}

	// OpenTelemetry
	var otelExporter *observability.SpanExporter
	if ep := os.Getenv("FLOWRULZ_OTEL_ENDPOINT"); ep != "" {
		otelExporter = observability.NewSpanExporter(ep)
	}

	// Flow registry
	flowCache := cache.NewMemoryCache()
	flowRegistry := flow.NewRegistry(flowCache)

	// Load .flow files from configured directories
	flowDirs := []string{}
	if fd := os.Getenv("FLOWRULZ_FLOW_DIR"); fd != "" {
		flowDirs = append(flowDirs, fd)
	}
	_ = flowRegistry.LoadDirectory(context.Background(), ".")

	return Dependencies{
		Engine:           eng,
		Scheduler:        sched,
		ReplyRouter:      replyRouter,
		PlanDist:         &planDistributorAdapter{distributor: planDist},
		Membership:       members,
		Partitions:       partitions,
		Rebalancer:       rebalancer,
		Registry:         &serviceLookupAdapter{registry: svcRegistry},
		DLQ:              dlq,
		RateLimiter:      &rateLimiterAdapter{limiter: rateLimiter},
		Dedup:            &dedupCheckerAdapter{tracker: dedup},
		Saga:             saga,
		StateStore:       store,
		Invoker:          invoker,
		TransportFactory: &transportFactoryAdapter{factory: transportFactory},
		Cluster:          raftCluster,
		ClusterNode:      clusterNode,
		GRPCBus:          grpcBus,
		AdminSrv:         adminSrv,
		Metrics:          metrics,
		OtelExporter:     otelExporter,
		FlowRegistry:     flowRegistry,
		FlowDirs:         flowDirs,
	}
}
