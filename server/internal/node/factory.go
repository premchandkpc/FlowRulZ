package node

import (
	"context"
	"fmt"
	"os"

	"github.com/premchandkpc/FlowRulZ/server/internal/admin"
	"github.com/premchandkpc/FlowRulZ/server/internal/cache"
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/flow"
	"github.com/premchandkpc/FlowRulZ/server/internal/invoker"
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

func DefaultDependencies(cfg Config) Dependencies {
	// Engine
	var eng *engine.Engine
	if cfg.CompilerAddr != "" {
		eng = engine.NewWithCompiler(cfg.PersistPath, compiler.NewRemote(cfg.CompilerAddr))
	} else {
		eng = engine.New(cfg.PersistPath)
	}

	// Metrics
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

	// Transport factory
	transportFactory := kafkatransport.NewTransportFactoryFromConfig(
		kafkatransport.RegistrationConfig{
			Brokers:    cfg.KafkaBrokers,
			GroupID:    cfg.KafkaGroupID,
			Acks:       cfg.KafkaAcks,
			Idempotent: cfg.KafkaIdempotent,
		},
	)

	if rawClusterNode != nil {
		cluster.RegisterClusterTransport(transportFactory, rawClusterNode)
		transportFactory.SetKind(transport.KindCluster)
	}

	// DLQ
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

	// Execution state store
	store := execstate.NewMemoryStore()

	// Saga tracker
	saga := reliability.NewSagaTracker(func(svc, method string, body []byte) error {
		return nil
	})

	// Service invoker
	svcInvoker := invoker.NewProductionInvoker(svcRegistry)

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

	flowDirs := []string{}
	if fd := os.Getenv("FLOWRULZ_FLOW_DIR"); fd != "" {
		flowDirs = append(flowDirs, fd)
	}
	_ = flowRegistry.LoadDirectory(context.Background(), ".")

	return Dependencies{
		Engine:           eng,
		Scheduler:        sched,
		ReplyRouter:      replyRouter,
		PlanDist:         planDist,
		Membership:       members,
		Partitions:       partitions,
		Rebalancer:       rebalancer,
		Registry:         svcRegistry,
		DLQ:              dlq,
		RateLimiter:      rateLimiter,
		Dedup:            dedup,
		Saga:             saga,
		StateStore:       store,
		Invoker:          svcInvoker,
		TransportFactory: transportFactory,
		Cluster:          raftCluster,
		ClusterNode:      clusterNode,
		GRPCBus:          grpcBus,
		AdminSrv:         adminSrv,
		Metrics:          &metricsAdapter{inner: metrics},
		OtelExporter:     otelExporter,
		FlowRegistry:     flowRegistry,
		FlowDirs:         flowDirs,
	}
}
