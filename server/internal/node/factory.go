package node

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/premchandkpc/FlowRulZ/server/internal/admin"
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
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
	var clusterNode *cluster.ClusterNode
	if len(cfg.KafkaBrokers) == 0 {
		clusterNode = cluster.NewClusterNode(cfg.NodeID, cfg.GRPCListenAddr())
	}

	// Transport config
	kafkaCfg := kafkatransport.Config{
		Brokers:    cfg.KafkaBrokers,
		GroupID:    cfg.KafkaGroupID,
		Acks:       kafkatransport.AcksLevelFromString(cfg.KafkaAcks),
		Idempotent: cfg.KafkaIdempotent,
	}

	// DLQ
	dlqDir := cfg.DLQDir()
	os.MkdirAll(dlqDir, 0755)
	dlqProducer := MakeProducerFromCluster(reliability.DefaultDLQTopic, clusterNode, kafkaCfg)
	dlq := reliability.NewDLQ(cfg.DLQMaxEntries(),
		reliability.WithDLQProducer(dlqProducer),
		reliability.WithDLQDir(dlqDir),
	)

	// Membership + Plan Distribution
	members := membership.New()

	planProducer := MakeProducerFromCluster(plandist.DefaultPlanTopic, clusterNode, kafkaCfg)
	ackProducer := MakeProducerFromCluster(plandist.DefaultAckTopic, clusterNode, kafkaCfg)
	planDist := plandist.New(cfg.NodeID,
		plandist.WithPlanProducer(planProducer),
		plandist.WithAckProducer(ackProducer),
		plandist.WithQuorumProvider(members),
	)

	// Partitioning
	partitions := partition.New(partition.DefaultNumPartitions)
	partProducer := MakeProducerFromCluster(partition.PartitionTopic, clusterNode, kafkaCfg)
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
	execDir := cfg.ExecDir()
	store, err := execstate.NewFileStore(execDir)
	if err != nil {
		slog.Warn("execstate: init warning", "error", err)
	}

	// Saga tracker
	saga := reliability.NewSagaTrackerWithDir(func(svc, method string, body []byte) error {
		return nil
	}, execDir)

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
		if cfg.HasTLS() {
			grpcBus = grpctransport.NewGRPCBusWithTLS(cfg.GRPCAddr, cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			grpcBus = grpctransport.NewGRPCBus(cfg.GRPCAddr)
		}
	}

	// OpenTelemetry
	var otelExporter *observability.SpanExporter
	if ep := os.Getenv("FLOWRULZ_OTEL_ENDPOINT"); ep != "" {
		otelExporter = observability.NewSpanExporter(ep)
	}

	return Dependencies{
		Engine:       eng,
		Scheduler:    sched,
		ReplyRouter:  replyRouter,
		PlanDist:     planDist,
		Membership:   members,
		Partitions:   partitions,
		Rebalancer:   rebalancer,
		Registry:     svcRegistry,
		DLQ:          dlq,
		RateLimiter:  rateLimiter,
		Dedup:        dedup,
		Saga:         saga,
		StateStore:   store,
		Cluster:      raftCluster,
		ClusterNode:  clusterNode,
		GRPCBus:      grpcBus,
		AdminSrv:     adminSrv,
		Metrics:      metrics,
		OtelExporter: otelExporter,
	}
}

func MakeProducerFromCluster(topic string, clusterNode *cluster.ClusterNode, kc kafkatransport.Config) transport.MessageProducer {
	if len(kc.Brokers) > 0 {
		return kafkatransport.NewProducer(topic, kc)
	}
	if clusterNode != nil {
		return cluster.NewClusterProducer(topic, clusterNode)
	}
	return transport.NewProducer(topic)
}


