package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/premchandkpc/FlowRulZ/server/internal/admin"
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/server/internal/common"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/membership"
	"github.com/premchandkpc/FlowRulZ/server/internal/node"
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
)

type NodeBuilder struct {
	cfg  node.Config
	deps node.Dependencies
	errs []error

	lifecycle *common.LifecycleRegistry
}

func NewNodeBuilder(cfg node.Config) *NodeBuilder {
	return &NodeBuilder{
		cfg:       cfg,
		lifecycle: common.NewLifecycleRegistry(),
	}
}

func (b *NodeBuilder) WithDefaults() *NodeBuilder {
	b.buildEngine()
	b.buildMetrics()
	b.buildScheduler()
	b.buildReplyRouter()
	b.buildReliability()
	b.buildRegistry()
	b.buildClusterNode()
	b.buildMessaging()
	b.buildDLQ()
	b.buildMembership()
	b.buildPlanDistribution()
	b.buildPartitioning()
	b.buildAdmin()
	b.buildStateStore()
	b.buildSaga()
	b.buildRaft()
	b.buildGRPC()
	b.buildOTel()
	return b
}

func (b *NodeBuilder) Build() (*node.ProdNode, error) {
	if len(b.errs) > 0 {
		return nil, fmt.Errorf("bootstrap: %d errors: %v", len(b.errs), b.errs[0])
	}
	pn := node.NewNode(b.cfg, b.deps)
	if len(b.errs) > 0 {
		return nil, fmt.Errorf("bootstrap: %d errors: %v", len(b.errs), b.errs[0])
	}
	return pn, nil
}

func (b *NodeBuilder) BuildWithLifecycle(ctx context.Context) (*node.ProdNode, error) {
	pn, err := b.Build()
	if err != nil {
		return nil, err
	}
	return pn, nil
}

func (b *NodeBuilder) Lifecycle() *common.LifecycleRegistry {
	return b.lifecycle
}

func (b *NodeBuilder) register(name string, svc common.Service) {
	b.lifecycle.Register(name, svc)
}

// --- internal builders ---

func (b *NodeBuilder) buildEngine() {
	var eng *engine.Engine
	if b.cfg.CompilerAddr != "" {
		eng = engine.NewWithCompiler(b.cfg.PersistPath, compiler.NewRemote(b.cfg.CompilerAddr))
	} else {
		eng = engine.New(b.cfg.PersistPath)
	}
	b.deps.Engine = eng
	b.register("engine", engineService{eng})
}

func (b *NodeBuilder) buildMetrics() {
	b.deps.Metrics = observability.NewMetricsCollector()
}

func (b *NodeBuilder) buildScheduler() {
	sched := scheduler.New(nil)
	b.deps.Scheduler = sched
	b.register("scheduler", schedulerService{sched})
}

func (b *NodeBuilder) buildReplyRouter() {
	b.deps.ReplyRouter = replyrouter.New(
		replyrouter.WithCleanupInterval(b.cfg.ReplyRouterCleanupInterval()),
		replyrouter.WithMaxPending(b.cfg.ReplyRouterMaxPending()),
	)
}

func (b *NodeBuilder) buildReliability() {
	dedup := reliability.NewDedupTracker(b.cfg.DedupCapacity(), b.cfg.DedupTTL())
	rateLimiter := reliability.NewRateLimiter()
	_ = dedup
	_ = rateLimiter
	b.deps.Dedup = dedup
	b.deps.RateLimiter = rateLimiter
}

func (b *NodeBuilder) buildRegistry() {
	svcRegistry := registry.New()
	svcRegistry.SetHeartbeatTimeout(b.cfg.RegistryHeartbeatTimeout())
	b.deps.Registry = svcRegistry
}

func (b *NodeBuilder) buildClusterNode() {
	var clusterNode *cluster.ClusterNode
	if len(b.cfg.KafkaBrokers) == 0 {
		clusterNode = cluster.NewClusterNode(b.cfg.NodeID, b.cfg.GRPCListenAddr())
	}
	b.deps.ClusterNode = clusterNode
}

func (b *NodeBuilder) buildMessaging() {
	kafkaCfg := kafkatransport.Config{
		Brokers:    b.cfg.KafkaBrokers,
		GroupID:    b.cfg.KafkaGroupID,
		Acks:       kafkatransport.AcksLevelFromString(b.cfg.KafkaAcks),
		Idempotent: b.cfg.KafkaIdempotent,
	}
	_ = kafkaCfg
}

func (b *NodeBuilder) buildDLQ() {
	dlqDir := b.cfg.DLQDir()
	os.MkdirAll(dlqDir, 0755)
	dlqProducer := makeProducerFromCluster(reliability.DefaultDLQTopic, b.deps.ClusterNode, kafkatransport.Config{})
	dlq := reliability.NewDLQ(b.cfg.DLQMaxEntries(),
		reliability.WithDLQProducer(dlqProducer),
		reliability.WithDLQDir(dlqDir),
	)
	b.deps.DLQ = dlq
}

func (b *NodeBuilder) buildMembership() {
	members := membership.New()
	b.deps.Membership = members
}

func (b *NodeBuilder) buildPlanDistribution() {
	kc := kafkatransport.Config{Brokers: b.cfg.KafkaBrokers, GroupID: b.cfg.KafkaGroupID}
	planProducer := makeProducerFromCluster(plandist.DefaultPlanTopic, b.deps.ClusterNode, kc)
	ackProducer := makeProducerFromCluster(plandist.DefaultAckTopic, b.deps.ClusterNode, kc)
	planDist := plandist.New(b.cfg.NodeID,
		plandist.WithPlanProducer(planProducer),
		plandist.WithAckProducer(ackProducer),
		plandist.WithQuorumProvider(b.deps.Membership),
	)
	b.deps.PlanDist = planDist
}

func (b *NodeBuilder) buildPartitioning() {
	partitions := partition.New(partition.DefaultNumPartitions)
	kc := kafkatransport.Config{Brokers: b.cfg.KafkaBrokers}
	partProducer := makeProducerFromCluster(partition.PartitionTopic, b.deps.ClusterNode, kc)
	partitions.SetProducer(partProducer)
	b.deps.Partitions = partitions

	rebalancer := partition.NewRebalanceNotifier(partitions,
		func() []string { return b.deps.Membership.AliveNodes() },
		func() uint64 { return b.deps.PlanDist.CurrentTerm() },
	)
	b.deps.Rebalancer = rebalancer
}

func (b *NodeBuilder) buildAdmin() {
	var adminSrv *admin.Server
	if b.cfg.CompilerAddr != "" {
		adminSrv = admin.NewWithCompiler(b.deps.Engine, compiler.NewRemote(b.cfg.CompilerAddr))
	} else {
		adminSrv = admin.New(b.deps.Engine)
	}
	adminSrv.RegisterDLQ(b.deps.DLQ)
	b.deps.AdminSrv = adminSrv
}

func (b *NodeBuilder) buildStateStore() {
	execDir := b.cfg.ExecDir()
	store, err := execstate.NewFileStore(execDir)
	if err != nil {
		slog.Warn("execstate: init warning", "error", err)
	}
	b.deps.StateStore = store
}

func (b *NodeBuilder) buildSaga() {
	execDir := b.cfg.ExecDir()
	b.deps.Saga = reliability.NewSagaTrackerWithDir(func(svc, method string, body []byte) error {
		return nil
	}, execDir)
}

func (b *NodeBuilder) buildRaft() {
	var raftCluster pkgcluster.ClusterMember
	if b.cfg.RaftDir != "" && b.cfg.RaftPort > 0 {
		raftBind := fmt.Sprintf("localhost:%d", b.cfg.RaftPort)
		rc := cluster.NewRaftCluster(b.cfg.NodeID, b.cfg.RaftDir, raftBind)
		raftCluster = cluster.NewClusterMember(rc)
	}
	b.deps.Cluster = raftCluster
}

func (b *NodeBuilder) buildGRPC() {
	var grpcBus *grpctransport.GRPCBus
	if b.cfg.GRPCAddr != "" {
		grpcBus = grpctransport.NewGRPCBus(b.cfg.GRPCAddr)
	}
	b.deps.GRPCBus = grpcBus
}

func (b *NodeBuilder) buildOTel() {
	var otelExporter *observability.SpanExporter
	if ep := os.Getenv("FLOWRULZ_OTEL_ENDPOINT"); ep != "" {
		otelExporter = observability.NewSpanExporter(ep)
	}
	b.deps.OtelExporter = otelExporter
}

// --- adapters for lifecycle ---

type engineService struct{ e *engine.Engine }
func (s engineService) Start(ctx context.Context) error { return nil }
func (s engineService) Stop() error                     { return nil }

type schedulerService struct{ s *scheduler.Scheduler }
func (s schedulerService) Start(ctx context.Context) error { return s.s.Start(ctx) }
func (s schedulerService) Stop() error                     { return s.s.Stop() }

// --- helpers ---

func makeProducerFromCluster(topic string, clusterNode *cluster.ClusterNode, kc kafkatransport.Config) transport.MessageProducer {
	if len(kc.Brokers) > 0 {
		return kafkatransport.NewProducer(topic, kc)
	}
	if clusterNode != nil {
		return cluster.NewClusterProducer(topic, clusterNode)
	}
	return transport.NewProducer(topic)
}

func makeConsumerFromCluster(topic string, handler transport.MessageHandler, clusterNode *cluster.ClusterNode, kc kafkatransport.Config) transport.MessageConsumer {
	if len(kc.Brokers) > 0 {
		return kafkatransport.NewConsumer(topic, handler, kc)
	}
	if clusterNode != nil {
		return cluster.NewClusterConsumer(topic, handler, clusterNode)
	}
	return transport.NewConsumer(topic, handler)
}
