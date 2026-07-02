package execnode

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/admin"
	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/membership"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/partition"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/plugins"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/server/internal/replyrouter"
	"github.com/premchandkpc/FlowRulZ/server/internal/scheduler"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
	grpctransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/grpc"
	kafkatransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/kafka"
)

const (
	DefaultMembersTopic = "_flowrulz_members"
)

type NodeDiscoveryMessage struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type ServiceResolver interface {
	Resolve(svcID uint16, svcName string) (string, error)
}

type ExecutionNode struct {
	Engine      *engine.Engine
	AdminSrv    *admin.Server
	HTTP        *http.Server
	Scheduler   *scheduler.Scheduler
	ReplyRouter *replyrouter.ReplyRouter
	DLQ         *reliability.DLQ
	RateLimiter *reliability.RateLimiter
	Metrics     *observability.MetricsCollector
	Dedup       *reliability.DedupTracker
	PlanDist    *plandist.PlanDistributor
	Membership  *membership.Membership
	Registry    *registry.ServiceRegistry
	RaftCluster *cluster.RaftCluster

	serviceResolver ServiceResolver

	circuitBreakers sync.Map

	Saga *reliability.SagaTracker

	consumers   []transport.MessageConsumer
	producers   []transport.MessageProducer
	httpAddr    string
	nodeID      string
	config      Config
	httpClient  *http.Client
	mu          sync.Mutex
	shutdownCh  chan struct{}
	leaderSubID int

	StateStore *execstate.FileStore
	Execs      *ExecRegistry

	GRPCBus      *grpctransport.GRPCBus
	OtelExporter *observability.SpanExporter
	ClusterNode  *cluster.ClusterNode
	Partitions   *partition.Manager
	Rebalancer   *partition.RebalanceNotifier
}

type Config struct {
	PersistPath     string
	ExecStateDir    string
	HTTPAddr        string
	GRPCAddr        string
	RaftPort        int
	RaftDir         string
	RaftBootstrap   bool
	CompilerAddr    string
	PluginDir       string
	Topic           string
	NodeID          string
	Seeds           []string
	KafkaBrokers    []string
	KafkaGroupID    string
	KafkaAcks       string
	KafkaIdempotent bool
	APIKey          string
}

func NewConfig() *Config {
	return &Config{
		HTTPAddr:      ":8080",
		GRPCAddr:      ":9090",
		RaftPort:      cluster.DefaultRaftPort,
		RaftDir:       filepath.Join(os.TempDir(), "flowrulz-raft"),
		RaftBootstrap: false,
		Topic:         "flowrulz-input",
		NodeID:        "node-1",
		KafkaBrokers:  []string{},
		KafkaGroupID:  "flowrulz",
	}
}

func New(cfg *Config) *ExecutionNode {
	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = "node-1"
	}

	en := &ExecutionNode{
		httpAddr:   cfg.HTTPAddr,
		nodeID:     nodeID,
		config:     *cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		shutdownCh: make(chan struct{}),
		consumers:  make([]transport.MessageConsumer, 0),
		producers:  make([]transport.MessageProducer, 0),
		Registry:   registry.New(),
	}

	if cfg.CompilerAddr != "" {
		en.Engine = engine.NewWithCompiler(cfg.PersistPath, compiler.NewRemote(cfg.CompilerAddr))
	} else {
		en.Engine = engine.New(cfg.PersistPath)
	}
	en.Metrics = observability.NewMetricsCollector()
	en.Scheduler = scheduler.New(nil)
	en.ReplyRouter = replyrouter.New(
		replyrouter.WithCleanupInterval(1*time.Second),
		replyrouter.WithMaxPending(10000),
	)
	en.Dedup = reliability.NewDedupTracker(10000, 5*time.Minute)
	en.RateLimiter = reliability.NewRateLimiter()

	if len(cfg.KafkaBrokers) == 0 {
		grpcAddr := cfg.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = fmt.Sprintf(":%d", 9090)
		}
		en.ClusterNode = cluster.NewClusterNode(nodeID, grpcAddr)
	}

	kafkaCfg := kafkatransport.Config{
		Brokers:    cfg.KafkaBrokers,
		GroupID:    cfg.KafkaGroupID,
		Acks:       kafkatransport.AcksLevelFromString(cfg.KafkaAcks),
		Idempotent: cfg.KafkaIdempotent,
	}

	dlqProducer := en.mkProducer(reliability.DefaultDLQTopic, kafkaCfg)
	dlqDir := cfg.ExecStateDir
	if dlqDir == "" {
		dlqDir = filepath.Join(os.TempDir(), "flowrulz-dlq")
	}
	os.MkdirAll(dlqDir, 0755)
	en.DLQ = reliability.NewDLQ(10000,
		reliability.WithDLQProducer(dlqProducer),
		reliability.WithDLQDir(dlqDir),
	)

	planProducer := en.mkProducer(plandist.DefaultPlanTopic, kafkaCfg)
	ackProducer := en.mkProducer(plandist.DefaultAckTopic, kafkaCfg)
	en.Membership = membership.New()
	en.PlanDist = plandist.New(nodeID,
		plandist.WithPlanProducer(planProducer),
		plandist.WithAckProducer(ackProducer),
		plandist.WithQuorumProvider(en.Membership),
	)

	en.Partitions = partition.New(partition.DefaultNumPartitions)
	partProducer := en.mkProducer(partition.PartitionTopic, kafkaCfg)
	en.Partitions.SetProducer(partProducer)
	en.Rebalancer = partition.NewRebalanceNotifier(en.Partitions,
		func() []string { return en.Membership.AliveNodes() },
		func() uint64 { return en.PlanDist.CurrentTerm() },
	)
	en.mu.Lock()
	en.producers = append(en.producers, partProducer)
	en.mu.Unlock()

	if cfg.CompilerAddr != "" {
		en.AdminSrv = admin.NewWithCompiler(en.Engine, compiler.NewRemote(cfg.CompilerAddr))
	} else {
		en.AdminSrv = admin.New(en.Engine)
	}
	en.AdminSrv.RegisterDLQ(en.DLQ)

	en.configureEngineHooks()

	en.mu.Lock()
	en.producers = append(en.producers, dlqProducer, planProducer, ackProducer)
	en.mu.Unlock()

	execDir := cfg.ExecStateDir
	if execDir == "" {
		execDir = filepath.Join(os.TempDir(), "flowrulz-execstate")
	}
	store, err := execstate.NewFileStore(execDir)
	if err != nil {
		slog.Warn("execstate: init warning", "error", err)
	}
	en.StateStore = store
	en.Execs = NewExecRegistry()

	en.Saga = reliability.NewSagaTrackerWithDir(func(svc, method string, body []byte) error {
		_, err := en.callService(svc, method, body, 0)
		return err
	}, execDir)

	en.Registry.SetHeartbeatTimeout(30 * time.Second)

	if cfg.RaftDir != "" && cfg.RaftPort > 0 {
		raftBind := fmt.Sprintf("localhost:%d", cfg.RaftPort)
		en.RaftCluster = cluster.NewRaftCluster(nodeID, cfg.RaftDir, raftBind)
	}

	if cfg.GRPCAddr != "" {
		en.GRPCBus = grpctransport.NewGRPCBus(cfg.GRPCAddr)
	}

	if ep := os.Getenv("FLOWRULZ_OTEL_ENDPOINT"); ep != "" {
		en.OtelExporter = observability.NewSpanExporter(ep)
	}

	if cfg.PluginDir != "" {
		if err := plugins.LoadDir(cfg.PluginDir); err != nil {
			slog.Warn("plugin load warning", "error", err)
		}
	} else if pd := os.Getenv("FLOWRULZ_PLUGIN_DIR"); pd != "" {
		if err := plugins.LoadDir(pd); err != nil {
			slog.Warn("plugin load warning", "error", err)
		}
	}

	return en
}
