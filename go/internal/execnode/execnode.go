package execnode

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

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
	Engine       *engine.Engine
	AdminSrv     *admin.Server
	HTTP         *http.Server
	Scheduler    *scheduler.Scheduler
	ReplyRouter  *replyrouter.ReplyRouter
	DLQ          *reliability.DLQ
	RateLimiter  *reliability.RateLimiter
	Metrics      *observability.MetricsCollector
	Dedup        *reliability.DedupTracker
	PlanDist     *plandist.PlanDistributor
	Membership   *membership.Membership
	Registry     *registry.ServiceRegistry
	RaftCluster  *cluster.RaftCluster

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

	GRPCBus     *grpctransport.GRPCBus
	OtelExporter *observability.SpanExporter
	ClusterNode  *cluster.ClusterNode
	Partitions  *partition.Manager
	Rebalancer  *partition.RebalanceNotifier
}

type Config struct {
	PersistPath    string
	ExecStateDir   string
	HTTPAddr       string
	GRPCAddr       string
	RaftPort       int
	RaftDir        string
	RaftBootstrap  bool
	CompilerAddr   string
	PluginDir      string
	Topic          string
	NodeID         string
	Seeds          []string
	KafkaBrokers   []string
	KafkaGroupID   string
	KafkaAcks      string
	KafkaIdempotent bool
	APIKey         string
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
		replyrouter.WithCleanupInterval(1 * time.Second),
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

	kafkaCfg := transport.KafkaConfig{
		Brokers:    cfg.KafkaBrokers,
		GroupID:    cfg.KafkaGroupID,
		Acks:       transport.AcksLevelFromString(cfg.KafkaAcks),
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

	en.Engine.AfterDeploy = func(id, dsl string, plan []byte, version uint64) {
		if !en.IsLeader() {
			return
		}
		term := uint64(0)
		if en.RaftCluster != nil {
			term = en.RaftCluster.CurrentTerm()
		} else {
			term = en.PlanDist.CurrentTerm() + 1
		}
		en.PlanDist.SetTerm(term)
		go en.distributePlan(id, dsl, plan, version)
	}
	en.Engine.AfterPromote = func(id string, version uint64) {
		if !en.IsLeader() {
			return
		}
		go en.distributeActivate(id, version)
	}

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

func (en *ExecutionNode) IsLeader() bool {
	if en.RaftCluster != nil {
		return en.RaftCluster.IsLeader()
	}
	return true // single-node mode, always leader
}

func (en *ExecutionNode) CurrentTerm() uint64 {
	if en.RaftCluster != nil {
		return en.RaftCluster.CurrentTerm()
	}
	return en.PlanDist.CurrentTerm()
}

func (en *ExecutionNode) httpCall(endpoint string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: request: %w", err)
	}
	resp, err := en.httpClient.Do(req)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		cb.Failure()
		return nil, fmt.Errorf("http call: status %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: read: %w", err)
	}

	cb.Success()
	return respBody, nil
}

func (en *ExecutionNode) mkProducer(topic string, kc transport.KafkaConfig) transport.MessageProducer {
	if len(kc.Brokers) > 0 {
		p := transport.NewKafkaProducer(topic, kc)
		en.mu.Lock()
		en.producers = append(en.producers, p)
		en.mu.Unlock()
		return p
	}
	if en.ClusterNode != nil {
		return cluster.NewClusterProducer(topic, en.ClusterNode)
	}
	return transport.NewProducer(topic)
}

func (en *ExecutionNode) mkConsumer(topic string, handler transport.MessageHandler, kc transport.KafkaConfig) transport.MessageConsumer {
	if len(kc.Brokers) > 0 {
		return transport.NewKafkaConsumer(topic, handler, kc)
	}
	if en.ClusterNode != nil {
		return cluster.NewClusterConsumer(topic, handler, en.ClusterNode)
	}
	return transport.NewConsumer(topic, handler)
}

func (en *ExecutionNode) distributePlan(id, dsl string, plan []byte, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := en.PlanDist.PublishPlan(ctx, id, version, plan, dsl); err != nil {
		slog.Error("plandist: publish plan error", "id", id, "version", version, "error", err)
		return
	}

	if err := en.PlanDist.WaitForAcks(ctx, id, version, 0, 10*time.Second); err != nil {
		slog.Error("plandist: ack wait error", "id", id, "version", version, "error", err)
	}

	if err := en.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error", "id", id, "version", version, "error", err)
	}
}

func (en *ExecutionNode) distributeActivate(id string, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := en.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error during promote", "id", id, "version", version, "error", err)
	}
}

func (en *ExecutionNode) joinRaftCluster(ctx context.Context) {
	raftAddr := fmt.Sprintf("localhost:%d", en.config.RaftPort)

	for _, seed := range en.config.Seeds {
		seedHTTP := seed
		if !strings.HasPrefix(seedHTTP, "http://") && !strings.HasPrefix(seedHTTP, "https://") {
			seedHTTP = "http://" + seedHTTP
		}
		seedURL := seedHTTP + "/cluster/join"
		body, _ := json.Marshal(map[string]string{
			"node_id":   en.nodeID,
			"raft_addr": raftAddr,
		})

		for i := 0; i < 30; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := en.httpClient.Post(seedURL, "application/json", bytes.NewReader(body))
			if err != nil {
				slog.Warn("raft join: attempt failed", "attempt", i+1, "seed_url", seedURL, "error", err)
				time.Sleep(2 * time.Second)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == 200 {
				slog.Info("raft join: successfully joined cluster", "seed_url", seedURL)
				return
			}
			slog.Warn("raft join: attempt got non-200", "attempt", i+1, "seed_url", seedURL, "status_code", resp.StatusCode)
			time.Sleep(2 * time.Second)
		}
	}
	slog.Error("raft join: failed to join cluster after 30 attempts")
}

func (en *ExecutionNode) RaftLeaderAddr() string {
	if en.RaftCluster == nil {
		return ""
	}
	return en.RaftCluster.LeaderAddr()
}

func (en *ExecutionNode) handleNodeDiscoveryMessage(ctx context.Context, msg []byte) ([]byte, error) {
	var nd NodeDiscoveryMessage
	if err := json.Unmarshal(msg, &nd); err != nil {
		slog.Error("discovery: unmarshal error", "error", err)
		return nil, nil
	}
	if nd.NodeID == en.nodeID {
		return nil, nil
	}
	en.Membership.Heartbeat(nd.NodeID, nd.Address)
	return nil, nil
}

func (en *ExecutionNode) handlePlanMessage(ctx context.Context, msg []byte) ([]byte, error) {
	pm, err := plandist.PlanMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal plan: %w", err)
	}

	// Reject plans from older terms
	if pm.Term < en.PlanDist.CurrentTerm() {
		slog.Warn("plandist: rejected plan from older term", "plan_term", pm.Term, "current_term", en.PlanDist.CurrentTerm())
		return nil, nil
	}

	switch pm.Type {
	case "plan":
		if err := en.Engine.AddVersion(pm.RuleID, pm.DSL, pm.Plan, pm.Version); err != nil {
			return nil, err
		}
		if err := en.PlanDist.SendAck(ctx, pm.RuleID, pm.Version, "ok"); err != nil {
			slog.Error("plandist: ack send error", "error", err)
		}
	case "activate":
		if err := en.Engine.Promote(pm.RuleID, pm.Version); err != nil {
			slog.Error("plandist: activate error", "error", err)
		}
	}
	return nil, nil
}

func (en *ExecutionNode) handleAckMessage(ctx context.Context, msg []byte) ([]byte, error) {
	am, err := plandist.AckMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal ack: %w", err)
	}
	en.PlanDist.RecordAck(*am)
	return nil, nil
}

func (en *ExecutionNode) handlePartitionMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if err := en.Partitions.HandleAssignmentMessage(msg); err != nil {
		slog.Error("partition: handle message error", "error", err)
	}
	return nil, nil
}

func (en *ExecutionNode) callService(svcName, method string, body []byte, timeoutMs uint64) ([]byte, error) {
	observability.RecordExec("svc_call")

	svcTimeout := 10 * time.Second
	if timeoutMs > 0 {
		svcTimeout = time.Duration(timeoutMs) * time.Millisecond
	}
	svcCtx, svcCancel := context.WithTimeout(context.Background(), svcTimeout)
	defer svcCancel()

	cbI, _ := en.circuitBreakers.LoadOrStore(svcName, reliability.NewCircuitBreaker(5, 30*time.Second))
	cb := cbI.(*reliability.CircuitBreaker)

	if !cb.Allow() {
		observability.RecordError("circuit_breaker_open")
		slog.Warn("circuit breaker open for service", "service", svcName)
		return nil, fmt.Errorf("circuit breaker open for service %s", svcName)
	}

	inst, _ := en.Registry.LookupInstance(svcName, method)
	if inst != nil {
		endpoint := fmt.Sprintf("%s://%s:%d", inst.Endpoint.Protocol, inst.Endpoint.Address, inst.Endpoint.Port)
		req, err := http.NewRequestWithContext(svcCtx, "POST", endpoint, bytes.NewReader(body))
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: request: %w", svcName, err)
		}
		resp, err := en.httpClient.Do(req)
		if err != nil {
			cb.Failure()
			en.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
			return nil, fmt.Errorf("service %s: call: %w", svcName, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			cb.Failure()
			en.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
			return nil, fmt.Errorf("service %s: status %d", svcName, resp.StatusCode)
		}

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: read: %w", svcName, err)
		}

		cb.Success()
		return respBody, nil
	}

	if en.serviceResolver != nil {
		endpoint, err := en.serviceResolver.Resolve(0, method)
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: resolve: %w", svcName, err)
		}
		return en.httpCall(endpoint, body, cb)
	}

	slog.Info("service call", "service", svcName, "method", method, "body_bytes", len(body))
	cb.Success()
	return body, nil
}

func (en *ExecutionNode) executePlan(ctx context.Context, plan []byte, body []byte) ([]byte, error) {
	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(plan); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	execID := uuid.New().String()
	now := time.Now().UTC()

	execCtx, cancel := context.WithCancel(ctx)
	en.Execs.Register(execID, cancel, "")

	defer func() {
		en.Execs.Unregister(execID)
		cancel()
	}()

	st := &execstate.State{
		ID:        execID,
		PlanBytes: plan,
		Status:    execstate.StatusCreated,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if en.StateStore != nil {
		en.StateStore.Create(execCtx, st)
	}

	out, err := en.runSteps(execCtx, execID, plan, names, nil, nil, st)
	if en.StateStore != nil {
		if err != nil {
			st.Status = execstate.StatusFailed
			st.Error = err.Error()
			en.StateStore.Save(execCtx, st)
		} else {
			en.StateStore.Delete(execCtx, execID)
		}
	}
	return out, err
}

func (en *ExecutionNode) runSteps(ctx context.Context, execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < 1000; step++ {
		select {
		case <-ctx.Done():
			en.tryCompensate(execID)
			return nil, fmt.Errorf("execution cancelled at step %d: %w", step, ctx.Err())
		default:
		}

		out, err := bridge.ExecuteStep(plan, ctxBytes, respBytes, nil)
		if err != nil {
			en.tryCompensate(execID)
			return nil, fmt.Errorf("step %d: %w", step, err)
		}

		ctxBytes = out.CtxBytes

		switch out.Result {
		case bridge.StepDone:
			observability.RecordExec("completed")
			if en.Saga != nil {
				en.Saga.Clear(execID)
			}
			return out.Output, nil

		case bridge.StepPending:
			observability.RecordExec("svc_pending")
			if en.StateStore != nil {
				st.Status = execstate.StatusWaitingForService
				st.PendingSvc = out.PendingSvc
				st.PendingBody = out.PendingBody
				st.CtxBytes = ctxBytes
				en.StateStore.Save(context.Background(), st)
			}

			rawName, ok := names[out.PendingSvc]
			if !ok {
				rawName = fmt.Sprintf("svc-%d", out.PendingSvc)
			}
			svcName, method, compSvc, compMethod := bridge.ParseCompensation(rawName)

			if en.Saga != nil && compSvc != "" {
				en.Saga.RegisterStep(execID, reliability.SagaStep{
					ServiceName: svcName,
					Method:      method,
					Body:        out.PendingBody,
					CompSvc:     compSvc,
					CompMethod:  compMethod,
				})
			}

			resp, err := en.callService(svcName, method, out.PendingBody, out.TimeoutMs)
			if err != nil {
				en.tryCompensate(execID)
				return nil, fmt.Errorf("service %s: %w", svcName, err)
			}

			if en.StateStore != nil {
				st.Status = execstate.StatusRunning
				st.PendingSvc = 0
				st.PendingBody = nil
				st.CtxBytes = ctxBytes
				en.StateStore.Save(context.Background(), st)
			}
			respBytes = resp

		case bridge.StepContinue:
			respBytes = nil
			if en.StateStore != nil {
				st.Status = execstate.StatusRunning
				st.CtxBytes = ctxBytes
				en.StateStore.Save(context.Background(), st)
			}
		}
	}

	en.tryCompensate(execID)
	return nil, fmt.Errorf("execution exceeded max steps")
}

func (en *ExecutionNode) tryCompensate(execID string) {
	if en.Saga == nil {
		return
	}
	if err := en.Saga.Compensate(execID); err != nil {
		slog.Error("saga: compensation error", "exec_id", execID, "error", err)
	}
}

func (en *ExecutionNode) recoverInFlight(ctx context.Context) {
	if en.StateStore == nil {
		return
	}

	inflight, err := en.StateStore.List(ctx, execstate.StatusRunning, execstate.StatusWaitingForService)
	if err != nil {
		slog.Error("recovery: list error", "error", err)
		return
	}

	for _, st := range inflight {
		go en.recoverExecution(st)
	}
}

func (en *ExecutionNode) recoverExecution(st *execstate.State) {
	slog.Info("recovery: resuming execution", "exec_id", st.ID, "status", st.Status, "rule_id", st.RuleID)

	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(st.PlanBytes); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	var startResp []byte
	if st.Status == execstate.StatusWaitingForService {
		rawName, ok := names[st.PendingSvc]
		if !ok {
			rawName = fmt.Sprintf("svc-%d", st.PendingSvc)
		}
		svcName, method := bridge.ParseServiceMethod(rawName)
		resp, err := en.callService(svcName, method, st.PendingBody, 0)
		if err != nil {
			slog.Warn("recovery: exec retry failed", "exec_id", st.ID, "service", svcName, "error", err)
			st.Status = execstate.StatusFailed
			st.Error = fmt.Sprintf("recovery retry: %v", err)
			en.StateStore.Save(context.Background(), st)
			return
		}
		startResp = resp
		st.Status = execstate.StatusRunning
		st.PendingSvc = 0
		st.PendingBody = nil
		en.StateStore.Save(context.Background(), st)
	}

	out, err := en.runSteps(context.Background(), st.ID, st.PlanBytes, names, st.CtxBytes, startResp, st)
	if err != nil {
		slog.Error("recovery: exec failed", "exec_id", st.ID, "error", err)
		st.Status = execstate.StatusFailed
		st.Error = err.Error()
		en.StateStore.Save(context.Background(), st)
		return
	}

	slog.Info("recovery: exec completed", "exec_id", st.ID, "bytes", len(out))
	en.StateStore.Delete(context.Background(), st.ID)
}

func (en *ExecutionNode) executeAll(ctx context.Context, body []byte) ([][]byte, error) {
	plans := en.Engine.ActivePlanBytes()
	if len(plans) == 0 {
		return nil, nil
	}

	type planResult struct {
		index int
		out   []byte
		err   error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]byte, len(plans))
	ch := make(chan planResult, len(plans))
	sem := make(chan struct{}, 10)

	for i, plan := range plans {
		sem <- struct{}{}
		go func(idx int, p []byte) {
			defer func() { <-sem }()
			out, err := en.executePlan(ctx, p, body)
			ch <- planResult{idx, out, err}
		}(i, plan)
	}

	var firstErr error
	for range plans {
		r := <-ch
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			cancel()
		}
		if r.err == nil {
			results[r.index] = r.out
		}
	}

	return results, firstErr
}

func (en *ExecutionNode) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(ctx context.Context, msg []byte) ([]byte, error) {
		if !en.RateLimiter.Allow("ingress") {
			observability.RecordError("rate_limited")
			en.DLQ.Send(&reliability.DeadLetterEntry{
				ID:    "ratelimited",
				Body:  msg,
				Error: "rate limited",
			})
			return nil, nil
		}

		msgID := make([]byte, 16)
		if _, err := rand.Read(msgID); err != nil {
			return nil, fmt.Errorf("message id generation failed: %w", err)
		}
		msgIDStr := hex.EncodeToString(msgID)

		if en.Dedup.Seen(msgIDStr) {
			observability.RecordExec("dedup_skipped")
			return nil, nil
		}
		en.Dedup.Mark(msgIDStr)

		execCtx, execCancel := context.WithTimeout(ctx, 30*time.Second)
		defer execCancel()

		results, err := en.executeAll(execCtx, msg)
		if err != nil {
			observability.RecordError("exec")
			en.DLQ.Send(&reliability.DeadLetterEntry{
				ID:    "exec-error",
				Body:  msg,
				Error: err.Error(),
			})
			return nil, err
		}
		if len(results) == 0 {
			return nil, nil
		}
		observability.RecordExec("msg")
		return results[0], nil
	}

	if en.ClusterNode != nil {
		if err := en.ClusterNode.Start(); err != nil {
			slog.Error("cluster: start error", "error", err)
		}
		for _, seedAddr := range en.config.Seeds {
			if seedAddr == en.config.GRPCAddr {
				continue
			}
			seedID := fmt.Sprintf("seed-%s", seedAddr)
			if err := en.ClusterNode.AddPeer(seedID, seedAddr); err != nil {
				slog.Error("cluster: connect to seed", "seed_addr", seedAddr, "error", err)
			}
		}
	}

	kafkaCfg := transport.KafkaConfig{
		Brokers:    en.config.KafkaBrokers,
		GroupID:    en.config.KafkaGroupID,
		Acks:       transport.AcksLevelFromString(en.config.KafkaAcks),
		Idempotent: en.config.KafkaIdempotent,
	}
	inputConsumer := en.mkConsumer(en.config.Topic, handler, kafkaCfg)
	membersConsumer := en.mkConsumer(DefaultMembersTopic, en.handleNodeDiscoveryMessage, kafkaCfg)
	planConsumer := en.mkConsumer(plandist.DefaultPlanTopic, en.handlePlanMessage, kafkaCfg)
	ackConsumer := en.mkConsumer(plandist.DefaultAckTopic, en.handleAckMessage, kafkaCfg)
	partConsumer := en.mkConsumer(partition.PartitionTopic, en.handlePartitionMessage, kafkaCfg)
	en.mu.Lock()
	en.consumers = append(en.consumers, inputConsumer, membersConsumer, planConsumer, ackConsumer, partConsumer)
	en.mu.Unlock()
	go inputConsumer.Start(ctx)
	go membersConsumer.Start(ctx)
	go planConsumer.Start(ctx)
	go ackConsumer.Start(ctx)
	go partConsumer.Start(ctx)

	en.PlanDist.Start(ctx)
	en.Membership.StartEviction(ctx, membership.DefaultHeartbeatTimeout)

	en.Rebalancer.SetNotify(func() {
		if !en.IsLeader() {
			return
		}
		assignments := en.Partitions.Rebalance(en.Membership.AliveNodes(), en.PlanDist.CurrentTerm())
		if len(assignments) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := en.Partitions.PublishAssignments(ctx, assignments); err != nil {
				slog.Error("partition: publish assignments error", "error", err)
			}
		}
	})

	if en.RaftCluster != nil {
		if err := en.RaftCluster.Start(); err != nil {
			log.Fatalf("raft: start error: %v", err)
		}
		if en.config.RaftBootstrap {
			if err := en.RaftCluster.BootstrapCluster(); err != nil {
				slog.Warn("raft: bootstrap", "error", err)
			}
		}
		en.RaftCluster.SubscribeLeaderChanges(func(isLeader bool) {
			if isLeader {
				term := en.RaftCluster.CurrentTerm()
				en.PlanDist.SetTerm(term)
				en.Partitions.OnLeaderChange(en.nodeID)
				en.Rebalancer.CheckAndRebalance()
				slog.Info("raft: node became leader", "node_id", en.nodeID, "term", term)
			} else {
				leaderAddr := en.RaftCluster.LeaderAddr()
				slog.Info("raft: node stepped down", "node_id", en.nodeID, "leader_addr", leaderAddr)
				en.Partitions.OnLeaderChange("")
			}
		})
		if !en.config.RaftBootstrap && len(en.config.Seeds) > 0 {
			go en.joinRaftCluster(ctx)
		}
	}

	en.Scheduler.Start(ctx)
	en.ReplyRouter.StartCleanup()
	en.Dedup.StartCleanup(ctx, 30*time.Second)

	en.recoverInFlight(ctx)

	if en.GRPCBus != nil {
		if err := en.GRPCBus.Start(); err != nil {
			slog.Error("grpc: start error", "error", err)
		}
	}

	if en.OtelExporter != nil {
		go en.OtelExporter.Start(ctx)
	}

	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", en.AdminSrv.Handler()))
	mux.HandleFunc("/register", en.Registry.RegisterHTTPHandler)
	mux.HandleFunc("/heartbeat", en.Registry.HeartbeatHTTPHandler)
	mux.HandleFunc("/services", en.Registry.ListServicesHTTPHandler)
	mux.HandleFunc("/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		if en.RaftCluster == nil {
			http.Error(w, "raft not configured", http.StatusBadRequest)
			return
		}
		var req struct {
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := en.RaftCluster.Join(req.NodeID, req.RaftAddr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"status":    "ok",
			"node_id":   en.nodeID,
			"is_leader": en.IsLeader(),
			"term":      en.CurrentTerm(),
		}
		json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if en.IsLeader() && en.PlanDist.CurrentTerm() == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "reason": "leader not initialized"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := en.Metrics.Snapshot()
		snap.Gauges["pending_requests"] = int64(en.ReplyRouter.PendingCount())
		snap.Gauges["dlq_size"] = int64(en.DLQ.Len())
		snap.Gauges["inflight_execs"] = int64(en.Execs.Len())
		json.NewEncoder(w).Encode(snap)
	})
	mux.HandleFunc("DELETE /executions/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if en.Execs.Cancel(id) {
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{"status": "cancelling", "id": id})
		} else {
			http.Error(w, "execution not found", http.StatusNotFound)
		}
	})
	mux.HandleFunc("GET /executions", func(w http.ResponseWriter, r *http.Request) {
		execs := en.Execs.List()
		json.NewEncoder(w).Encode(execs)
	})
	mux.HandleFunc("GET /partitions", func(w http.ResponseWriter, r *http.Request) {
		assignments := en.Partitions.Assignments()
		nodeParts := make(map[string][]uint32)
		for _, n := range en.Membership.AliveNodes() {
			nodeParts[n] = en.Partitions.PartitionsForNode(n)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"num_partitions": en.Partitions.NumPartitions(),
			"assignments":    assignments,
			"node_partitions": nodeParts,
		})
	})
	mux.HandleFunc("POST /partitions/rebalance", func(w http.ResponseWriter, r *http.Request) {
		if !en.IsLeader() {
			http.Error(w, "not leader", http.StatusForbidden)
			return
		}
		assignments := en.Partitions.Rebalance(en.Membership.AliveNodes(), en.PlanDist.CurrentTerm())
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := en.Partitions.PublishAssignments(ctx, assignments); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":       "rebalanced",
			"assignments":  len(assignments),
		})
	})

	en.HTTP = &http.Server{Addr: en.httpAddr, Handler: mux}
	go func() {
		slog.Info("HTTP server started", "addr", en.httpAddr)
		if err := en.HTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	slog.Info("execnode started", "node_id", en.nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("shutting down")
	en.Shutdown()
}

func (en *ExecutionNode) Shutdown() {
	en.mu.Lock()
	defer en.mu.Unlock()

	slog.Info("shutdown: cancelling in-flight executions", "count", en.Execs.Len())
	en.Execs.CancelAll()

	for _, c := range en.consumers {
		c.Stop()
	}
	en.consumers = nil

	en.PlanDist.Stop()
	en.Scheduler.Stop()
	en.ReplyRouter.StopCleanup()

	for _, p := range en.producers {
		p.Close()
	}
	en.producers = nil

	if en.HTTP != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := en.HTTP.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown error", "error", err)
		}
	}

	if en.ClusterNode != nil {
		en.ClusterNode.Stop()
	}

	if en.GRPCBus != nil {
		en.GRPCBus.Stop()
	}

	if en.OtelExporter != nil {
		en.OtelExporter.Stop()
	}

	if en.RaftCluster != nil {
		en.RaftCluster.Stop()
	}

	if en.StateStore != nil {
		en.StateStore.Close()
	}

	close(en.shutdownCh)
	slog.Info("execnode: shutdown complete", "node_id", en.nodeID)
}


