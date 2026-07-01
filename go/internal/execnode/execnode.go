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
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
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

type HeartbeatMessage struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Timestamp time.Time `json:"timestamp"`
	Term      uint64    `json:"term"`
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

	serviceResolver ServiceResolver

	circuitBreakers sync.Map  // svcName string → *reliability.CircuitBreaker

	Saga *reliability.SagaTracker

	consumers   []transport.MessageConsumer
	producers   []transport.MessageProducer
	httpAddr    string
	nodeID      string
	config      Config
	httpClient  *http.Client
	mu          sync.Mutex
	shutdownCh  chan struct{}
	stopHb      chan struct{}
	isLeader    int32  // atomic: 0 = follower, 1 = leader
	clusterTerm uint64 // atomic: managed via LoadUint64/StoreUint64

	membersProducer transport.MessageProducer

	StateStore *execstate.FileStore
	Execs      *ExecRegistry
	TermStore  *TermStore

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
		stopHb:     make(chan struct{}),
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
	en.membersProducer = en.mkProducer(DefaultMembersTopic, kafkaCfg)
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
		en.SetTerm(en.PlanDist.CurrentTerm() + 1)
		go en.distributePlan(id, dsl, plan, version)
	}
	en.Engine.AfterPromote = func(id string, version uint64) {
		if !en.IsLeader() {
			return
		}
		go en.distributeActivate(id, version)
	}

	en.mu.Lock()
	en.producers = append(en.producers, dlqProducer, planProducer, ackProducer, en.membersProducer)
	en.mu.Unlock()

	execDir := cfg.ExecStateDir
	if execDir == "" {
		execDir = filepath.Join(os.TempDir(), "flowrulz-execstate")
	}
	store, err := execstate.NewFileStore(execDir)
	if err != nil {
		log.Printf("execstate: init warning: %v", err)
	}
	en.StateStore = store
	en.Execs = NewExecRegistry()
	en.TermStore = NewTermStore(execDir)
	if term, _ := en.TermStore.Load(); term > 0 {
		en.SetTerm(term)
		log.Printf("term: restored term %d from disk", term)
	}

	en.Saga = reliability.NewSagaTrackerWithDir(func(svc, method string, body []byte) error {
		_, err := en.callService(svc, method, body, 0)
		return err
	}, execDir)

	en.Registry.SetHeartbeatTimeout(30 * time.Second)

	if cfg.GRPCAddr != "" {
		en.GRPCBus = grpctransport.NewGRPCBus(cfg.GRPCAddr)
	}

	if ep := os.Getenv("FLOWRULZ_OTEL_ENDPOINT"); ep != "" {
		en.OtelExporter = observability.NewSpanExporter(ep)
	}

	if cfg.PluginDir != "" {
		if err := plugins.LoadDir(cfg.PluginDir); err != nil {
			log.Printf("[execnode] plugin load warning: %v", err)
		}
	} else if pd := os.Getenv("FLOWRULZ_PLUGIN_DIR"); pd != "" {
		if err := plugins.LoadDir(pd); err != nil {
			log.Printf("[execnode] plugin load warning: %v", err)
		}
	}

	return en
}

func (en *ExecutionNode) SetLeader(v bool) {
	var val int32 = 0
	if v {
		val = 1
	}
	atomic.StoreInt32(&en.isLeader, val)
}

func (en *ExecutionNode) IsLeader() bool {
	return atomic.LoadInt32(&en.isLeader) != 0
}

func (en *ExecutionNode) SetTerm(term uint64) {
	atomic.StoreUint64(&en.clusterTerm, term)
	en.PlanDist.SetTerm(term)
}

func (en *ExecutionNode) CurrentTerm() uint64 {
	return atomic.LoadUint64(&en.clusterTerm)
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
		log.Printf("plandist: publish plan error for %s v%d: %v", id, version, err)
		return
	}

	if err := en.PlanDist.WaitForAcks(ctx, id, version, 0, 10*time.Second); err != nil {
		log.Printf("plandist: ack wait error for %s v%d: %v", id, version, err)
	}

	if err := en.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		log.Printf("plandist: activate error for %s v%d: %v", id, version, err)
	}
}

func (en *ExecutionNode) distributeActivate(id string, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := en.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		log.Printf("plandist: activate error during promote %s v%d: %v", id, version, err)
	}
}

func (en *ExecutionNode) startHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(membership.DefaultHeartbeatInterval)
	defer ticker.Stop()

	log.Printf("heartbeat: starting for node %s every %v", en.nodeID, membership.DefaultHeartbeatInterval)

	for {
		select {
		case <-ticker.C:
			hb := HeartbeatMessage{
				NodeID:    en.nodeID,
				Address:   en.httpAddr,
				Timestamp: time.Now(),
				Term:      atomic.LoadUint64(&en.clusterTerm),
			}
			data, err := json.Marshal(hb)
			if err != nil {
				log.Printf("heartbeat: marshal error: %v", err)
				continue
			}
			if err := en.membersProducer.Send(ctx, []byte(en.nodeID), data); err != nil {
				log.Printf("heartbeat: publish error: %v", err)
			}
		case <-ctx.Done():
			log.Printf("heartbeat: stopped")
			return
		}
	}
}

func (en *ExecutionNode) handleMembershipMessage(ctx context.Context, msg []byte) ([]byte, error) {
	var hb HeartbeatMessage
	if err := json.Unmarshal(msg, &hb); err != nil {
		log.Printf("membership: unmarshal heartbeat error: %v", err)
		return nil, nil
	}

	if hb.NodeID == en.nodeID {
		return nil, nil
	}

	// Term-based fencing: if a non-leader heartbeat carries a higher term,
	// a new leader has been elected — step down if we're the current leader.
	if hb.Term > en.PlanDist.CurrentTerm() {
		if en.IsLeader() {
			log.Printf("fencing: stepping down from term %d due to higher term %d from %s",
				en.PlanDist.CurrentTerm(), hb.Term, hb.NodeID)
			en.SetLeader(false)
		}
		en.SetTerm(hb.Term)
	}

	en.Membership.Heartbeat(hb.NodeID, hb.Address)
	en.runLeaderElection()
	return nil, nil
}

func (en *ExecutionNode) runLeaderElection() {
	leaderID := en.Membership.LeaderID()
	if leaderID == "" {
		return
	}

	shouldBeLeader := leaderID == en.nodeID
	isCurrentlyLeader := en.IsLeader()

	if shouldBeLeader && !isCurrentlyLeader {
		en.SetLeader(true)
		en.SetTerm(en.PlanDist.CurrentTerm() + 1)
		if en.TermStore != nil {
			en.TermStore.Save(en.PlanDist.CurrentTerm(), en.nodeID)
		}
		log.Printf("leader election: node %s promoted to leader (term %d)", en.nodeID, en.PlanDist.CurrentTerm())
		en.Partitions.OnLeaderChange(en.nodeID)
		en.Rebalancer.CheckAndRebalance()
	} else if !shouldBeLeader && isCurrentlyLeader {
		en.SetLeader(false)
		if en.TermStore != nil {
			en.TermStore.Save(en.PlanDist.CurrentTerm(), leaderID)
		}
		log.Printf("leader election: node %s stepped down (new leader: %s)", en.nodeID, leaderID)
		en.Partitions.OnLeaderChange(leaderID)
	}

	// Rebalance check on every election run (even if no change) to catch membership changes
	en.Rebalancer.CheckAndRebalance()
}

func (en *ExecutionNode) handlePlanMessage(ctx context.Context, msg []byte) ([]byte, error) {
	pm, err := plandist.PlanMessageFromBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("plandist: unmarshal plan: %w", err)
	}

	// Reject plans from older terms
	if pm.Term < en.PlanDist.CurrentTerm() {
		log.Printf("plandist: rejected plan from term %d (current %d)", pm.Term, en.PlanDist.CurrentTerm())
		return nil, nil
	}

	switch pm.Type {
	case "plan":
		if err := en.Engine.AddVersion(pm.RuleID, pm.DSL, pm.Plan, pm.Version); err != nil {
			return nil, err
		}
		if err := en.PlanDist.SendAck(ctx, pm.RuleID, pm.Version, "ok"); err != nil {
			log.Printf("plandist: ack send error: %v", err)
		}
	case "activate":
		if err := en.Engine.Promote(pm.RuleID, pm.Version); err != nil {
			log.Printf("plandist: activate error: %v", err)
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
		log.Printf("partition: handle message error: %v", err)
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
		log.Printf("circuit breaker open for service %s", svcName)
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

	log.Printf("service call: name=%s method=%s body=%d bytes (stub)", svcName, method, len(body))
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
		log.Printf("saga: compensation error for %s: %v", execID, err)
	}
}

func (en *ExecutionNode) recoverInFlight(ctx context.Context) {
	if en.StateStore == nil {
		return
	}

	inflight, err := en.StateStore.List(ctx, execstate.StatusRunning, execstate.StatusWaitingForService)
	if err != nil {
		log.Printf("recovery: list error: %v", err)
		return
	}

	for _, st := range inflight {
		go en.recoverExecution(st)
	}
}

func (en *ExecutionNode) recoverExecution(st *execstate.State) {
	log.Printf("recovery: resuming execution %s (status=%s, rule=%s)", st.ID, st.Status, st.RuleID)

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
			log.Printf("recovery: exec %s service %s retry failed: %v", st.ID, svcName, err)
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
		log.Printf("recovery: exec %s failed: %v", st.ID, err)
		st.Status = execstate.StatusFailed
		st.Error = err.Error()
		en.StateStore.Save(context.Background(), st)
		return
	}

	log.Printf("recovery: exec %s completed (%d bytes)", st.ID, len(out))
	en.StateStore.Delete(context.Background(), st.ID)
}

func (en *ExecutionNode) executeAll(ctx context.Context, body []byte) ([][]byte, error) {
	plans := en.Engine.ActivePlanBytes()
	if len(plans) == 0 {
		return nil, nil
	}

	var results [][]byte
	for _, plan := range plans {
		res, err := en.executePlan(ctx, plan, body)
		if err != nil {
			return results, err
		}
		results = append(results, res)
	}
	return results, nil
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
			log.Printf("cluster: start error: %v", err)
		}
		for _, seedAddr := range en.config.Seeds {
			if seedAddr == en.config.GRPCAddr {
				continue
			}
			seedID := fmt.Sprintf("seed-%s", seedAddr)
			if err := en.ClusterNode.AddPeer(seedID, seedAddr); err != nil {
				log.Printf("cluster: connect to seed %s: %v", seedAddr, err)
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
	membersConsumer := en.mkConsumer(DefaultMembersTopic, en.handleMembershipMessage, kafkaCfg)
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
	en.Membership.StartLeaderLeaseChecker(ctx, membership.DefaultHeartbeatInterval)
	en.Membership.OnLeaseExpiry(func(leaderID string) {
		log.Printf("lease: leader %s lost, running re-election", leaderID)
		en.runLeaderElection()
	})

	en.Rebalancer.SetNotify(func() {
		if !en.IsLeader() {
			return
		}
		assignments := en.Partitions.Rebalance(en.Membership.AliveNodes(), en.PlanDist.CurrentTerm())
		if len(assignments) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := en.Partitions.PublishAssignments(ctx, assignments); err != nil {
				log.Printf("partition: publish assignments error: %v", err)
			}
		}
	})
	go en.startHeartbeat(ctx)

	en.Scheduler.Start(ctx)
	en.ReplyRouter.StartCleanup()
	en.Dedup.StartCleanup(ctx, 30*time.Second)

	en.Registry.StartHeartbeatChecker(en.stopHb)

	en.recoverInFlight(ctx)

	if en.GRPCBus != nil {
		if err := en.GRPCBus.Start(); err != nil {
			log.Printf("grpc: start error: %v", err)
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
		log.Printf("HTTP server on %s", en.httpAddr)
		if err := en.HTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	log.Printf("execnode %s: started", en.nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down")
	en.Shutdown()
}

func (en *ExecutionNode) Shutdown() {
	en.mu.Lock()
	defer en.mu.Unlock()

	log.Printf("shutdown: cancelling %d in-flight executions", en.Execs.Len())
	en.Execs.CancelAll()

	for _, c := range en.consumers {
		c.Stop()
	}
	en.consumers = nil

	close(en.stopHb)
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
			log.Printf("http shutdown error: %v", err)
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

	if en.StateStore != nil {
		en.StateStore.Close()
	}

	close(en.shutdownCh)
	log.Printf("execnode %s: shutdown complete", en.nodeID)
}


