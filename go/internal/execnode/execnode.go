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

	"github.com/premchandkpc/FlowRulZ/go/internal/admin"
	"github.com/premchandkpc/FlowRulZ/go/bridge"
	"github.com/premchandkpc/FlowRulZ/go/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/go/internal/engine"
	"github.com/premchandkpc/FlowRulZ/go/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/go/internal/membership"
	"github.com/premchandkpc/FlowRulZ/go/internal/observability"
	"github.com/premchandkpc/FlowRulZ/go/internal/plandist"
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

	GRPCBus *grpctransport.GRPCBus
}

type Config struct {
	PersistPath   string
	ExecStateDir  string
	HTTPAddr      string
	GRPCAddr      string
	CompilerAddr  string
	Topic         string
	NodeID        string
	Seeds         []string
	KafkaBrokers  []string
	KafkaGroupID  string
	APIKey        string
}

func NewConfig() *Config {
	return &Config{
		HTTPAddr:      ":8080",
		Topic:         "flowrulz-input",
		NodeID:        "node-1",
		KafkaBrokers:  []string{"localhost:9092"},
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

	kafkaCfg := transport.KafkaConfig{
		Brokers: cfg.KafkaBrokers,
		GroupID: cfg.KafkaGroupID,
	}

	dlqProducer := en.mkProducer(reliability.DefaultDLQTopic, kafkaCfg)
	en.DLQ = reliability.NewDLQ(10000, reliability.WithDLQProducer(dlqProducer))

	planProducer := en.mkProducer(plandist.DefaultPlanTopic, kafkaCfg)
	ackProducer := en.mkProducer(plandist.DefaultAckTopic, kafkaCfg)
	en.membersProducer = en.mkProducer(DefaultMembersTopic, kafkaCfg)
	en.Membership = membership.New()
	en.PlanDist = plandist.New(nodeID,
		plandist.WithPlanProducer(planProducer),
		plandist.WithAckProducer(ackProducer),
		plandist.WithQuorumProvider(en.Membership),
	)

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

	en.Saga = reliability.NewSagaTracker(func(svc, method string, body []byte) error {
		_, err := en.callService(svc, method, body)
		return err
	})

	en.Registry.SetHeartbeatTimeout(30 * time.Second)

	if cfg.GRPCAddr != "" {
		en.GRPCBus = grpctransport.NewGRPCBus(cfg.GRPCAddr)
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

func (en *ExecutionNode) JoinCluster(id, address string) {
	en.Membership.Add(id, address)
}

func (en *ExecutionNode) LeaveCluster(id string) {
	en.Membership.Remove(id)
}

func (en *ExecutionNode) AliveCount() int {
	return en.Membership.AliveCount()
}

func (en *ExecutionNode) CurrentTerm() uint64 {
	return atomic.LoadUint64(&en.clusterTerm)
}

func (en *ExecutionNode) httpCall(endpoint string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	resp, err := en.httpClient.Post(endpoint, "application/octet-stream", bytes.NewReader(body))
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
	return transport.NewProducer(topic)
}

func (en *ExecutionNode) mkConsumer(topic string, handler transport.MessageHandler, kc transport.KafkaConfig) transport.MessageConsumer {
	if len(kc.Brokers) > 0 {
		return transport.NewKafkaConsumer(topic, handler, kc)
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
		log.Printf("leader election: node %s promoted to leader (term %d)", en.nodeID, en.PlanDist.CurrentTerm())
	} else if !shouldBeLeader && isCurrentlyLeader {
		en.SetLeader(false)
		log.Printf("leader election: node %s stepped down (new leader: %s)", en.nodeID, leaderID)
	}
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

func (en *ExecutionNode) callService(svcName, method string, body []byte) ([]byte, error) {
	observability.RecordExec("svc_call")

	v, _ := en.circuitBreakers.Load(svcName)
	cb, ok := v.(*reliability.CircuitBreaker)
	if !ok {
		cb = reliability.NewCircuitBreaker(5, 30*time.Second)
		en.circuitBreakers.Store(svcName, cb)
	}

	if !cb.Allow() {
		observability.RecordError("circuit_breaker_open")
		log.Printf("circuit breaker open for service %s", svcName)
		return nil, fmt.Errorf("circuit breaker open for service %s", svcName)
	}

	inst, _ := en.Registry.LookupInstance(svcName, method)
	if inst != nil {
		endpoint := fmt.Sprintf("%s://%s:%d", inst.Endpoint.Protocol, inst.Endpoint.Address, inst.Endpoint.Port)
		resp, err := en.httpClient.Post(endpoint, "application/octet-stream", bytes.NewReader(body))
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

func (en *ExecutionNode) executePlan(plan []byte, body []byte) ([]byte, error) {
	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(plan); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	execID := uuid.New().String()
	now := time.Now().UTC()

	st := &execstate.State{
		ID:        execID,
		PlanBytes: plan,
		Status:    execstate.StatusCreated,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if en.StateStore != nil {
		en.StateStore.Create(context.Background(), st)
	}

	out, err := en.runSteps(execID, plan, names, nil, nil, st)
	if en.StateStore != nil {
		if err != nil {
			st.Status = execstate.StatusFailed
			st.Error = err.Error()
			en.StateStore.Save(context.Background(), st)
		} else {
			en.StateStore.Delete(context.Background(), execID)
		}
	}
	return out, err
}

func (en *ExecutionNode) runSteps(execID string, plan []byte, names map[uint16]string, startCtx, startResp []byte, st *execstate.State) ([]byte, error) {
	ctxBytes, respBytes := startCtx, startResp

	for step := 0; step < 1000; step++ {
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

			resp, err := en.callService(svcName, method, out.PendingBody)
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
		resp, err := en.callService(svcName, method, st.PendingBody)
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

	out, err := en.runSteps(st.ID, st.PlanBytes, names, st.CtxBytes, startResp, st)
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

func (en *ExecutionNode) executeAll(body []byte) ([][]byte, error) {
	plans := en.Engine.ActivePlanBytes()
	if len(plans) == 0 {
		return nil, nil
	}

	var results [][]byte
	for _, plan := range plans {
		res, err := en.executePlan(plan, body)
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

		results, err := en.executeAll(msg)
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

	kafkaCfg := transport.KafkaConfig{
		Brokers: en.config.KafkaBrokers,
		GroupID: en.config.KafkaGroupID,
	}
	inputConsumer := en.mkConsumer(en.config.Topic, handler, kafkaCfg)
	membersConsumer := en.mkConsumer(DefaultMembersTopic, en.handleMembershipMessage, kafkaCfg)
	planConsumer := en.mkConsumer(plandist.DefaultPlanTopic, en.handlePlanMessage, kafkaCfg)
	ackConsumer := en.mkConsumer(plandist.DefaultAckTopic, en.handleAckMessage, kafkaCfg)
	en.mu.Lock()
	en.consumers = append(en.consumers, inputConsumer, membersConsumer, planConsumer, ackConsumer)
	en.mu.Unlock()
	go inputConsumer.Start(ctx)
	go membersConsumer.Start(ctx)
	go planConsumer.Start(ctx)
	go ackConsumer.Start(ctx)

	en.PlanDist.Start(ctx)
	en.Membership.StartEviction(ctx, membership.DefaultHeartbeatTimeout)
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

	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", en.AdminSrv.Handler()))
	mux.HandleFunc("/register", en.Registry.RegisterHTTPHandler)
	mux.HandleFunc("/heartbeat", en.Registry.HeartbeatHTTPHandler)
	mux.HandleFunc("/services", en.Registry.ListServicesHTTPHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := en.Metrics.Snapshot()
		snap.Gauges["pending_requests"] = int64(en.ReplyRouter.PendingCount())
		snap.Gauges["dlq_size"] = int64(en.DLQ.Len())
		json.NewEncoder(w).Encode(snap)
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

	if en.GRPCBus != nil {
		en.GRPCBus.Stop()
	}

	if en.StateStore != nil {
		en.StateStore.Close()
	}

	close(en.shutdownCh)
	log.Printf("execnode %s: shutdown complete", en.nodeID)
}


