# FlowRulZ Go Architecture — SOLID Restructure

> **Status:** Phase 1 complete. Phase 2 (interface extraction) complete — 13 packages in `go/pkg/`. Phase 3a (adapter layer) complete — 6 `pkgsupport.go` files implementing `pkg/` interfaces from `internal/` types, all with compile-time assertions. Phase 3b (ProdNode DI wiring) partial — `Dependencies` struct + `NewNode(cfg, deps)` + `DefaultDependencies()` created, 5 fields migrated to `pkg/` interfaces, 8+ fields remain concrete. Next: migrate `execnode/execnode.go` and `admin/api.go` to interfaces, create `pkg/transport` interfaces.

## Package Dependency Hierarchy (top→bottom)

```
cmd/              — wiring, config parsing, main()
  └─ internal/    — implementations (one pkg per concern)
       └─ pkg/    — interfaces + domain types (no impls)
```

**Golden rule:** `pkg/` never imports `internal/`. `internal/` imports `pkg/`. Higher layers import lower ones. No cycles.

---

## Layer 1: `go/pkg/` — Interfaces + Domain Types

Zero imports from `go/internal/`. Zero creation logic. Only interfaces, enums, pure data types.

### 1.1 `go/pkg/node/` — Node Interface

```go
package node

import "context"

type ID string

type Node interface {
    // Identity
    ID() ID
    Addr() string

    // Lifecycle
    Start(ctx context.Context) error
    Shutdown(ctx context.Context) error

    // Execution
    Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error)

    // Leadership
    IsLeader() bool
    CurrentTerm() uint64
    LeaderID() ID

    // Health
    Ready(ctx context.Context) error
}

type ExecuteRequest struct {
    RuleID string
    Body   []byte
    Timeout time.Duration
    Metadata map[string]string
}

type ExecuteResponse struct {
    Body     []byte
    Duration time.Duration
    RuleID   string
    Error    string
}
```

### 1.2 `go/pkg/transport/` — Segregated Transport Interfaces

```go
package transport

import "context"

// --- Messages ---

type Message struct {
    ID            string
    Type          MessageType
    Topic         string
    Body          []byte
    Headers       map[string]string
    CorrelationID string
    ReplyTo       string
    PartitionKey  string
    CreatedAt     time.Time
}

type MessageType int32
type MessageHandler func(ctx context.Context, msg *Message)

type Subscription struct {
    ID    string
    Topic string
}

// --- Role interfaces (ISP: each consumer picks what it needs) ---

type Publisher interface {
    Publish(ctx context.Context, topic string, msg *Message) error
    PublishToPartition(ctx context.Context, topic, key string, msg *Message) error
}

type Subscriber interface {
    Subscribe(ctx context.Context, topic string, handler MessageHandler) (*Subscription, error)
    Unsubscribe(ctx context.Context, sub *Subscription) error
}

type Requester interface {
    Request(ctx context.Context, topic string, msg *Message, timeout time.Duration) (*Message, error)
}

type Replier interface {
    Reply(ctx context.Context, correlationID string, msg *Message) error
}

type Broadcaster interface {
    Broadcast(ctx context.Context, topic string, msg *Message) error
}

// --- Consumer/Producer (low-level, for adapter implementors) ---

type MessageProducer interface {
    Send(ctx context.Context, key []byte, msg []byte) error
    Close() error
}

type MessageConsumer interface {
    Topic() string
    Start(ctx context.Context) error
    Stop() error
}

// --- Full bus (only for bus implementors) ---
type FullEventBus interface {
    Publisher
    Subscriber
    Requester
    Replier
    Broadcaster
    Close() error
    TopicStats() map[string]int
}
```

### 1.3 `go/pkg/cluster/` — Cluster Membership & Consensus

```go
package cluster

type MemberID string
type ClusterState int

const (
    Follower ClusterState = iota
    Candidate
    Leader
)

type MemberInfo struct {
    ID      MemberID
    Address string
    RaftAddr string
    IsLeader bool
    IsAlive  bool
    LastSeen time.Time
}

// --- ClusterMember (consensus participant) ---

type ClusterMember interface {
    ID() MemberID
    Addr() string

    // Lifecycle
    Start(ctx context.Context) error
    Stop(ctx context.Context) error

    // Leadership
    State() ClusterState
    IsLeader() bool
    CurrentTerm() uint64
    LeaderID() MemberID
    LeaderAddr() string

    // Observations
    SubscribeLeaderChanges(fn func(isLeader bool)) CancelFunc
    SubscribeTermChanges(fn func(term uint64)) CancelFunc

    // Cluster manipulation
    Join(memberID MemberID, addr string) error
    Remove(memberID MemberID) error

    // Bootstrap
    BootstrapCluster() error
}

type CancelFunc func()

// --- Gossiper (membership propagation) ---

type Gossiper interface {
    Start(ctx context.Context) error
    Stop() error

    OnNodeJoin(fn func(nodeID, addr string)) CancelFunc
    OnNodeLeave(fn func(nodeID string)) CancelFunc

    Publish(topic string, key string, data []byte) error
    AddPeer(id, addr string) error
    RemovePeer(id string) error
}
```

### 1.4 `go/pkg/scheduler/` — Task Scheduler

```go
package scheduler

import "context"

type Lane int

const (
    LaneFast   Lane = iota // 50 workers, 5k queue
    LaneNormal             // 20 workers, 2k queue
    LaneHeavy              // 5 workers, 500 queue
)

type ExecutionID string

type ExecutionContext struct {
    ID              ExecutionID
    Plan            *Plan
    State           State
    Variables       map[string]any
    IncomingBody    []byte
    Output          []byte
    Duration        time.Duration
    Lane            Lane
    ResultCh        chan *Result
    CreatedAt       time.Time

    // Service tracking
    WaitingService  string
    WaitingStartTime time.Time
}

type State int
const (
    StateCreated State = iota
    StateReady
    StateRunning
    StateWaitingForService
    StateCompleted
    StateFailed
    StateCancelled
)

type Plan struct {
    ID           string
    Instructions []Instruction
    PlanBytes    []byte       // raw compiled plan (for bridge execution)
    ServiceNames map[uint16]string
    // ...
}

type Instruction struct {
    Op      OpCode
    Service string
    Args    []string
}

type OpCode int
const (
    OpCallService OpCode = iota
    OpValidate
    OpBranch
    OpPublish
    OpReturn
)

type Result struct {
    Body  []byte
    Error error
}

// --- Scheduler interface ---

type Scheduler interface {
    // Identity
    ID() string

    // Lifecycle
    Start(ctx context.Context) error
    Stop() error

    // Execution
    Enqueue(ctx *ExecutionContext) error

    // Introspection
    Snapshot() SchedulerSnapshot
    ExecCount() int64
}

type SchedulerSnapshot struct {
    ReadyQueueLen   int
    WaitingQueueLen int
    ActiveWorkers   int
    LaneCounts      map[Lane]int
}
```

### 1.5 `go/pkg/store/` — Execution State Persistence

```go
package store

import "context"

type ExecutionID string

type ExecutionRecord struct {
    ID         ExecutionID
    PlanID     string
    State      string
    Body       []byte
    Output     []byte
    Error      string
    CreatedAt  time.Time
    CompletedAt time.Time
    NodeID     string
}

type Store interface {
    Create(ctx context.Context, record *ExecutionRecord) error
    Save(ctx context.Context, record *ExecutionRecord) error
    Load(ctx context.Context, id ExecutionID) (*ExecutionRecord, error)
    List(ctx context.Context) ([]*ExecutionRecord, error)
    ListByPlan(ctx context.Context, planID string) ([]*ExecutionRecord, error)
    Delete(ctx context.Context, id ExecutionID) error
    Close() error
}
```

### 1.6 `go/pkg/vm/` — Plan Compilation & Execution (decouples CGo)

```go
package vm

import "context"

type CompileResult struct {
    PlanBytes    []byte
    Instructions int
    Services     []string
    Complexity   int
    Version      uint64
}

type StepResult struct {
    CtxBytes   []byte
    Output     []byte
    Error      string
    Result     StepCode
    PendingSvc uint16
    PendingBody []byte
}

type StepCode int
const (
    StepDone    StepCode = iota
    StepPending
    StepContinue
    StepFailed
)

// --- PlanCompiler (DSL → bytes) ---

type PlanCompiler interface {
    Compile(ctx context.Context, dsl string, ruleID string) (*CompileResult, error)
    CompileAndCache(ctx context.Context, dsl string, ruleID string) (*CompileResult, error)
    InvalidateCache(ruleID string)
}

// --- VMRunner (execute compiled plan bytes) ---

type VMRunner interface {
    InitContext(ctx context.Context, body []byte) ([]byte, error)
    ExecuteStep(ctx context.Context, plan []byte, ctxBytes, respBytes []byte, opts *StepOptions) (*StepResult, error)
    ParseServiceMethod(raw string) (service string, method string)
}

type StepOptions struct {
    MaxSteps     int
    Timeout      time.Duration
    ServiceCallback func(svcID uint16, body []byte) ([]byte, error)
}
```

### 1.7 `go/pkg/registry/` — Service Discovery

```go
package registry

import "context"

type ServiceID string
type LBStrategy int

const (
    RoundRobin LBStrategy = iota
    LeastConnections
    Random
)

type ServiceRegistration struct {
    Name      string
    Version   string
    Address   string
    Methods   []MethodSpec
    Metadata  map[string]string
    Tags      []string
}

type MethodSpec struct {
    Name   string
    Input  string
    Output string
}

type ServiceInstance struct {
    ID        ServiceID
    Name      string
    Address   string
    IsAlive   bool
    LastSeen  time.Time
    Meta      map[string]string
}

type Registry interface {
    Register(ctx context.Context, svc *ServiceRegistration) error
    Unregister(ctx context.Context, name string) error

    Lookup(ctx context.Context, name string) (*ServiceInstance, error)
    LookupMultiple(ctx context.Context, names []string) ([]*ServiceInstance, error)
    ListServices(ctx context.Context) ([]*ServiceRegistration, error)

    HealthCheck(ctx context.Context, name string) (bool, error)
    SubscribeChanges(ctx context.Context, pattern string) (<-chan RegistryEvent, error)
}

type RegistryEvent struct {
    Type  EventType
    Name  string
    Error error
}

type EventType int
const (
    EventRegistered EventType = iota
    EventUnregistered
    EventHealthChanged
)
```

### 1.8 `go/pkg/plandist/` — Plan Distribution

```go
package plandist

import (
    "context"
    "time"
)

type PlanMessage struct {
    Type    string
    RuleID  string
    Version uint64
    Term    uint64
    Plan    []byte
    DSL     string
    NodeID  string
}

type AckMessage struct {
    NodeID  string
    RuleID  string
    Version uint64
    Status  string
}

type PlanDistributor interface {
    Start(ctx context.Context) error
    Stop() error

    PublishPlan(ctx context.Context, ruleID string, version uint64, plan []byte, dsl string) error
    ActivatePlan(ctx context.Context, ruleID string, version uint64) error
    DeactivatePlan(ctx context.Context, ruleID string) error

    SendAck(ctx context.Context, ruleID string, version uint64, status string) error
    WaitForAcks(ctx context.Context, ruleID string, version uint64, quorum int, timeout time.Duration) error

    SetTerm(term uint64)
    CurrentTerm() uint64

    OnPlan(fn func(ctx context.Context, msg PlanMessage) error)
    OnAck(fn func(ctx context.Context, msg AckMessage))
}

// --- QuorumProvider (for PlanDistributor to know cluster size) ---

type QuorumProvider interface {
    AliveCount() int
}
```

### 1.9 `go/pkg/membership/` — Node Membership Tracking

```go
package membership

import (
    "context"
    "time"
)

type NodeInfo struct {
    ID       string
    Address  string
    IsAlive  bool
    LastSeen time.Time
}

type Membership interface {
    Add(id, address string)
    Remove(id string)
    Heartbeat(id, address string)
    MarkDead(id string)
    MarkAlive(id string)

    AliveCount() int
    AliveNodes() []string
    LeaderID() string
    Snapshot() []NodeInfo
    Lookup(id string) *NodeInfo

    LeaderLastSeen() time.Time
    SetLeaderLease(d time.Duration)
    OnLeaseExpiry(cb func(leaderID string)) CancelFunc

    StartEviction(ctx context.Context, interval time.Duration)
    StartLeaderLeaseChecker(ctx context.Context, interval time.Duration)
}

type CancelFunc func()
```

### 1.10 `go/pkg/reliability/` — Resilience Patterns

```go
package reliability

import "context"

type CircuitState int
const (
    CircuitClosed CircuitState = iota
    CircuitHalfOpen
    CircuitOpen
)

type CircuitBreaker interface {
    Execute(ctx context.Context, name string, fn func(context.Context) error) error
    State(name string) CircuitState
    Reset(name string)
}

type RateLimiter interface {
    Allow(ctx context.Context, key string) bool
    Wait(ctx context.Context, key string) error
    SetRate(key string, rate float64, burst int)
}

type Deduplicator interface {
    IsDuplicate(ctx context.Context, id string) bool
    MarkSeen(ctx context.Context, id string) error
    StartCleanup(ctx context.Context, interval time.Duration)
    StopCleanup()
}

type DLQ interface {
    Push(ctx context.Context, msg *DeadLetterMessage) error
    Pop(ctx context.Context) (*DeadLetterMessage, error)
    Peek(ctx context.Context) (*DeadLetterMessage, error)
    Len() int
    Clear(ctx context.Context) error
}

type DeadLetterMessage struct {
    OriginalTopic string
    OriginalKey   []byte
    Body          []byte
    Headers       map[string]string
    FailCount     int
    LastError     string
    FailedAt      time.Time
}

type SagaOrchestrator interface {
    Begin(ctx context.Context, sagaID string, steps []SagaStep) error
    ExecuteStep(ctx context.Context, sagaID string, stepName string) error
    Compensate(ctx context.Context, sagaID string) error
    Status(ctx context.Context, sagaID string) (*SagaStatus, error)
}

type SagaStep struct {
    Name        string
    Execute     func(context.Context) error
    Compensate  func(context.Context) error
    Timeout     time.Duration
}
```

### 1.11 `go/pkg/replyrouter/` — Reply Correlation

```go
package replyrouter

import "context"

type ReplyRouter interface {
    Register(ctx context.Context, correlationID string, ch chan<- *Message, timeout time.Duration) error
    Cancel(correlationID string)
    Deliver(ctx context.Context, correlationID string, msg *Message) bool
    PendingCount() int
    StartCleanup(ctx context.Context)
    StopCleanup()
}
```

### 1.12 `go/pkg/partition/` — Partition Management

```go
package partition

import "context"

type PartitionID uint32
type NodeID string

type Assignment struct {
    NodeID    NodeID
    Address   string
    Partition PartitionID
    Term      uint64
}

type PartitionManager interface {
    NumPartitions() uint32
    NodeForPartition(partition PartitionID) NodeID
    PartitionsForNode(nodeID NodeID) []PartitionID
    PartitionForKey(key string) PartitionID

    Assignments() []NodeID
    Rebalance(aliveNodes []NodeID, term uint64) []Assignment
    ApplyAssignments(assignments []Assignment)

    PublishAssignments(ctx context.Context, assignments []Assignment) error
    HandleAssignmentMessage(msg []byte) error

    LeaderID() NodeID
    OnLeaderChange(leaderID NodeID)

    SetProducer(p MessageProducer)
}

// --- RebalanceNotifier (watches membership, triggers rebalance) ---

type RebalanceNotifier interface {
    SetNotify(fn func())
    CheckAndRebalance() bool
}
```

### 1.13 `go/pkg/engine/` — Rule Engine

```go
package engine

import "context"

type Rule struct {
    ID      string
    DSL     string
    Version uint64
    Active  bool
    Lane    scheduler.Lane
}

type Engine interface {
    Start(ctx context.Context) error
    Stop() error

    // Rule management
    AddRule(ctx context.Context, rule *Rule) error
    RemoveRule(ctx context.Context, ruleID string) error
    GetRule(ctx context.Context, ruleID string) (*Rule, error)
    ListRules(ctx context.Context) ([]*Rule, error)

    // Execution
    Execute(ctx context.Context, ruleID string, body []byte, opts *ExecuteOptions) (*scheduler.Result, error)

    // Compilation
    CompileRule(ctx context.Context, rule *Rule) error
    InvalidateCompilation(ruleID string)
}

type ExecuteOptions struct {
    Timeout     time.Duration
    CorrelationID string
    ReplyTo     string
    Metadata    map[string]string
}
```

---

## Layer 2: `go/internal/` — Implementations

Each sub-package implements exactly ONE interface from `pkg/`.

### 2.1 `go/internal/node/` — Node Assembly

```go
package node

// --- ProdNode implements node.Node ---

type ProdNode struct {
    id     node.ID
    config Config

    // === Injected Dependencies (all interfaces from pkg/) ===
    engine          engine.Engine
    scheduler       scheduler.Scheduler
    cluster         cluster.ClusterMember
    gossiper        cluster.Gossiper
    transportPub    transport.Publisher
    transportSub    transport.Subscriber
    requester       transport.Requester
    store           store.Store
    registry        registry.Registry
    planDist        plandist.PlanDistributor
    membership      membership.Membership
    compiler        vm.PlanCompiler
    vm              vm.VMRunner
    partition       partition.PartitionManager
    rebalancer      partition.RebalanceNotifier
    cb              reliability.CircuitBreaker
    rl              reliability.RateLimiter
    dedup           reliability.Deduplicator
    dlq             reliability.DLQ
    saga            reliability.SagaOrchestrator
    replyRouter     replyrouter.ReplyRouter

    // === Internally owned ===
    httpServer      *http.Server
    grpcServer      *grpc.Server
    admin           *admin.AdminServer
    consumers       []transport.MessageConsumer
    producers       []transport.MessageProducer
    cancelFns       []cluster.CancelFunc
    metrics         *metrics.Collector

    mu              sync.RWMutex
    shutdownCh      chan struct{}
    logger          *slog.Logger
}

// --- DI Constructor ---

type Dependencies struct {
    Engine          engine.Engine
    Scheduler       scheduler.Scheduler
    Cluster         cluster.ClusterMember
    Gossiper        cluster.Gossiper
    TransportPub    transport.Publisher
    TransportSub    transport.Subscriber
    Requester       transport.Requester
    Store           store.Store
    Registry        registry.Registry
    PlanDist        plandist.PlanDistributor
    Membership      membership.Membership
    Compiler        vm.PlanCompiler
    VM              vm.VMRunner
    Partition       partition.PartitionManager
    Rebalancer      partition.RebalanceNotifier
    CircuitBreaker  reliability.CircuitBreaker
    RateLimiter     reliability.RateLimiter
    Dedup           reliability.Deduplicator
    DLQ             reliability.DLQ
    Saga            reliability.SagaOrchestrator
    ReplyRouter     replyrouter.ReplyRouter
    Logger          *slog.Logger
}

// NewNode — pure DI, no hidden creations
func NewNode(cfg Config, deps Dependencies) *ProdNode {
    // Validate all required deps
    if deps.Engine == nil { panic("engine required") }
    if deps.Scheduler == nil { panic("scheduler required") }
    // ... validate all

    return &ProdNode{
        id:          node.ID(cfg.NodeID),
        config:      cfg,
        engine:      deps.Engine,
        scheduler:   deps.Scheduler,
        // ...
    }
}

func (n *ProdNode) Start(ctx context.Context) error {
    // 1. Start cluster membership + raft
    n.cluster.Start(ctx)

    // 2. Start gossiper for node discovery
    n.gossiper.OnNodeJoin(func(nodeID, addr string) {
        n.membership.Heartbeat(nodeID, addr)
        if addr != "" && nodeID != string(n.id) {
            n.gossiper.AddPeer(nodeID, addr)
        }
    })

    // 3. Start consumers
    for _, c := range n.consumers { go c.Start(ctx) }

    // 4. Start plan distribution
    n.planDist.Start(ctx)
    n.membership.StartEviction(ctx, 10*time.Second)

    // 5. Start scheduler
    n.scheduler.Start(ctx)

    // 6. Start reply router cleanup
    n.replyRouter.StartCleanup(ctx)

    // 7. Start HTTP + gRPC servers
    go n.serveHTTP(ctx)
    go n.serveGRPC(ctx)

    // 8. Signal readiness
    n.logger.Info("node started", "id", n.id)
    return nil
}
```

### 2.2 `go/internal/node/factory.go` — Default Wiring

```go
package node

// DefaultDependencies builds production-wired dependencies.
// Used by cmd/ binaries. Tests provide their own wiring.
func DefaultDependencies(cfg Config) Dependencies {
    return Dependencies{
        Engine:          engine.New(engine.Config{...}),
        Scheduler:       scheduler.New(...),
        Cluster:         cluster.NewRaftCluster(cfg.RaftAddr, ...),
        Gossiper:        cluster.NewGossiper(...),
        TransportPub:    transport.NewKafkaProducer(cfg.KafkaBrokers),
        TransportSub:    transport.NewKafkaConsumer(cfg.KafkaBrokers, cfg.GroupID),
        Store:           execstate.NewFileStore(cfg.DataDir),
        Registry:        registry.New(...),
        PlanDist:        plandist.NewDistributor(...),
        Membership:      membership.New(),
        Compiler:        compiler.NewLocalCompiler(),
        VM:              bridge.NewVMRunner(),
        Partition:       partition.NewManager(64),
        Rebalancer:      partition.NewRebalanceNotifier(...),
        CircuitBreaker:  reliability.NewCircuitBreaker(...),
        RateLimiter:     reliability.NewRateLimiter(...),
        Dedup:           reliability.NewDedup(...),
        DLQ:             reliability.NewDLQ(...),
        Saga:            reliability.NewSagaOrchestrator(...),
        ReplyRouter:     replyrouter.NewReplyRouter(),
        Logger:          slog.Default(),
    }
}
```

### 2.3 `go/internal/node/http.go` — HTTP Lifecycle

```go
package node

// serveHTTP starts the HTTP server with admin + health + metrics routes
func (n *ProdNode) serveHTTP(ctx context.Context) {
    mux := http.NewServeMux()
    
    // Admin routes (mounted from reusable admin API)
    adminAPI := admin.NewAdminAPI(n.engine, n.registry, n.scheduler)
    adminAPI.RegisterRoutes(mux, "/admin/")
    
    // Health + readiness
    mux.HandleFunc("/health", n.handleHealth)
    mux.HandleFunc("/readyz", n.handleReadyz)
    
    // Metrics
    mux.HandleFunc("/metrics", n.handleMetrics)
    
    n.httpServer = &http.Server{
        Addr:    n.config.HTTPAddr,
        Handler: mux,
    }
    
    if err := n.httpServer.ListenAndServe(); err != http.ErrServerClosed {
        n.logger.Error("http server error", "error", err)
    }
}
```

### 2.4 `go/internal/transport/kafka/` — Kafka Impl

```go
package kafka

type Producer struct {
    writer *kafka.Writer
    // ...
}

func NewProducer(brokers []string, opts ...Option) *Producer { ... }
func (p *Producer) Send(ctx context.Context, key, msg []byte) error { ... }
func (p *Producer) Close() error { ... }

type Consumer struct {
    reader  *kafka.Reader
    topic   string
    handler func(ctx context.Context, msg []byte)
    stopCh  chan struct{}
}

func NewConsumer(brokers []string, groupID, topic string) *Consumer { ... }
func (c *Consumer) Start(ctx context.Context) error { ... }
func (c *Consumer) Stop() error { ... }
func (c *Consumer) Topic() string { ... }
```

### 2.5 `go/internal/transport/memory/` — In-Memory Bus

```go
package memory

// MemoryBus implements transport.FullEventBus — usable by tests + simulator
type MemoryBus struct {
    subscribers map[string]map[string]chan *transport.Message
    handlers    map[string]func(ctx context.Context, msg *transport.Message)
    mu          sync.RWMutex
}

func NewBus() *MemoryBus { ... }
func (b *MemoryBus) Publish(ctx context.Context, topic string, msg *transport.Message) error { ... }
func (b *MemoryBus) Subscribe(ctx context.Context, topic string, handler transport.MessageHandler) (*transport.Subscription, error) { ... }
func (b *MemoryBus) Request(ctx context.Context, topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error) { ... }
func (b *MemoryBus) Reply(ctx context.Context, correlationID string, msg *transport.Message) error { ... }
func (b *MemoryBus) Broadcast(ctx context.Context, topic string, msg *transport.Message) error { ... }
func (b *MemoryBus) Close() error { ... }
```

### 2.6 `go/internal/scheduler/` — Production Scheduler

```go
package scheduler

type ProdScheduler struct {
    id       string
    workers  int
    stopCh   chan struct{}
    wg       sync.WaitGroup

    // Priority lanes
    lanes    map[scheduler.Lane]*laneQueue
    
    // Injected deps
    store    store.Store
    registry registry.Registry
    bus      transport.Publisher
    metrics  *metrics.Collector
    logger   *slog.Logger
    
    // Execution tracking
    active   sync.Map
    execCount atomic.Int64
}

type laneQueue struct {
    ready   chan *scheduler.ExecutionContext
    waiting map[string]*scheduler.ExecutionContext
}

func NewProdScheduler(id string, workers int, store store.Store, reg registry.Registry, bus transport.Publisher, logger *slog.Logger) *ProdScheduler {
    return &ProdScheduler{
        id:      id,
        workers: workers,
        lanes: map[scheduler.Lane]*laneQueue{
            scheduler.LaneFast:   {ready: make(chan *scheduler.ExecutionContext, 5000)},
            scheduler.LaneNormal: {ready: make(chan *scheduler.ExecutionContext, 2000)},
            scheduler.LaneHeavy:  {ready: make(chan *scheduler.ExecutionContext, 500)},
        },
        store:    store,
        registry: reg,
        bus:      bus,
        metrics:  metrics.NewCollector(),
        logger:   logger,
    }
}

func (s *ProdScheduler) Enqueue(ctx *scheduler.ExecutionContext) error {
    lane := s.lanes[ctx.Lane]
    select {
    case lane.ready <- ctx:
        return nil
    default:
        return ErrQueueFull
    }
}
```

### 2.7 `go/internal/cluster/` — Raft + Gossip Implementations

```go
package cluster

type RaftCluster struct {
    id        cluster.MemberID
    addr      string
    raft      *raft.Raft
    transport *raft.InmemTransport
    fsm       *FSM
    leaderCh  chan bool
    termCh    chan uint64

    // Leader change subscribers
    leaderSubs map[string]func(bool)
    termSubs   map[string]func(uint64)
    mu         sync.RWMutex
}

func NewRaftCluster(id, addr string, opts ...Option) *RaftCluster { ... }
func (c *RaftCluster) Start(ctx context.Context) error { ... }
func (c *RaftCluster) Stop() { ... }
func (c *RaftCluster) IsLeader() bool { ... }
func (c *RaftCluster) LeaderID() cluster.MemberID { ... }

type Gossiper struct {
    nodeID    string
    transport transport.Publisher
    joinCBs   []func(nodeID, addr string)
    leaveCBs  []func(nodeID string)
    peers     map[string]string
    mu        sync.Mutex
}

func NewGossiper(nodeID string, pub transport.Publisher) *Gossiper { ... }
func (g *Gossiper) OnNodeJoin(fn func(nodeID, addr string)) cluster.CancelFunc { ... }
```

### 2.8 `go/internal/store/` — Execution Store Implementation

```go
package store

type FileStore struct {
    dir    string
    mu     sync.RWMutex
    cache  map[store.ExecutionID]*store.ExecutionRecord
}

func NewFileStore(dir string) (*FileStore, error) { ... }
func (fs *FileStore) Create(ctx context.Context, record *store.ExecutionRecord) error { ... }
func (fs *FileStore) Save(ctx context.Context, record *store.ExecutionRecord) error { ... }
func (fs *FileStore) Load(ctx context.Context, id store.ExecutionID) (*store.ExecutionRecord, error) { ... }
func (fs *FileStore) List(ctx context.Context) ([]*store.ExecutionRecord, error) { ... }
func (fs *FileStore) Delete(ctx context.Context, id store.ExecutionID) error { ... }
func (fs *FileStore) Close() error { ... }
```

### 2.9 `go/bridge/vm.go` — CGo VM Adapter (implements `vm.PlanCompiler` + `vm.VMRunner`)

```go
package bridge

type BridgeVM struct{}

func NewBridgeVM() *BridgeVM { return &BridgeVM{} }

func (b *BridgeVM) Compile(ctx context.Context, dsl string, ruleID string) (*vm.CompileResult, error) {
    // Calls C.flowrulz_compile via CGo
}
func (b *BridgeVM) InitContext(ctx context.Context, body []byte) ([]byte, error) { ... }
func (b *BridgeVM) ExecuteStep(ctx context.Context, plan, ctxBytes, respBytes []byte, opts *vm.StepOptions) (*vm.StepResult, error) { ... }
func (b *BridgeVM) ParseServiceMethod(raw string) (string, string) { ... }
```

---

## Layer 3: `go/cmd/` — Wiring & Assembly

### 3.1 `go/cmd/flowrulz/main.go` — Production Entrypoint

```go
func main() {
    cfg := config.Load()
    
    // === Build all dependencies ===
    // (each creates and returns the concrete impl that conforms to pkg/ interface)
    
    kafkaPub := kafka.NewProducer(cfg.KafkaBrokers)
    kafkaSub := kafka.NewConsumer(cfg.KafkaBrokers, cfg.GroupID, cfg.Topic)
    raftCluster := cluster.NewRaftCluster(cfg.NodeID, cfg.RaftAddr)
    gossiper := cluster.NewGossiper(cfg.NodeID, kafkaPub)
    fileStore := store.NewFileStore(cfg.DataDir)
    svcRegistry := registry.NewRegistry(...)
    planDist := plandist.NewDistributor(cfg.NodeID, ...)
    membership := membership.New()
    localCompiler := compiler.NewLocalCompiler()
    bridgeVM := bridge.NewBridgeVM()
    partitionMgr := partition.NewManager(64)
    rebalancer := partition.NewRebalanceNotifier(partitionMgr, membership.AliveNodes, planDist.CurrentTerm)
    prodSched := scheduler.NewProdScheduler(cfg.NodeID, cfg.Workers, fileStore, svcRegistry, kafkaPub, slog.Default())
    cb := reliability.NewCircuitBreaker(...)
    rl := reliability.NewRateLimiter(...)
    dedup := reliability.NewDedup(...)
    dlq := reliability.NewDLQ(...)
    saga := reliability.NewSagaOrchestrator(...)
    replyRouter := replyrouter.NewReplyRouter()
    ruleEngine := engine.NewRuleEngine(localCompiler, prodSched, svcRegistry)
    
    // === Wire into Dependencies struct ===
    deps := node.Dependencies{
        Engine:          ruleEngine,
        Scheduler:       prodSched,
        Cluster:         raftCluster,
        Gossiper:        gossiper,
        TransportPub:    kafkaPub,
        TransportSub:    kafkaSub,
        Requester:       kafkaPub,   // implements Requester if using Kafka reply
        Store:           fileStore,
        Registry:        svcRegistry,
        PlanDist:        planDist,
        Membership:      membership,
        Compiler:        bridgeVM,
        VM:              bridgeVM,
        Partition:       partitionMgr,
        Rebalancer:      rebalancer,
        CircuitBreaker:  cb,
        RateLimiter:     rl,
        Dedup:           dedup,
        DLQ:             dlq,
        Saga:            saga,
        ReplyRouter:     replyRouter,
        Logger:          slog.Default(),
    }
    
    // === Assemble ===
    n := node.NewNode(cfg, deps)
    
    // === Run ===
    ctx := context.Background()
    if err := n.Start(ctx); err != nil {
        log.Fatal(err)
    }
    
    // === Wait for signal ===
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    
    n.Shutdown(ctx)
}
```

### 3.2 Simulator — Uses Same Interfaces with Memory Implementations

```go
// simulator/cmd/simulator/main.go

func main() {
    cfg := config.Load()
    
    // === Memory-backed deps (no Kafka, no CGo) ===
    memBus := memory.NewBus()        // implements transport.FullEventBus
    memStore := store.NewMemoryStore() // implements store.Store
    memRegistry := registry.NewMemoryRegistry()
    mockVM := mock.NewMockVM()       // implements vm.PlanCompiler + vm.VMRunner (no CGo!)
    mockCluster := mock.NewMockCluster()
    prodSched := scheduler.NewProdScheduler("sim-1", cfg.Workers, memStore, memRegistry, memBus, slog.Default())
    
    deps := node.Dependencies{
        Cluster:    mockCluster,
        TransportPub: memBus,
        TransportSub: memBus,
        Store:      memStore,
        Registry:   memRegistry,
        Compiler:   mockVM,
        VM:         mockVM,
        Scheduler:  prodSched,
        // ...
    }
    
    n := node.NewNode(cfg, deps)
}
```

---

## Actual Current File Tree

```
go/
├── pkg/                                          # Interfaces + domain types (no impls)
│   ├── cluster/cluster.go, gossiper.go, types.go
│   ├── engine/engine.go, types.go
│   ├── membership/membership.go, types.go
│   ├── node/node.go, types.go, errors.go
│   ├── partition/partition.go, rebalance.go, types.go
│   ├── plandist/plandist.go, types.go
│   ├── registry/registry.go, types.go
│   ├── reliability/circuitbreaker.go, ratelimiter.go, dedup.go, dlq.go, saga.go
│   ├── replyrouter/replyrouter.go
│   ├── scheduler/scheduler.go, types.go, lane.go, errors.go
│   ├── store/store.go, types.go
│   ├── transport/interfaces.go, types.go, errors.go, eventbus.go
│   └── vm/vm.go, types.go
│
├── internal/                                      # Implementations
│   ├── admin/api.go, service.go
│   ├── cluster/node.go, raft.go, gossip.go, transport.go
│   ├── compiler/compiler.go
│   ├── engine/engine.go, persistence.go
│   ├── execnode/              # Legacy node — 11 files
│   ├── execstate/             # Execution file store
│   ├── flow/flow.go
│   ├── logger/logger.go
│   ├── membership/membership.go, lease.go
│   ├── node/                  # ProdNode DI assembler — 10 files
│   │   ├── prod.go, config.go, lifecylce.go
│   │   ├── grpc.go, cluster.go, http.go
│   │   ├── handlers.go, messages.go, recovery.go
│   │   └── exec_registry.go, execute_plan.go
│   ├── observability/metrics.go, tracer.go
│   ├── partition/manager.go, rebalance.go
│   ├── plandist/distributor.go, ack.go
│   ├── plugins/loader.go
│   ├── registry/registry.go, lookup.go, health.go, http.go
│   ├── reliability/circuitbreaker.go, ratelimit.go, dedup.go, dlq.go, saga.go
│   ├── replyrouter/router.go
│   ├── scheduler/prod.go, lane.go, worker.go
│   └── transport/
│       ├── producer.go, consumer.go, types.go
│       ├── kafka/config.go, consumer.go, producer.go
│       ├── grpc/bus.go, client.go, *.pb.go
│       └── memory/bus.go
│
├── bridge/                                         # CGo (FlowRulZ C core)
│   ├── bridge.go, caller_bridge.c                  # CGo declarations
│   ├── vm_adapter.go                               # BridgeVM adapter
│   ├── compile.go, execute.go, plan.go, memory.go  # helpers
│   └── bridge_test.go
│
├── cmd/
│   ├── flowrulz/main.go
│   └── flowrulz-compiler/main.go
│
└── flow/client.go                                   # Client SDK

simulator/ (unchanged external project)

> **Completed restructuring** (Phase 2-3): All file renames done (`raft_cluster.go→raft.go`, `plan.go→distributor.go`, `scheduler.go→prod.go`, `timerwheel.go→worker.go`, `server.go→api.go`). Kafka moved to subdirectory (`kafka/producer.go+consumer.go+config.go`). Node package expanded to 10 files (grpc.go+cluster.go extracted). Phase 3a: 6 adapter files (`pkgsupport.go`) in `internal/{execstate,scheduler,registry,cluster,engine,reliability}/`. Phase 3b: `Dependencies` struct + `NewNode(cfg,deps)` + `DefaultDependencies()` in `internal/node/`. Remaining: migrate `execnode/execnode.go` and `admin/api.go` to DI pattern, switch remaining 8+ concrete ProdNode fields to interfaces, create `pkg/transport` interfaces.

---

## SOLID Compliance Matrix

| Principle | How It's Achieved |
|-----------|------------------|
| **S**ingle Responsibility | One package per concern. `pkg/` has interfaces. `internal/` has implementations. `ProdNode` assembles, doesn't implement individual concerns. |
| **O**pen/Closed | All behavior behind interfaces. Adding Kafka→Redis transport: impl new adapter, no existing code changes. Adding a new lane: impl new `Scheduler`. |
| **L**iskov Substitution | All consumers depend on interfaces. Simulator's `MemoryBus` is a drop-in for production `KafkaPub`. Any `Store` impl works. |
| **I**nterface Segregation | `Publisher != Subscriber != Requester`. No 9-method god interface. Each consumer takes exactly what it needs. `ExecutionNode` declares 8 role interfaces, not one god interface. |
| **D**ependency Inversion | `cmd/` wires concrete impls → constructs `Dependencies` struct → injects into `ProdNode`. High-level `ProdNode` never says `kafka.NewProducer(...)`. |
| **DRY** | Single `ExecutionContext` in `pkg/scheduler/`. Single `Message` in `pkg/transport/`. No parallel scheduler implementations with different types. |
| **DI** | `NewNode(cfg, deps)` — everything passed in. No hidden `new()` calls inside constructors. Test provides `deps` with mocks. |
