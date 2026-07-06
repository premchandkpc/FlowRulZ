// Package ports defines the inbound and outbound port interfaces for the core domain.
// Zero imports from internal/ — only stdlib and pkg/ allowed.
package ports

import (
	"context"
	"net/http"
	"time"
)

// --- Domain Types (no leaking adapter types) ---

type ExecutionID string

type ExecutionRecord struct {
	ID          ExecutionID
	PlanID      string
	State       string
	Output      []byte
	Error       string
	CreatedAt   time.Time
	CompletedAt time.Time
}

type ServiceInstance struct {
	ID      string
	Name    string
	Address string
	Healthy bool
	Methods []MethodSpec
	Meta    map[string]string
}

type MethodSpec struct {
	Name   string
	Input  string
	Output string
}

type DeadLetterEntry struct {
	ID        string
	Topic     Key
	Payload   []byte
	Error     string
	Timestamp time.Time
}

type SagaStep struct {
	StepID      string
	ServiceName string
	Method      string
	Body        []byte
	CompSvc     string
	CompMethod  string
}

type MetricSnapshot struct {
	Counters map[string]float64
	Gauges   map[string]float64
}

type LeadershipToken struct {
	Term     uint64
	LeaderID string
	Valid    bool
}

type AckMessage struct {
	RuleID  string
	Version uint64
	Status  string
	NodeID  string
}

type Key = string

// --- Outbound Ports (what the core needs from adapters) ---

// RuleEngine manages rule lifecycle: deploy, promote, active plans.
type RuleEngine interface {
	ActivePlanBytes() [][]byte
	AddVersion(id, dsl string, plan []byte, version uint64) error
	Promote(id string, version uint64) error
	SetAfterDeploy(fn func(id, dsl string, plan []byte, version uint64))
	SetAfterPromote(fn func(id string, version uint64))
}

// ServiceInvoker dispatches calls to external services.
type ServiceInvoker interface {
	Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error)
}

// StateStore persists execution state (ephemeral or durable).
type StateStore interface {
	Create(ctx context.Context, record *ExecutionRecord) error
	Save(ctx context.Context, record *ExecutionRecord) error
	Load(ctx context.Context, id ExecutionID) (*ExecutionRecord, error)
	List(ctx context.Context) ([]*ExecutionRecord, error)
	Delete(ctx context.Context, id ExecutionID) error
	Close() error
}

// MessageProducer sends messages to a topic.
type MessageProducer interface {
	SendMessage(ctx context.Context, key string, value []byte) error
	Close() error
}

// MessageConsumer receives messages from a topic.
type MessageConsumer interface {
	Start(ctx context.Context) error
	Stop() error
}

// MessageHandler processes a received message.
type MessageHandler func(ctx context.Context, key string, value []byte) error

// TransportFactory creates producers and consumers for topics.
type TransportFactory interface {
	NewProducer(topic string) MessageProducer
	NewConsumer(topic string, handler MessageHandler) MessageConsumer
}

// ClusterMember provides cluster membership and leadership.
type ClusterMember interface {
	IsLeader() bool
	CurrentTerm() uint64
	LeaderID() string
	LeaderAddr() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	SubscribeLeaderChanges(fn func(isLeader bool))
	CaptureLeadershipToken() LeadershipToken
	ValidateLeadershipToken(token LeadershipToken) bool
}

// ClusterTransport manages peer-to-peer cluster communication.
type ClusterTransport interface {
	Start() error
	Stop()
	AddPeer(id, addr string) error
	Publish(topic, key string, body []byte) error
}

// ServiceRegistry discovers and health-checks services.
type ServiceRegistry interface {
	Lookup(serviceName, method string) (*ServiceInstance, error)
	MarkUnhealthy(serviceName, nodeID string)
	SetHeartbeatTimeout(timeout time.Duration)
	RegisterHTTPHandler(w http.ResponseWriter, r *http.Request)
	HeartbeatHTTPHandler(w http.ResponseWriter, r *http.Request)
	ListServicesHTTPHandler(w http.ResponseWriter, r *http.Request)
}

// MetricsCollector records execution and error metrics.
type MetricsCollector interface {
	RecordExec(name string)
	RecordError(name string)
	Snapshot() MetricSnapshot
}

// Tracer manages distributed tracing lifecycle.
type Tracer interface {
	Start(ctx context.Context)
	Stop()
}

// RateLimiter controls request rate per key.
type RateLimiter interface {
	Allow(key string) bool
}

// Deduplicator prevents duplicate processing.
type Deduplicator interface {
	CheckAndMark(key string) bool
	StartCleanup(ctx context.Context, interval time.Duration)
}

// DeadLetterQueue stores failed messages for later retry.
type DeadLetterQueue interface {
	Send(entry *DeadLetterEntry) error
	Len() int
}

// SagaTracker manages compensating transactions.
type SagaTracker interface {
	RegisterStep(execID string, step SagaStep)
	Compensate(execID string) error
	Clear(execID string)
}

// PlanDistributor distributes execution plans to cluster nodes.
type PlanDistributor interface {
	CurrentTerm() uint64
	SendAck(ctx context.Context, ruleID string, version uint64, status string) error
	RecordAck(msg AckMessage)
	SetTerm(term uint64)
	Start(ctx context.Context) error
	Stop() error
	PublishPlan(ctx context.Context, id string, version uint64, plan []byte, dsl string) error
	WaitForAcks(ctx context.Context, id string, version uint64, quorum int, timeout time.Duration) error
	ActivatePlan(ctx context.Context, id string, version uint64) error
}

// PartitionManager manages key-space shard assignment.
type PartitionManager interface {
	Assign(partition int, nodeID string)
	Rebalance(nodes []string, term uint64) map[int]string
	OnLeaderChange(leaderID string)
}

// RebalanceNotifier triggers rebalance on cluster changes.
type RebalanceNotifier interface {
	CheckAndRebalance()
	SetNotify(fn func())
}

// ReplyRouter correlates request/reply by correlation ID.
type ReplyRouter interface {
	Register(id string) <-chan []byte
	Route(id string, msg []byte) bool
	PendingCount() int
	StartCleanup(ctx context.Context)
	StopCleanup()
}

// Scheduler manages delayed and periodic task execution.
type Scheduler interface {
	Start(ctx context.Context) error
	Stop() error
}

// ExecTracker tracks in-flight executions for cancellation.
type ExecTracker interface {
	Register(id string, cancel context.CancelFunc, name string)
	Unregister(id string)
	Cancel(id string) bool
	CancelAll()
	List() map[string]time.Time
	Len() int
}

// AdminHandler provides the admin HTTP handler.
type AdminHandler interface {
	Handler() http.Handler
}

// GRPCService represents a startable/stoppable gRPC service.
type GRPCService interface {
	Start() error
	Stop()
}

// FileWatcher watches .flow files for changes.
type FileWatcher interface {
	Start(ctx context.Context) error
	Stop()
}
