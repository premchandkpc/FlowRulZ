package node

import (
	"context"
	"net/http"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
)

// --- Existing interfaces (kept for backward compatibility) ---

type ServiceInvoker interface {
	Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error)
}

type NodeEngine interface {
	ActivePlanBytes() [][]byte
	AddVersion(id, dsl string, plan []byte, version uint64) error
	Promote(id string, version uint64) error
	SetAfterDeploy(fn func(id, dsl string, plan []byte, version uint64))
	SetAfterPromote(fn func(id string, version uint64))
}

type NodeDLQ interface {
	Send(entry *reliability.DeadLetterEntry) error
	Len() int
}

type NodeSagaTracker interface {
	RegisterStep(execID string, step reliability.SagaStep)
	Compensate(execID string) error
	Clear(execID string)
}

type GRPCService interface {
	Start() error
	Stop()
}

type AdminHandler interface {
	Handler() http.Handler
}

type MetricsSnapshotProvider interface {
	Snapshot() observability.MetricSnapshot
}

type SpanExporter interface {
	Start(ctx context.Context)
	Stop()
}

type ClusterTransport interface {
	Start() error
	Stop()
	AddPeer(id, addr string) error
	Publish(topic, key string, body []byte) error
	Gossiper() GossipProvider
}

type GossipProvider interface {
	OnNodeJoin(fn func(nodeID, address string))
}

type RateLimiter interface {
	Allow(key string) bool
}

type DedupChecker interface {
	CheckAndMark(key string) bool
	StartCleanup(ctx context.Context, interval time.Duration)
}

type ServiceLookup interface {
	LookupInstance(serviceName, method string) (*registry.ServiceInstance, error)
	MarkUnhealthy(serviceName, nodeID string)
	SetHeartbeatTimeout(timeout time.Duration)
}

type TransportFactory interface {
	NewConsumer(topic string, handler transport.MessageHandler) transport.MessageConsumer
	NewProducer(topic string) transport.MessageProducer
}

type PlanDistributor interface {
	CurrentTerm() uint64
	SendAck(ctx context.Context, ruleID string, version uint64, status string) error
	RecordAck(msg plandist.AckMessage)
	SetTerm(term uint64)
	Start(ctx context.Context) error
	Stop() error
	PublishPlan(ctx context.Context, id string, version uint64, plan []byte, dsl string) error
	WaitForAcks(ctx context.Context, id string, version uint64, quorum int, timeout time.Duration) error
	ActivatePlan(ctx context.Context, id string, version uint64) error
}

type ProtocolDispatcher interface {
	CallHTTP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte) ([]byte, error)
	CallGRPC(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte) ([]byte, error)
	CallTCP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte) ([]byte, error)
}

type LeadershipStrategy interface {
	IsLeader() bool
	CurrentTerm() uint64
	CaptureLeadershipToken() ports.LeadershipToken
	ValidateLeadershipToken(token ports.LeadershipToken) bool
	LeaderID(selfNodeID string) string
}

type CircuitBreakerRegistry interface {
	For(serviceName string) *reliability.CircuitBreaker
}

type ExecTracker interface {
	Register(id string, cancel context.CancelFunc, name string)
	Unregister(id string)
	Cancel(id string) bool
	CancelAll()
}

type ExecLister interface {
	List() map[string]time.Time
	Len() int
}

type StateStore = execstate.Store
