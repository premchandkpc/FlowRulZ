package node

import (
	"context"
	"net/http"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
)

// ServiceInvoker abstracts service call dispatch for both production and simulator.
type ServiceInvoker interface {
	Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error)
}

// NodeEngine defines the engine interface needed by the execution node.
type NodeEngine interface {
	ActivePlanBytes() [][]byte
	AddVersion(id, dsl string, plan []byte, version uint64) error
	Promote(id string, version uint64) error
	SetAfterDeploy(fn func(id, dsl string, plan []byte, version uint64))
	SetAfterPromote(fn func(id string, version uint64))
}

// NodeDLQ defines the DLQ interface needed by the execution node.
type NodeDLQ interface {
	Send(entry *reliability.DeadLetterEntry) error
	Len() int
}

// NodeSagaTracker defines the saga compensation interface needed by the execution node.
type NodeSagaTracker interface {
	RegisterStep(execID string, step reliability.SagaStep)
	Compensate(execID string) error
	Clear(execID string)
}

// GRPCService represents a startable/stoppable gRPC service.
type GRPCService interface {
	Start() error
	Stop()
}

// AdminHandler provides the admin HTTP handler.
type AdminHandler interface {
	Handler() http.Handler
}

// MetricsSnapshotProvider returns a point-in-time snapshot of metrics.
type MetricsSnapshotProvider interface {
	Snapshot() observability.MetricSnapshot
}

// SpanExporter represents a startable/stoppable trace exporter.
type SpanExporter interface {
	Start(ctx context.Context)
	Stop()
}

// ClusterTransport manages peer-to-peer cluster communication.
type ClusterTransport interface {
	Start() error
	Stop()
	AddPeer(id, addr string) error
	Publish(topic, key string, body []byte) error
	Gossiper() GossipProvider
}

// GossipProvider abstracts the gossip layer.
type GossipProvider interface {
	OnNodeJoin(fn func(nodeID, address string))
}

// --- New interfaces for decoupling ---

// RateLimiter abstracts rate limiting for ingress pipeline.
type RateLimiter interface {
	Allow(key string) bool
}

// DedupChecker abstracts deduplication for ingress pipeline.
type DedupChecker interface {
	CheckAndMark(key string) bool
	StartCleanup(ctx context.Context, interval time.Duration)
}

// ServiceLookup abstracts service instance lookup and health marking.
type ServiceLookup interface {
	LookupInstance(serviceName, method string) (*registry.ServiceInstance, error)
	MarkUnhealthy(serviceName, nodeID string)
	SetHeartbeatTimeout(timeout time.Duration)
}

// TransportFactory abstracts transport consumer/producer creation.
type TransportFactory interface {
	NewConsumer(topic string, handler transport.MessageHandler) transport.MessageConsumer
	NewProducer(topic string) transport.MessageProducer
}

// PlanDistributor abstracts plan distribution operations.
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

// ProtocolDispatcher abstracts protocol-aware service call dispatch.
type ProtocolDispatcher interface {
	CallHTTP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte) ([]byte, error)
	CallGRPC(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte) ([]byte, error)
	CallTCP(ctx context.Context, inst *registry.ServiceInstance, method string, body []byte) ([]byte, error)
}

// LeadershipStrategy abstracts leadership queries (Raft vs single-leader fallback).
type LeadershipStrategy interface {
	IsLeader() bool
	CurrentTerm() uint64
	CaptureLeadershipToken() pkgcluster.LeadershipToken
	ValidateLeadershipToken(token pkgcluster.LeadershipToken) bool
	LeaderID(selfNodeID string) string
}

// CircuitBreakerRegistry provides per-service circuit breakers.
type CircuitBreakerRegistry interface {
	For(serviceName string) *reliability.CircuitBreaker
}

// ExecTracker abstracts execution registration and cancellation.
type ExecTracker interface {
	Register(id string, cancel context.CancelFunc, name string)
	Unregister(id string)
	Cancel(id string) bool
	CancelAll()
}

// ExecLister abstracts execution listing for admin handlers.
type ExecLister interface {
	List() map[string]time.Time
	Len() int
}

// StateStore abstracts execution state persistence.
type StateStore = execstate.Store
