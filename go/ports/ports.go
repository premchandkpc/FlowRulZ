// Package ports defines the interfaces (ports) that the domain depends on.
// These are owned by the domain, implemented by adapters.
//
// Driving adapters (input) call into the domain through these interfaces.
// Driven adapters (output) implement these interfaces for the domain to call.
package ports

import (
	"context"
	"time"
)

// ServiceInvoker calls a downstream service via the protocol specified
// in the service registry (HTTP, gRPC, or TCP). This replaces the
// never-branches-on-protocol callService in node/execnode.
type ServiceInvoker interface {
	// Invoke calls a service method with the given body.
	// The protocol is determined by the registry endpoint, not hardcoded.
	Invoke(ctx context.Context, service, method string, body []byte) ([]byte, error)
}

// StateStore persists execution state. The domain defines WHAT to store;
// adapters decide WHERE (file, Postgres, etcd).
type StateStore interface {
	Create(ctx context.Context, state *ExecutionState) error
	Save(ctx context.Context, state *ExecutionState) error
	Load(ctx context.Context, id string) (*ExecutionState, error)
	ListByStatus(ctx context.Context, statuses ...ExecutionStatus) ([]*ExecutionState, error)
	Delete(ctx context.Context, id string) error
	Close() error

	// SavePending marks an execution as waiting for a service response.
	SavePending(ctx context.Context, execID string, pendingSvc uint16, pendingBody, ctxBytes []byte) error

	// SaveRunning marks an execution as running with updated context bytes.
	SaveRunning(ctx context.Context, execID string, ctxBytes []byte) error

	// SaveCompleted marks an execution as completed with output.
	SaveCompleted(ctx context.Context, execID string, output []byte) error

	// SaveFailed marks an execution as failed with error.
	SaveFailed(ctx context.Context, execID string, errMsg string) error
}

// ExecutionStatus represents the status of an execution.
type ExecutionStatus int

const (
	StatusCreated ExecutionStatus = iota
	StatusRunning
	StatusWaitingForService
	StatusCompleted
	StatusFailed
)

// ExecutionState represents the state of a single execution.
type ExecutionState struct {
	ID         string
	RuleID     string
	Version    uint64
	PlanBytes  []byte
	CtxBytes   []byte // serialized VM execution context
	Status     ExecutionStatus
	PendingSvc uint16       // which service call is pending
	PendingBody []byte      // pending service request body
	Error      string
	Output     []byte
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// MessageBus is a unified pub/sub and request/reply transport.
// This replaces the separate Kafka/grpc-bus/eventbus APIs.
type MessageBus interface {
	// Publish sends a message to a topic.
	Publish(ctx context.Context, topic string, msg *Message) error

	// Subscribe registers a handler for a topic.
	Subscribe(ctx context.Context, topic string, handler MessageHandler) (*Subscription, error)

	// Unsubscribe removes a subscription.
	Unsubscribe(ctx context.Context, sub *Subscription) error

	// Request sends a request and waits for a reply.
	Request(ctx context.Context, topic string, msg *Message, timeout time.Duration) (*Message, error)

	// Reply sends a reply to a correlation ID.
	Reply(ctx context.Context, correlationID string, msg *Message) error

	// Close shuts down the bus.
	Close() error
}

// Message is a transport-agnostic message envelope.
type Message struct {
	ID            string
	Topic         string
	Body          []byte
	Headers       map[string]string
	CorrelationID string
	PartitionKey  string
	CreatedAt     time.Time
}

// MessageHandler processes an incoming message.
type MessageHandler func(ctx context.Context, msg *Message) error

// Subscription represents a topic subscription.
type Subscription struct {
	ID    string
	Topic string
}

// ServiceRegistry provides service discovery with protocol awareness.
// The domain uses this to find WHERE a service is; adapters determine
// HOW to reach it (HTTP, gRPC, TCP).
type ServiceRegistry interface {
	// Lookup returns the endpoint for a service.
	Lookup(ctx context.Context, service string) (*ServiceEndpoint, error)

	// Register registers a service endpoint.
	Register(ctx context.Context, service string, endpoint *ServiceEndpoint) error

	// Unregister removes a service endpoint.
	Unregister(ctx context.Context, service string) error
}

// Protocol is the network protocol used to reach a service.
type Protocol string

const (
	ProtocolHTTP Protocol = "http"
	ProtocolGRPC Protocol = "grpc"
	ProtocolTCP  Protocol = "tcp"
)

// ServiceEndpoint is a network endpoint for a service.
type ServiceEndpoint struct {
	Address  string
	Port     int
	Protocol Protocol
	Healthy  bool
}

// ClusterCoordinator provides leader election and term management.
// Raft is one adapter for this port; the domain doesn't know or care.
type ClusterCoordinator interface {
	// IsLeader returns true if this node is the current leader.
	IsLeader() bool

	// CurrentTerm returns the current consensus term.
	CurrentTerm() uint64

	// CaptureLeadershipToken captures leadership state for fencing.
	// Use this to prevent split-brain: capture token, do work, validate before publish.
	CaptureLeadershipToken() LeadershipToken

	// ValidateLeadershipToken checks if a previously captured token is still valid.
	ValidateLeadershipToken(token LeadershipToken) bool
}

// LeadershipToken captures leadership state at a point in time.
type LeadershipToken struct {
	Leader bool
	Term   uint64
}

// Valid returns true if this token represents valid leadership.
func (lt LeadershipToken) Valid() bool {
	return lt.Leader && lt.Term > 0
}

// DedupTracker prevents duplicate message processing.
// The port signature forces a real key, making random-ID misuse visible.
type DedupTracker interface {
	// CheckAndMark atomically checks if a key was seen and marks it.
	// Returns true if the key was already seen (duplicate).
	CheckAndMark(key string) bool

	// Mark records a key as seen.
	Mark(key string)

	// Seen returns true if the key was seen within the TTL.
	Seen(key string) bool
}

// SagaCompensator manages saga compensation steps.
// The domain decides WHEN to compensate; this adapter decides HOW to persist/execute.
type SagaCompensator interface {
	// RegisterStep records a step for potential compensation.
	RegisterStep(execID string, step SagaStep)

	// GetSteps returns all registered steps for an execution.
	GetSteps(execID string) []SagaStep

	// Clear removes all steps for an execution (on success).
	Clear(execID string)

	// Compensate runs compensation steps in reverse order.
	Compensate(ctx context.Context, execID string) error
}

// SagaStep represents a single step in a saga that may need compensation.
type SagaStep struct {
	ServiceName string
	Method      string
	Body        []byte
	CompSvc     string
	CompMethod  string
}

// PlanDistributor distributes compiled plans to cluster nodes.
type PlanDistributor interface {
	// Distribute sends a plan to all cluster nodes.
	Distribute(ctx context.Context, plan []byte, version uint64) error

	// Acknowledge waits for acknowledgments from nodes.
	Acknowledge(ctx context.Context, planID string, timeout time.Duration) ([]string, error)
}
