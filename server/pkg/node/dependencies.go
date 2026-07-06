package node

import (
	"context"
	"net/http"
	"time"
)

// ExecRegistry tracks in-flight executions for cancellation and observability.
type ExecRegistry interface {
	Register(id string, cancel context.CancelFunc, name string)
	Unregister(id string)
	Cancel(id string) bool
	CancelAll()
	List() map[string]time.Time
	Len() int
}

// NodeEngine defines the engine interface needed by the execution node.
type NodeEngine interface {
	ActivePlanBytes() [][]byte
	AddVersion(id, dsl string, plan []byte, version uint64) error
	Promote(id string, version uint64) error
	SetAfterDeploy(fn func(id, dsl string, plan []byte, version uint64))
	SetAfterPromote(fn func(id string, version uint64))
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
}
