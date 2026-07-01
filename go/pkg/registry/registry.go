package registry

import (
	"context"
	"errors"
	"time"
)

type ServiceID string

type LBStrategy int

const (
	RoundRobin       LBStrategy = iota
	LeastConnections
	Random
)

type ServiceRegistration struct {
	Name     string
	Version  string
	Address  string
	Methods  []MethodSpec
	Metadata map[string]string
	Tags     []string
}

type MethodSpec struct {
	Name   string
	Input  string
	Output string
}

type ServiceInstance struct {
	ID       ServiceID
	Name     string
	Address  string
	IsAlive  bool
	LastSeen time.Time
	Meta     map[string]string
}

type EventType int

const (
	EventRegistered     EventType = iota
	EventUnregistered
	EventHealthChanged
)

type RegistryEvent struct {
	Type  EventType
	Name  string
	Error error
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

var (
	ErrServiceNotFound = errors.New("service not found")
	ErrServiceUnavailable = errors.New("service unavailable")
)
