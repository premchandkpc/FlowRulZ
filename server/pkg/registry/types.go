package registry

import "time"

type ServiceID string

type LBStrategy int

const (
	RoundRobin LBStrategy = iota
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
	EventRegistered EventType = iota
	EventUnregistered
	EventHealthChanged
)

type RegistryEvent struct {
	Type  EventType
	Name  string
	Error error
}
