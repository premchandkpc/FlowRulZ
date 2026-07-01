package registry

import (
	"sync/atomic"
)

type Protocol string

const (
	ProtocolHTTP Protocol = "http"
	ProtocolGRPC Protocol = "grpc"
	ProtocolTCP  Protocol = "tcp"
)

type LBStrategy string

const (
	LBStrategyRandom      LBStrategy = "random"
	LBStrategyRoundRobin  LBStrategy = "roundrobin"
	LBStrategyLocalPrefer LBStrategy = "localprefer"
	LBStrategyLeastLoaded LBStrategy = "leastloaded"
)

type MethodInfo struct {
	Name       string   `json:"name"`
	InputType  string   `json:"input_type,omitempty"`
	OutputType string   `json:"output_type,omitempty"`
	Sync       bool     `json:"sync"`
	Async      bool     `json:"async"`
	TimeoutMs  int      `json:"timeout_ms,omitempty"`
	Tags       []string `json:"tags,omitempty"`
}

type ServiceCapabilities struct {
	Sync  bool `json:"sync"`
	Async bool `json:"async"`
}

type Endpoint struct {
	NodeID   string   `json:"node_id"`
	Address  string   `json:"address"`
	Port     int      `json:"port"`
	Protocol Protocol `json:"protocol"`
	Healthy  bool     `json:"healthy"`
	Load     float64  `json:"load,omitempty"`
}

type ServiceInstance struct {
	ID           string              `json:"id"`
	Name         string              `json:"name"`
	Version      string              `json:"version,omitempty"`
	Methods      []MethodInfo        `json:"methods,omitempty"`
	Capabilities ServiceCapabilities `json:"capabilities"`
	Endpoint     Endpoint            `json:"endpoint"`
	Zone         string              `json:"zone,omitempty"`
	Weight       int                 `json:"weight,omitempty"`
	Tags         []string            `json:"tags,omitempty"`
	Metadata     map[string]string   `json:"metadata,omitempty"`
	Healthy      bool                `json:"healthy"`
	HeartbeatAt  time.Time           `json:"heartbeat_at"`
	RegisteredAt time.Time           `json:"registered_at"`
}

type ServiceInfo struct {
	Name      string             `json:"name"`
	Methods   []MethodInfo       `json:"methods,omitempty"`
	Instances []*ServiceInstance `json:"instances"`
	Endpoints []*Endpoint        `json:"endpoints"`
}

type ServiceRegistry struct {
	mu          sync.RWMutex
	services    map[string][]*Endpoint
	instances   map[string][]*ServiceInstance
	roundRobin  map[string]*uint64
	defStrategy LBStrategy
	hbTimeout   time.Duration
}

func New() *ServiceRegistry {
	return &ServiceRegistry{
		services:    make(map[string][]*Endpoint),
		instances:   make(map[string][]*ServiceInstance),
		roundRobin:  make(map[string]*uint64),
		defStrategy: LBStrategyLeastLoaded,
		hbTimeout:   30 * time.Second,
	}
}

var localNodeIDValue atomic.Value

func init() {
	localNodeIDValue.Store("")
}

func localNodeID() string {
	v, _ := localNodeIDValue.Load().(string)
	return v
}

func copyEndpoints(src []*Endpoint) []*Endpoint {
	if src == nil {
		return nil
	}
	out := make([]*Endpoint, len(src))
	copy(out, src)
	return out
}

func copyInstances(src []*ServiceInstance) []*ServiceInstance {
	if src == nil {
		return nil
	}
	out := make([]*ServiceInstance, len(src))
	copy(out, src)
	return out
}
