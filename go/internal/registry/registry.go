package registry

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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

func (r *ServiceRegistry) Register(name string, endpoint *Endpoint) error {
	if name == "" {
		return fmt.Errorf("registry: empty service name")
	}
	if endpoint == nil {
		return fmt.Errorf("registry: nil endpoint")
	}
	if endpoint.Address == "" {
		return fmt.Errorf("registry: empty endpoint address")
	}
	if endpoint.Port <= 0 {
		return fmt.Errorf("registry: invalid port %d", endpoint.Port)
	}
	if endpoint.Protocol == "" {
		endpoint.Protocol = ProtocolHTTP
	}
	if endpoint.NodeID == "" {
		endpoint.NodeID = localNodeID()
	}
	endpoint.Healthy = true

	r.mu.Lock()
	defer r.mu.Unlock()

	eps := r.services[name]
	for i, ep := range eps {
		if ep.NodeID == endpoint.NodeID && ep.Address == endpoint.Address && ep.Port == endpoint.Port {
			eps[i] = endpoint
			r.services[name] = eps
			return nil
		}
	}
	r.services[name] = append(eps, endpoint)
	if _, ok := r.roundRobin[name]; !ok {
		var zero uint64
		r.roundRobin[name] = &zero
	}
	return nil
}

func (r *ServiceRegistry) RegisterInstance(inst *ServiceInstance) error {
	if inst.Name == "" {
		return fmt.Errorf("registry: empty service name")
	}
	if inst.Endpoint.Address == "" {
		return fmt.Errorf("registry: empty endpoint address")
	}
	if inst.Endpoint.Port <= 0 {
		return fmt.Errorf("registry: invalid port %d", inst.Endpoint.Port)
	}
	if inst.ID == "" {
		inst.ID = fmt.Sprintf("%s-%s-%d", inst.Name, inst.Endpoint.Address, inst.Endpoint.Port)
	}
	if inst.Endpoint.Protocol == "" {
		inst.Endpoint.Protocol = ProtocolHTTP
	}
	if inst.Weight <= 0 {
		inst.Weight = 100
	}
	inst.Healthy = true
	inst.HeartbeatAt = time.Now()
	if inst.RegisteredAt.IsZero() {
		inst.RegisteredAt = time.Now()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.services[inst.Name] = append(r.services[inst.Name], &Endpoint{
		NodeID:   inst.Endpoint.NodeID,
		Address:  inst.Endpoint.Address,
		Port:     inst.Endpoint.Port,
		Protocol: inst.Endpoint.Protocol,
		Healthy:  true,
	})

	existing := r.instances[inst.Name]
	for i, e := range existing {
		if e.ID == inst.ID {
			existing[i] = inst
			r.instances[inst.Name] = existing
			return nil
		}
	}
	r.instances[inst.Name] = append(existing, inst)
	if _, ok := r.roundRobin[inst.Name]; !ok {
		var zero uint64
		r.roundRobin[inst.Name] = &zero
	}
	return nil
}

func (r *ServiceRegistry) Unregister(name string, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	eps := r.services[name]
	filtered := make([]*Endpoint, 0, len(eps))
	for _, ep := range eps {
		if ep.NodeID != nodeID {
			filtered = append(filtered, ep)
		}
	}
	if len(filtered) == 0 {
		delete(r.services, name)
		delete(r.roundRobin, name)
	} else {
		r.services[name] = filtered
	}

	instances := r.instances[name]
	kept := make([]*ServiceInstance, 0, len(instances))
	for _, inst := range instances {
		if inst.Endpoint.NodeID != nodeID {
			kept = append(kept, inst)
		}
	}
	if len(kept) == 0 {
		delete(r.instances, name)
	} else {
		r.instances[name] = kept
	}
}

func (r *ServiceRegistry) UnregisterInstance(name, instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	instances := r.instances[name]
	kept := make([]*ServiceInstance, 0, len(instances))
	for _, inst := range instances {
		if inst.ID != instanceID {
			kept = append(kept, inst)
		}
	}
	if len(kept) == 0 {
		delete(r.instances, name)
	} else {
		r.instances[name] = kept
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
