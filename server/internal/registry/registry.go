package registry

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Protocol string

const (
	ProtocolHTTP  Protocol = "http"
	ProtocolGRPC  Protocol = "grpc"
	ProtocolTCP   Protocol = "tcp"
	ProtocolKafka Protocol = "kafka"
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
	// Kafka-specific fields (only populated when Protocol == ProtocolKafka)
	Topic         string `json:"topic,omitempty"`
	ReplyTopic    string `json:"reply_topic,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
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
	if endpoint.Protocol == "" {
		endpoint.Protocol = ProtocolHTTP
	}

	// Protocol-specific validation
	switch endpoint.Protocol {
	case ProtocolHTTP, ProtocolGRPC, ProtocolTCP:
		if endpoint.Address == "" {
			return fmt.Errorf("registry: empty endpoint address for %s", endpoint.Protocol)
		}
		if endpoint.Port <= 0 {
			return fmt.Errorf("registry: invalid port %d for %s", endpoint.Port, endpoint.Protocol)
		}
	case ProtocolKafka:
		if endpoint.Topic == "" {
			return fmt.Errorf("registry: empty topic for kafka endpoint")
		}
	default:
		return fmt.Errorf("registry: unsupported protocol %q", endpoint.Protocol)
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
	if inst.Endpoint.Protocol == "" {
		inst.Endpoint.Protocol = ProtocolHTTP
	}

	// Protocol-specific validation
	switch inst.Endpoint.Protocol {
	case ProtocolHTTP, ProtocolGRPC, ProtocolTCP:
		if inst.Endpoint.Address == "" {
			return fmt.Errorf("registry: empty endpoint address for %s", inst.Endpoint.Protocol)
		}
		if inst.Endpoint.Port <= 0 {
			return fmt.Errorf("registry: invalid port %d for %s", inst.Endpoint.Port, inst.Endpoint.Protocol)
		}
	case ProtocolKafka:
		if inst.Endpoint.Topic == "" {
			return fmt.Errorf("registry: empty topic for kafka endpoint")
		}
	default:
		return fmt.Errorf("registry: unsupported protocol %q", inst.Endpoint.Protocol)
	}

	if inst.ID == "" {
		if inst.Endpoint.Protocol == ProtocolKafka {
			inst.ID = fmt.Sprintf("%s-%s", inst.Name, inst.Endpoint.Topic)
		} else {
			inst.ID = fmt.Sprintf("%s-%s-%d", inst.Name, inst.Endpoint.Address, inst.Endpoint.Port)
		}
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

	existing := r.instances[inst.Name]
	found := false
	for i, e := range existing {
		if e.ID == inst.ID {
			existing[i] = inst
			r.instances[inst.Name] = existing
			found = true
			break
		}
	}

	eps := r.services[inst.Name]
	ep := &Endpoint{
		NodeID:        inst.Endpoint.NodeID,
		Address:       inst.Endpoint.Address,
		Port:          inst.Endpoint.Port,
		Protocol:      inst.Endpoint.Protocol,
		Healthy:       true,
		Topic:         inst.Endpoint.Topic,
		ReplyTopic:    inst.Endpoint.ReplyTopic,
		ConsumerGroup: inst.Endpoint.ConsumerGroup,
	}
	epExists := false
	for i, e := range eps {
		if e.NodeID == ep.NodeID && e.Address == ep.Address && e.Port == ep.Port {
			eps[i] = ep
			r.services[inst.Name] = eps
			epExists = true
			break
		}
	}
	if !epExists {
		r.services[inst.Name] = append(eps, ep)
	}

	if !found {
		r.instances[inst.Name] = append(existing, inst)
	}
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
