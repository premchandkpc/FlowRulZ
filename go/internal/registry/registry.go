package registry

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Protocol string

const (
	ProtocolHTTP  Protocol = "http"
	ProtocolGRPC  Protocol = "grpc"
	ProtocolTCP   Protocol = "tcp"
)

type LBStrategy string

const (
	LBStrategyRandom       LBStrategy = "random"
	LBStrategyRoundRobin   LBStrategy = "roundrobin"
	LBStrategyLocalPrefer  LBStrategy = "localprefer"
	LBStrategyLeastLoaded  LBStrategy = "leastloaded"
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
	Name      string      `json:"name"`
	Methods   []MethodInfo `json:"methods,omitempty"`
	Instances []*ServiceInstance `json:"instances"`
	Endpoints []*Endpoint `json:"endpoints"`
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
		services:   make(map[string][]*Endpoint),
		instances:  make(map[string][]*ServiceInstance),
		roundRobin: make(map[string]*uint64),
		defStrategy: LBStrategyLeastLoaded,
		hbTimeout:  30 * time.Second,
	}
}

func (r *ServiceRegistry) SetHeartbeatTimeout(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hbTimeout = d
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

func (r *ServiceRegistry) Heartbeat(name, instanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	instances := r.instances[name]
	for _, inst := range instances {
		if inst.ID == instanceID {
			inst.HeartbeatAt = time.Now()
			inst.Healthy = true
			for _, ep := range r.services[name] {
				if ep.NodeID == inst.Endpoint.NodeID && ep.Address == inst.Endpoint.Address {
					ep.Healthy = true
					return nil
				}
			}
			return nil
		}
	}
	return fmt.Errorf("registry: instance %s/%s not found", name, instanceID)
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

func (r *ServiceRegistry) CheckExpired() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var expired []string
	now := time.Now()
	for name, instances := range r.instances {
		for _, inst := range instances {
			if now.Sub(inst.HeartbeatAt) > r.hbTimeout {
				inst.Healthy = false
				for _, ep := range r.services[name] {
					if ep.NodeID == inst.Endpoint.NodeID && ep.Address == inst.Endpoint.Address {
						ep.Healthy = false
					}
				}
				expired = append(expired, fmt.Sprintf("%s/%s", name, inst.ID))
			}
		}
	}
	return expired
}

func (r *ServiceRegistry) Lookup(name string) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	eps, ok := r.services[name]
	if !ok {
		return nil
	}
	healthy := make([]*Endpoint, 0, len(eps))
	for _, ep := range eps {
		if ep.Healthy {
			healthy = append(healthy, ep)
		}
	}
	return healthy
}

func (r *ServiceRegistry) LookupInstance(name, method string) (*ServiceInstance, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	instances, ok := r.instances[name]
	if !ok || len(instances) == 0 {
		return nil, fmt.Errorf("registry: service %q not found", name)
	}

	var candidates []*ServiceInstance
	for _, inst := range instances {
		if !inst.Healthy {
			continue
		}
		if time.Since(inst.HeartbeatAt) > r.hbTimeout {
			continue
		}
		if method != "" {
			for _, m := range inst.Methods {
				if m.Name == method {
					candidates = append(candidates, inst)
					break
				}
			}
		} else {
			candidates = append(candidates, inst)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("registry: no healthy instance of %q for method %q", name, method)
	}
	return r.pickInstance(candidates), nil
}

func (r *ServiceRegistry) Pick(name string) (*Endpoint, error) {
	return r.PickWithStrategy(name, r.defStrategy)
}

func (r *ServiceRegistry) PickWithStrategy(name string, strategy LBStrategy) (*Endpoint, error) {
	r.mu.RLock()
	eps, ok := r.services[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("registry: service %q not found", name)
	}

	healthy := make([]*Endpoint, 0, len(eps))
	for _, ep := range eps {
		if ep.Healthy {
			healthy = append(healthy, ep)
		}
	}
	if len(healthy) == 0 {
		return nil, fmt.Errorf("registry: no healthy endpoints for %q", name)
	}

	switch strategy {
	case LBStrategyRoundRobin:
		r.mu.Lock()
		counter, ok := r.roundRobin[name]
		if !ok {
			var zero uint64
			counter = &zero
			r.roundRobin[name] = counter
		}
		idx := atomic.AddUint64(counter, 1) % uint64(len(healthy))
		r.mu.Unlock()
		return healthy[idx], nil

	case LBStrategyLocalPrefer:
		for _, ep := range healthy {
			if ep.NodeID == "" || ep.NodeID == localNodeID() {
				return ep, nil
			}
		}
		return healthy[rand.Intn(len(healthy))], nil

	case LBStrategyLeastLoaded:
		selected := healthy[0]
		for _, ep := range healthy[1:] {
			if ep.Load < selected.Load {
				selected = ep
			}
		}
		return selected, nil

	default:
		return healthy[rand.Intn(len(healthy))], nil
	}
}

func (r *ServiceRegistry) pickInstance(candidates []*ServiceInstance) *ServiceInstance {
	switch r.defStrategy {
	case LBStrategyLeastLoaded:
		selected := candidates[0]
		for _, inst := range candidates[1:] {
			if inst.Endpoint.Load < selected.Endpoint.Load {
				selected = inst
			}
		}
		return selected
	case LBStrategyRoundRobin:
		r.mu.Lock()
		counter, ok := r.roundRobin[candidates[0].Name]
		if !ok {
			var zero uint64
			counter = &zero
			r.roundRobin[candidates[0].Name] = counter
		}
		idx := atomic.AddUint64(counter, 1) % uint64(len(candidates))
		r.mu.Unlock()
		return candidates[idx]
	case LBStrategyLocalPrefer:
		local := localNodeID()
		for _, inst := range candidates {
			if inst.Endpoint.NodeID == local {
				return inst
			}
		}
		return candidates[rand.Intn(len(candidates))]
	default:
		return candidates[rand.Intn(len(candidates))]
	}
}

func (r *ServiceRegistry) MarkUnhealthy(name string, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, ep := range r.services[name] {
		if ep.NodeID == nodeID {
			ep.Healthy = false
		}
	}
	for _, inst := range r.instances[name] {
		if inst.Endpoint.NodeID == nodeID {
			inst.Healthy = false
		}
	}
}

func (r *ServiceRegistry) MarkHealthy(name string, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, ep := range r.services[name] {
		if ep.NodeID == nodeID {
			ep.Healthy = true
		}
	}
	for _, inst := range r.instances[name] {
		if inst.Endpoint.NodeID == nodeID {
			inst.Healthy = true
		}
	}
}

func (r *ServiceRegistry) ListServices() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	for name := range r.instances {
		seen[name] = true
	}
	for name := range r.services {
		seen[name] = true
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	return names
}

func (r *ServiceRegistry) ListEndpoints(name string) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	eps, ok := r.services[name]
	if !ok {
		return nil
	}
	out := make([]*Endpoint, len(eps))
	copy(out, eps)
	return out
}

func (r *ServiceRegistry) ListInstances(name string) []*ServiceInstance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	instances, ok := r.instances[name]
	if !ok {
		return nil
	}
	out := make([]*ServiceInstance, len(instances))
	copy(out, instances)
	return out
}

func (r *ServiceRegistry) Snapshot() map[string][]*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snap := make(map[string][]*Endpoint, len(r.services))
	for name, eps := range r.services {
		out := make([]*Endpoint, len(eps))
		copy(out, eps)
		snap[name] = out
	}
	return snap
}

func (r *ServiceRegistry) ServiceInfo(name string) *ServiceInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	instances, ok := r.instances[name]
	if !ok {
		return nil
	}
	eps := r.services[name]
	var methods []MethodInfo
	if len(instances) > 0 {
		methods = instances[0].Methods
	}
	return &ServiceInfo{
		Name:      name,
		Methods:   methods,
		Instances: copyInstances(instances),
		Endpoints: copyEndpoints(eps),
	}
}

func (r *ServiceRegistry) AllServiceInfo() []*ServiceInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var out []*ServiceInfo
	for name, instances := range r.instances {
		var methods []MethodInfo
		if len(instances) > 0 {
			methods = instances[0].Methods
		}
		out = append(out, &ServiceInfo{
			Name:      name,
			Methods:   methods,
			Instances: copyInstances(instances),
			Endpoints: copyEndpoints(r.services[name]),
		})
	}
	return out
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
