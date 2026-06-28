package registry

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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

type Endpoint struct {
	NodeID   string   `json:"node_id"`
	Address  string   `json:"address"`
	Port     int      `json:"port"`
	Protocol Protocol `json:"protocol"`
	Healthy  bool     `json:"healthy"`
	Load     float64  `json:"load,omitempty"`

	nodeID string
}

type ServiceInfo struct {
	Name      string      `json:"name"`
	Endpoints []*Endpoint `json:"endpoints"`
}

type ServiceRegistry struct {
	mu        sync.RWMutex
	services  map[string][]*Endpoint
	roundRobin map[string]*uint64 // per-service round-robin counter
	defStrategy LBStrategy
}

func New() *ServiceRegistry {
	return &ServiceRegistry{
		services:    make(map[string][]*Endpoint),
		roundRobin:  make(map[string]*uint64),
		defStrategy: LBStrategyRandom,
	}
}

func NewWithStrategy(strategy LBStrategy) *ServiceRegistry {
	r := New()
	r.defStrategy = strategy
	return r
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

func (r *ServiceRegistry) MarkUnhealthy(name string, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	eps := r.services[name]
	for _, ep := range eps {
		if ep.NodeID == nodeID {
			ep.Healthy = false
		}
	}
}

func (r *ServiceRegistry) MarkHealthy(name string, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	eps := r.services[name]
	for _, ep := range eps {
		if ep.NodeID == nodeID {
			ep.Healthy = true
		}
	}
}

func (r *ServiceRegistry) ListServices() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.services))
	for name := range r.services {
		names = append(names, name)
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

var localNodeIDValue atomic.Value

func init() {
	localNodeIDValue.Store("")
}

func SetLocalNodeID(id string) {
	localNodeIDValue.Store(id)
}

func localNodeID() string {
	v, _ := localNodeIDValue.Load().(string)
	return v
}
