package registry

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

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
