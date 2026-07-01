package registry

import (
	"fmt"
	"time"
)

func (r *ServiceRegistry) SetHeartbeatTimeout(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hbTimeout = d
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
