package registry

import (
	"context"
	"fmt"
	"sync"

	pkgregistry "github.com/premchandkpc/FlowRulZ/server/pkg/registry"
)

var _ pkgregistry.Registry = (*Registry)(nil)

type Registry struct {
	inner *ServiceRegistry
	subsMu sync.RWMutex
	subs   map[string]chan pkgregistry.RegistryEvent
}

func NewRegistry() *Registry {
	return &Registry{
		inner: New(),
		subs:  make(map[string]chan pkgregistry.RegistryEvent),
	}
}

func (r *Registry) Register(ctx context.Context, svc *pkgregistry.ServiceRegistration) error {
	ep := &Endpoint{
		Address:  svc.Address,
		Protocol: ProtocolHTTP,
		Healthy:  true,
	}
	return r.inner.Register(svc.Name, ep)
}

func (r *Registry) Unregister(ctx context.Context, name string) error {
	r.inner.Unregister(name, "")
	return nil
}

func (r *Registry) Lookup(ctx context.Context, name string) (*pkgregistry.ServiceInstance, error) {
	inst, err := r.inner.LookupInstance(name, "")
	if err != nil {
		return nil, pkgregistry.ErrServiceNotFound
	}
	return &pkgregistry.ServiceInstance{
		ID:      pkgregistry.ServiceID(inst.ID),
		Name:    inst.Name,
		Address: fmt.Sprintf("%s:%d", inst.Endpoint.Address, inst.Endpoint.Port),
		IsAlive: inst.Healthy,
		Meta:    inst.Metadata,
	}, nil
}

func (r *Registry) LookupMultiple(ctx context.Context, names []string) ([]*pkgregistry.ServiceInstance, error) {
	var instances []*pkgregistry.ServiceInstance
	for _, name := range names {
		inst, err := r.Lookup(ctx, name)
		if err != nil {
			continue
		}
		instances = append(instances, inst)
	}
	if instances == nil {
		return []*pkgregistry.ServiceInstance{}, nil
	}
	return instances, nil
}

func (r *Registry) ListServices(ctx context.Context) ([]*pkgregistry.ServiceRegistration, error) {
	names := r.inner.ListServices()
	services := make([]*pkgregistry.ServiceRegistration, 0, len(names))
	for _, name := range names {
		info := r.inner.ServiceInfo(name)
		if info == nil {
			continue
		}
		methods := make([]pkgregistry.MethodSpec, len(info.Methods))
		for i, m := range info.Methods {
			methods[i] = pkgregistry.MethodSpec{
				Name:   m.Name,
				Input:  m.InputType,
				Output: m.OutputType,
			}
		}
		services = append(services, &pkgregistry.ServiceRegistration{
			Name:    name,
			Methods: methods,
		})
	}
	return services, nil
}

func (r *Registry) HealthCheck(ctx context.Context, name string) (bool, error) {
	inst, err := r.inner.LookupInstance(name, "")
	if err != nil {
		return false, pkgregistry.ErrServiceNotFound
	}
	return inst.Healthy, nil
}

func (r *Registry) SubscribeChanges(ctx context.Context, pattern string) (<-chan pkgregistry.RegistryEvent, error) {
	ch := make(chan pkgregistry.RegistryEvent, 64)
	r.subsMu.Lock()
	r.subs[pattern] = ch
	r.subsMu.Unlock()

	go func() {
		<-ctx.Done()
		r.subsMu.Lock()
		delete(r.subs, pattern)
		r.subsMu.Unlock()
		close(ch)
	}()

	return ch, nil
}
