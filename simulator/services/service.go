package services

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type MethodInfo struct {
	Name string `json:"name"`
}

type MockService struct {
	Name          string
	Methods       []MethodInfo
	BaseLatency   time.Duration
	LatencyJitter time.Duration
	FailureRate   float64
	MaxConcurrent int

	mu       sync.Mutex
	running  int

}



type CallResult struct {
	Body    []byte
	Latency time.Duration
	Error   error
}

type ServiceRegistry struct {
	mu       sync.RWMutex
	services map[string]*MockService
}

func NewRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		services: make(map[string]*MockService),
	}
}

func (r *ServiceRegistry) Register(svc *MockService) {
	r.mu.Lock()
	r.services[svc.Name] = svc
	r.mu.Unlock()
}

func (r *ServiceRegistry) Get(name string) *MockService {
	r.mu.RLock()
	svc := r.services[name]
	r.mu.RUnlock()
	return svc
}

func (r *ServiceRegistry) All() []*MockService {
	r.mu.RLock()
	svcs := make([]*MockService, 0, len(r.services))
	for _, s := range r.services {
		svcs = append(svcs, s)
	}
	r.mu.RUnlock()
	return svcs
}

func (r *ServiceRegistry) Names() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.services))
	for n := range r.services {
		names = append(names, n)
	}
	r.mu.RUnlock()
	return names
}

func (s *MockService) Call(ctx context.Context, body []byte) CallResult {
	start := time.Now()

	s.mu.Lock()
	if s.MaxConcurrent > 0 && s.running >= s.MaxConcurrent {
		s.mu.Unlock()
		return CallResult{
			Body:    nil,
			Latency: time.Since(start),
			Error:   context.DeadlineExceeded,
		}
	}
	s.running++
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running--
		s.mu.Unlock()
	}()

	latency := s.BaseLatency
	if s.LatencyJitter > 0 {
		jitter := time.Duration(rand.Int63n(int64(s.LatencyJitter)))
		latency += jitter
	}

	select {
	case <-time.After(latency):
	case <-ctx.Done():
		return CallResult{Latency: time.Since(start), Error: ctx.Err()}
	}

	if s.FailureRate > 0 && rand.Float64() < s.FailureRate {
		return CallResult{
			Body:      nil,
			Latency:   time.Since(start),
			Error:     context.DeadlineExceeded,
		}
	}

	resp := []byte(`{"status":"ok"}`)
	return CallResult{
		Body:    resp,
		Latency: time.Since(start),
		Error:   nil,
	}
}

func DefaultServices() *ServiceRegistry {
	r := NewRegistry()
	defs := []struct {
		name        string
		latency     time.Duration
		jitter      time.Duration
		failureRate float64
		concurrency int
	}{
		{"validate", 0, 0, 0.0, 1000},
		{"inventory", 8 * time.Millisecond, 4 * time.Millisecond, 0.01, 100},
		{"fraud", 15 * time.Millisecond, 5 * time.Millisecond, 0.02, 50},
		{"payment", 40 * time.Millisecond, 10 * time.Millisecond, 0.03, 20},
		{"email", 5 * time.Millisecond, 2 * time.Millisecond, 0.005, 200},
		{"loyalty", 10 * time.Millisecond, 3 * time.Millisecond, 0.01, 100},
		{"invoice", 12 * time.Millisecond, 3 * time.Millisecond, 0.01, 80},
		{"shipping", 20 * time.Millisecond, 5 * time.Millisecond, 0.02, 40},
		{"notification", 3 * time.Millisecond, 1 * time.Millisecond, 0.005, 500},
	}
	for _, d := range defs {
		methods := []MethodInfo{}
		switch d.name {
		case "payment":
			methods = []MethodInfo{{Name: "authorize"}, {Name: "capture"}, {Name: "refund"}}
		case "inventory":
			methods = []MethodInfo{{Name: "check"}, {Name: "reserve"}, {Name: "release"}}
		case "fraud":
			methods = []MethodInfo{{Name: "check"}}
		}
		r.Register(&MockService{
			Name:          d.name,
			Methods:       methods,
			BaseLatency:   d.latency,
			LatencyJitter: d.jitter,
			FailureRate:   d.failureRate,
			MaxConcurrent: d.concurrency,
	
		})
	}
	return r
}
