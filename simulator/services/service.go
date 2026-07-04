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

	mu      sync.Mutex
	running int
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
		methods     []MethodInfo
	}{
		// Business Services
		{"validate", 0, 0, 0.0, 1000, []MethodInfo{{Name: "check"}}},
		{"order", 5 * time.Millisecond, 2 * time.Millisecond, 0.005, 100, []MethodInfo{{Name: "create"}, {Name: "cancel"}, {Name: "status"}}},
		{"payment", 40 * time.Millisecond, 10 * time.Millisecond, 0.03, 20, []MethodInfo{{Name: "validate"}, {Name: "authorize"}, {Name: "capture"}, {Name: "refund"}, {Name: "failure"}, {Name: "retry"}}},
		{"inventory", 8 * time.Millisecond, 4 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "reserve"}, {Name: "release"}, {Name: "lowstock"}, {Name: "warehouse"}}},
		{"shipping", 15 * time.Millisecond, 5 * time.Millisecond, 0.02, 50, []MethodInfo{{Name: "schedule"}, {Name: "track"}, {Name: "cancel"}}},
		{"notification", 3 * time.Millisecond, 1 * time.Millisecond, 0.005, 500, []MethodInfo{{Name: "email"}, {Name: "sms"}, {Name: "push"}, {Name: "webhook"}}},
		{"fraud", 15 * time.Millisecond, 5 * time.Millisecond, 0.02, 50, []MethodInfo{{Name: "check"}}},
		{"loyalty", 10 * time.Millisecond, 3 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "award"}, {Name: "redeem"}}},
		{"invoice", 12 * time.Millisecond, 3 * time.Millisecond, 0.01, 80, []MethodInfo{{Name: "generate"}, {Name: "send"}, {Name: "pay"}}},
		
		// Infrastructure Simulators
		{"database", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 50, []MethodInfo{{Name: "query"}, {Name: "execute"}, {Name: "transaction"}}},
		{"redis", 1 * time.Millisecond, 500 * time.Microsecond, 0.001, 100, []MethodInfo{{Name: "get"}, {Name: "set"}, {Name: "del"}}},
		{"kafka", 5 * time.Millisecond, 2 * time.Millisecond, 0.001, 30, []MethodInfo{{Name: "publish"}, {Name: "consume"}}},
		{"payment-gateway", 50 * time.Millisecond, 20 * time.Millisecond, 0.05, 10, []MethodInfo{{Name: "authorize"}, {Name: "capture"}, {Name: "refund"}}},
		{"email-provider", 10 * time.Millisecond, 3 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "send"}, {Name: "bounce"}, {Name: "complaint"}}},
		{"sms-gateway", 8 * time.Millisecond, 2 * time.Millisecond, 0.01, 200, []MethodInfo{{Name: "send"}, {Name: "status"}}},
		{"warehouse-api", 12 * time.Millisecond, 4 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "stock"}, {Name: "allocate"}, {Name: "ship"}}},
		{"metadata-server", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 20, []MethodInfo{{Name: "get"}, {Name: "put"}, {Name: "watch"}}},
		{"metrics-server", 1 * time.Millisecond, 500 * time.Microsecond, 0.001, 15, []MethodInfo{{Name: "record"}, {Name: "query"}}},
		{"trace-viewer", 3 * time.Millisecond, 1 * time.Millisecond, 0.001, 10, []MethodInfo{{Name: "get"}, {Name: "export"}}},
		{"log-viewer", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 25, []MethodInfo{{Name: "write"}, {Name: "read"}}},
	}
	for _, d := range defs {
		r.Register(&MockService{
			Name:          d.name,
			Methods:       d.methods,
			BaseLatency:   d.latency,
			LatencyJitter: d.jitter,
			FailureRate:   d.failureRate,
			MaxConcurrent: d.concurrency,
		})
	}
	return r
}
