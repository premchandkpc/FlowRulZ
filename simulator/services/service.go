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
	Domain        string

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

func (r *ServiceRegistry) ByDomain(domain string) []*MockService {
	r.mu.RLock()
	svcs := make([]*MockService, 0)
	for _, s := range r.services {
		if s.Domain == domain {
			svcs = append(svcs, s)
		}
	}
	r.mu.RUnlock()
	return svcs
}

func (r *ServiceRegistry) Domains() []string {
	r.mu.RLock()
	seen := make(map[string]bool)
	domains := make([]string, 0)
	for _, s := range r.services {
		if s.Domain != "" && !seen[s.Domain] {
			seen[s.Domain] = true
			domains = append(domains, s.Domain)
		}
	}
	r.mu.RUnlock()
	return domains
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
			Body:    nil,
			Latency: time.Since(start),
			Error:   context.DeadlineExceeded,
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
		domain      string
	}{
		// ═══════════════════════════════════════════════════════════
		// Customer Domain
		// ═══════════════════════════════════════════════════════════
		{"customer", 5 * time.Millisecond, 2 * time.Millisecond, 0.005, 100, []MethodInfo{{Name: "create"}, {Name: "get"}, {Name: "update"}, {Name: "delete"}, {Name: "search"}}, "customer"},
		{"address", 3 * time.Millisecond, 1 * time.Millisecond, 0.003, 100, []MethodInfo{{Name: "create"}, {Name: "update"}, {Name: "delete"}, {Name: "validate"}}, "customer"},
		{"profile", 4 * time.Millisecond, 2 * time.Millisecond, 0.004, 80, []MethodInfo{{Name: "get"}, {Name: "update"}, {Name: "preferences"}}, "customer"},
		{"identity", 8 * time.Millisecond, 3 * time.Millisecond, 0.01, 50, []MethodInfo{{Name: "verify"}, {Name: "create"}, {Name: "revoke"}}, "customer"},
		{"authentication", 10 * time.Millisecond, 4 * time.Millisecond, 0.015, 60, []MethodInfo{{Name: "login"}, {Name: "logout"}, {Name: "refresh"}, {Name: "mfa"}}, "customer"},
		{"authorization", 6 * time.Millisecond, 2 * time.Millisecond, 0.008, 80, []MethodInfo{{Name: "check"}, {Name: "grant"}, {Name: "revoke"}, {Name: "roles"}}, "customer"},
		{"wishlist", 4 * time.Millisecond, 2 * time.Millisecond, 0.005, 60, []MethodInfo{{Name: "add"}, {Name: "remove"}, {Name: "list"}}, "customer"},
		{"review", 6 * time.Millisecond, 3 * time.Millisecond, 0.01, 50, []MethodInfo{{Name: "create"}, {Name: "update"}, {Name: "delete"}, {Name: "moderate"}}, "customer"},
		{"support", 8 * time.Millisecond, 4 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "ticket"}, {Name: "escalate"}, {Name: "resolve"}}, "customer"},
		{"chat", 2 * time.Millisecond, 1 * time.Millisecond, 0.01, 200, []MethodInfo{{Name: "send"}, {Name: "history"}, {Name: "typing"}}, "customer"},

		// ═══════════════════════════════════════════════════════════
		// Catalog Domain
		// ═══════════════════════════════════════════════════════════
		{"catalog", 8 * time.Millisecond, 3 * time.Millisecond, 0.008, 100, []MethodInfo{{Name: "search"}, {Name: "get"}, {Name: "browse"}, {Name: "filter"}}, "catalog"},
		{"search", 12 * time.Millisecond, 5 * time.Millisecond, 0.015, 60, []MethodInfo{{Name: "query"}, {Name: "suggest"}, {Name: "autocomplete"}, {Name: "index"}}, "catalog"},
		{"recommendation", 15 * time.Millisecond, 6 * time.Millisecond, 0.02, 40, []MethodInfo{{Name: "get"}, {Name: "similar"}, {Name: "trending"}, {Name: "personalize"}}, "catalog"},
		{"inventory", 8 * time.Millisecond, 4 * time.Millisecond, 0.015, 100, []MethodInfo{{Name: "reserve"}, {Name: "release"}, {Name: "check"}, {Name: "batch"}}, "catalog"},
		{"pricing", 6 * time.Millisecond, 3 * time.Millisecond, 0.01, 80, []MethodInfo{{Name: "calculate"}, {Name: "discount"}, {Name: "bulk"}, {Name: "dynamic"}}, "catalog"},
		{"promotion", 5 * time.Millisecond, 2 * time.Millisecond, 0.008, 60, []MethodInfo{{Name: "apply"}, {Name: "validate"}, {Name: "stack"}}, "catalog"},
		{"coupon", 4 * time.Millisecond, 2 * time.Millisecond, 0.005, 80, []MethodInfo{{Name: "validate"}, {Name: "redeem"}, {Name: "create"}}, "catalog"},
		{"tax", 10 * time.Millisecond, 4 * time.Millisecond, 0.012, 50, []MethodInfo{{Name: "calculate"}, {Name: "validate"}, {Name: "jurisdiction"}}, "catalog"},

		// ═══════════════════════════════════════════════════════════
		// Order Domain
		// ═══════════════════════════════════════════════════════════
		{"order", 10 * time.Millisecond, 4 * time.Millisecond, 0.012, 80, []MethodInfo{{Name: "create"}, {Name: "cancel"}, {Name: "status"}, {Name: "history"}, {Name: "update"}}, "order"},
		{"payment", 40 * time.Millisecond, 15 * time.Millisecond, 0.03, 30, []MethodInfo{{Name: "authorize"}, {Name: "capture"}, {Name: "refund"}, {Name: "void"}, {Name: "status"}}, "order"},
		{"fraud", 20 * time.Millisecond, 8 * time.Millisecond, 0.02, 40, []MethodInfo{{Name: "check"}, {Name: "score"}, {Name: "flag"}, {Name: "review"}}, "order"},
		{"invoice", 12 * time.Millisecond, 4 * time.Millisecond, 0.01, 60, []MethodInfo{{Name: "generate"}, {Name: "send"}, {Name: "void"}, {Name: "pdf"}}, "order"},
		{"billing", 15 * time.Millisecond, 5 * time.Millisecond, 0.015, 50, []MethodInfo{{Name: "charge"}, {Name: "credit"}, {Name: "statement"}, {Name: "balance"}}, "order"},
		{"refund", 25 * time.Millisecond, 10 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "process"}, {Name: "status"}, {Name: "approve"}, {Name: "reject"}}, "order"},

		// ═══════════════════════════════════════════════════════════
		// Shipping Domain
		// ═══════════════════════════════════════════════════════════
		{"shipping", 15 * time.Millisecond, 6 * time.Millisecond, 0.018, 50, []MethodInfo{{Name: "schedule"}, {Name: "track"}, {Name: "cancel"}, {Name: "rate"}}, "shipping"},
		{"courier", 20 * time.Millisecond, 8 * time.Millisecond, 0.025, 30, []MethodInfo{{Name: "dispatch"}, {Name: "track"}, {Name: "proof"}}, "shipping"},
		{"warehouse", 12 * time.Millisecond, 5 * time.Millisecond, 0.015, 40, []MethodInfo{{Name: "allocate"}, {Name: "pick"}, {Name: "pack"}, {Name: "ship"}}, "shipping"},

		// ═══════════════════════════════════════════════════════════
		// Notification Domain
		// ═══════════════════════════════════════════════════════════
		{"notification", 3 * time.Millisecond, 1 * time.Millisecond, 0.005, 500, []MethodInfo{{Name: "email"}, {Name: "sms"}, {Name: "push"}, {Name: "webhook"}}, "notification"},
		{"email", 10 * time.Millisecond, 4 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "send"}, {Name: "template"}, {Name: "track"}, {Name: "bounce"}}, "notification"},
		{"sms", 8 * time.Millisecond, 3 * time.Millisecond, 0.012, 200, []MethodInfo{{Name: "send"}, {Name: "status"}, {Name: "verify"}}, "notification"},
		{"push", 5 * time.Millisecond, 2 * time.Millisecond, 0.008, 300, []MethodInfo{{Name: "send"}, {Name: "badge"}, {Name: "silent"}}, "notification"},
		{"webhook", 6 * time.Millisecond, 3 * time.Millisecond, 0.01, 150, []MethodInfo{{Name: "deliver"}, {Name: "retry"}, {Name: "status"}}, "notification"},

		// ═══════════════════════════════════════════════════════════
		// Analytics Domain
		// ═══════════════════════════════════════════════════════════
		{"analytics", 15 * time.Millisecond, 6 * time.Millisecond, 0.015, 40, []MethodInfo{{Name: "track"}, {Name: "query"}, {Name: "aggregate"}, {Name: "export"}}, "analytics"},
		{"audit", 8 * time.Millisecond, 3 * time.Millisecond, 0.008, 60, []MethodInfo{{Name: "log"}, {Name: "query"}, {Name: "export"}}, "analytics"},
		{"reporting", 20 * time.Millisecond, 8 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "generate"}, {Name: "schedule"}, {Name: "download"}}, "analytics"},

		// ═══════════════════════════════════════════════════════════
		// AI Domain
		// ═══════════════════════════════════════════════════════════
		{"ai", 50 * time.Millisecond, 20 * time.Millisecond, 0.03, 20, []MethodInfo{{Name: "predict"}, {Name: "classify"}, {Name: "generate"}}, "ai"},
		{"llm", 100 * time.Millisecond, 40 * time.Millisecond, 0.05, 10, []MethodInfo{{Name: "complete"}, {Name: "chat"}, {Name: "embed"}}, "ai"},
		{"ocr", 30 * time.Millisecond, 12 * time.Millisecond, 0.02, 20, []MethodInfo{{Name: "extract"}, {Name: "verify"}}, "ai"},
		{"image", 25 * time.Millisecond, 10 * time.Millisecond, 0.018, 25, []MethodInfo{{Name: "resize"}, {Name: "crop"}, {Name: "analyze"}}, "ai"},
		{"translation", 20 * time.Millisecond, 8 * time.Millisecond, 0.015, 30, []MethodInfo{{Name: "translate"}, {Name: "detect"}, {Name: "batch"}}, "ai"},

		// ═══════════════════════════════════════════════════════════
		// Utility Domain
		// ═══════════════════════════════════════════════════════════
		{"currency", 5 * time.Millisecond, 2 * time.Millisecond, 0.008, 80, []MethodInfo{{Name: "convert"}, {Name: "rate"}, {Name: "history"}}, "utility"},
		{"geo", 8 * time.Millisecond, 3 * time.Millisecond, 0.01, 60, []MethodInfo{{Name: "geocode"}, {Name: "reverse"}, {Name: "distance"}, {Name: "timezone"}}, "utility"},
		{"weather", 12 * time.Millisecond, 5 * time.Millisecond, 0.015, 40, []MethodInfo{{Name: "current"}, {Name: "forecast"}, {Name: "alerts"}}, "utility"},
		{"document", 10 * time.Millisecond, 4 * time.Millisecond, 0.012, 50, []MethodInfo{{Name: "create"}, {Name: "render"}, {Name: "sign"}, {Name: "archive"}}, "utility"},

		// ═══════════════════════════════════════════════════════════
		// Platform Services
		// ═══════════════════════════════════════════════════════════
		{"validate", 0, 0, 0.0, 1000, []MethodInfo{{Name: "check"}}, "platform"},
		{"metadata-server", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 20, []MethodInfo{{Name: "get"}, {Name: "put"}, {Name: "watch"}}, "platform"},
		{"metrics-server", 1 * time.Millisecond, 500 * time.Microsecond, 0.001, 15, []MethodInfo{{Name: "record"}, {Name: "query"}}, "platform"},
		{"trace-viewer", 3 * time.Millisecond, 1 * time.Millisecond, 0.001, 10, []MethodInfo{{Name: "get"}, {Name: "export"}}, "platform"},
		{"log-viewer", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 25, []MethodInfo{{Name: "write"}, {Name: "read"}}, "platform"},

		// ═══════════════════════════════════════════════════════════
		// Infrastructure
		// ═══════════════════════════════════════════════════════════
		{"database", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 50, []MethodInfo{{Name: "query"}, {Name: "execute"}, {Name: "transaction"}}, "infrastructure"},
		{"redis", 1 * time.Millisecond, 500 * time.Microsecond, 0.001, 100, []MethodInfo{{Name: "get"}, {Name: "set"}, {Name: "del"}, {Name: "ttl"}}, "infrastructure"},
		{"kafka", 5 * time.Millisecond, 2 * time.Millisecond, 0.001, 30, []MethodInfo{{Name: "publish"}, {Name: "consume"}, {Name: "offset"}}, "infrastructure"},
		{"payment-gateway", 50 * time.Millisecond, 20 * time.Millisecond, 0.05, 10, []MethodInfo{{Name: "authorize"}, {Name: "capture"}, {Name: "refund"}}, "infrastructure"},
		{"email-provider", 10 * time.Millisecond, 3 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "send"}, {Name: "bounce"}, {Name: "complaint"}}, "infrastructure"},
		{"sms-gateway", 8 * time.Millisecond, 2 * time.Millisecond, 0.01, 200, []MethodInfo{{Name: "send"}, {Name: "status"}}, "infrastructure"},
		{"warehouse-api", 12 * time.Millisecond, 4 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "stock"}, {Name: "allocate"}, {Name: "ship"}}, "infrastructure"},
	}

	for _, d := range defs {
		r.Register(&MockService{
			Name:          d.name,
			Methods:       d.methods,
			BaseLatency:   d.latency,
			LatencyJitter: d.jitter,
			FailureRate:   d.failureRate,
			MaxConcurrent: d.concurrency,
			Domain:        d.domain,
		})
	}
	return r
}

// SimpleServices returns only 4 core services for Mode 1 (Simple).
func SimpleServices() *ServiceRegistry {
	r := NewRegistry()
	simple := []struct {
		name    string
		latency time.Duration
		methods []MethodInfo
	}{
		{"order", 5 * time.Millisecond, []MethodInfo{{Name: "create"}, {Name: "cancel"}, {Name: "status"}}},
		{"payment", 40 * time.Millisecond, []MethodInfo{{Name: "authorize"}, {Name: "capture"}, {Name: "refund"}}},
		{"inventory", 8 * time.Millisecond, []MethodInfo{{Name: "reserve"}, {Name: "release"}, {Name: "check"}}},
		{"notification", 3 * time.Millisecond, []MethodInfo{{Name: "email"}, {Name: "sms"}, {Name: "push"}}},
	}
	for _, d := range simple {
		r.Register(&MockService{
			Name:          d.name,
			Methods:       d.methods,
			BaseLatency:   d.latency,
			MaxConcurrent: 100,
			Domain:        "simple",
		})
	}
	return r
}

// EnterpriseServices returns all 40+ services for Mode 2 (Enterprise).
func EnterpriseServices() *ServiceRegistry {
	return DefaultServices()
}

// ChaosServices returns services with high failure rates for Mode 3 (Chaos).
func ChaosServices() *ServiceRegistry {
	r := DefaultServices()
	for _, svc := range r.All() {
		svc.FailureRate = 0.1 + rand.Float64()*0.3
		svc.BaseLatency *= 3
	}
	return r
}

// PerformanceServices returns services optimized for high throughput.
func PerformanceServices() *ServiceRegistry {
	r := DefaultServices()
	for _, svc := range r.All() {
		svc.FailureRate = 0.001
		svc.BaseLatency = time.Millisecond
		svc.LatencyJitter = time.Millisecond
		svc.MaxConcurrent = 1000
	}
	return r
}
