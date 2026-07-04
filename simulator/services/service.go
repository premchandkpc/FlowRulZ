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
	}{
		// ═══════════════════════════════════════════════════════════
		// Customer Domain
		// ═══════════════════════════════════════════════════════════
		{"customer", 5 * time.Millisecond, 2 * time.Millisecond, 0.005, 100, []MethodInfo{{Name: "create"}, {Name: "get"}, {Name: "update"}, {Name: "delete"}, {Name: "search"}}},
		{"address", 3 * time.Millisecond, 1 * time.Millisecond, 0.003, 100, []MethodInfo{{Name: "create"}, {Name: "update"}, {Name: "delete"}, {Name: "validate"}}},
		{"profile", 4 * time.Millisecond, 2 * time.Millisecond, 0.004, 80, []MethodInfo{{Name: "get"}, {Name: "update"}, {Name: "preferences"}}},
		{"identity", 8 * time.Millisecond, 3 * time.Millisecond, 0.01, 50, []MethodInfo{{Name: "verify"}, {Name: "create"}, {Name: "revoke"}}},
		{"authentication", 10 * time.Millisecond, 4 * time.Millisecond, 0.015, 60, []MethodInfo{{Name: "login"}, {Name: "logout"}, {Name: "refresh"}, {Name: "mfa"}}},
		{"authorization", 6 * time.Millisecond, 2 * time.Millisecond, 0.008, 80, []MethodInfo{{Name: "check"}, {Name: "grant"}, {Name: "revoke"}, {Name: "roles"}}},
		{"wishlist", 4 * time.Millisecond, 2 * time.Millisecond, 0.005, 60, []MethodInfo{{Name: "add"}, {Name: "remove"}, {Name: "list"}}},
		{"review", 6 * time.Millisecond, 3 * time.Millisecond, 0.01, 50, []MethodInfo{{Name: "create"}, {Name: "update"}, {Name: "delete"}, {Name: "moderate"}}},
		{"support", 8 * time.Millisecond, 4 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "ticket"}, {Name: "escalate"}, {Name: "resolve"}}},
		{"chat", 2 * time.Millisecond, 1 * time.Millisecond, 0.01, 200, []MethodInfo{{Name: "send"}, {Name: "history"}, {Name: "typing"}}},

		// ═══════════════════════════════════════════════════════════
		// Catalog Domain
		// ═══════════════════════════════════════════════════════════
		{"catalog", 8 * time.Millisecond, 3 * time.Millisecond, 0.008, 100, []MethodInfo{{Name: "search"}, {Name: "get"}, {Name: "browse"}, {Name: "filter"}}},
		{"search", 12 * time.Millisecond, 5 * time.Millisecond, 0.015, 60, []MethodInfo{{Name: "query"}, {Name: "suggest"}, {Name: "autocomplete"}, {Name: "index"}}},
		{"recommendation", 15 * time.Millisecond, 6 * time.Millisecond, 0.02, 40, []MethodInfo{{Name: "get"}, {Name: "similar"}, {Name: "trending"}, {Name: "personalize"}}},
		{"inventory", 8 * time.Millisecond, 4 * time.Millisecond, 0.015, 100, []MethodInfo{{Name: "reserve"}, {Name: "release"}, {Name: "check"}, {Name: "batch"}}},
		{"pricing", 6 * time.Millisecond, 3 * time.Millisecond, 0.01, 80, []MethodInfo{{Name: "calculate"}, {Name: "discount"}, {Name: "bulk"}, {Name: "dynamic"}}},
		{"promotion", 5 * time.Millisecond, 2 * time.Millisecond, 0.008, 60, []MethodInfo{{Name: "apply"}, {Name: "validate"}, {Name: "stack"}}},
		{"coupon", 4 * time.Millisecond, 2 * time.Millisecond, 0.005, 80, []MethodInfo{{Name: "validate"}, {Name: "redeem"}, {Name: "create"}}},
		{"tax", 10 * time.Millisecond, 4 * time.Millisecond, 0.012, 50, []MethodInfo{{Name: "calculate"}, {Name: "validate"}, {Name: "jurisdiction"}}},

		// ═══════════════════════════════════════════════════════════
		// Order Domain
		// ═══════════════════════════════════════════════════════════
		{"order", 10 * time.Millisecond, 4 * time.Millisecond, 0.012, 80, []MethodInfo{{Name: "create"}, {Name: "cancel"}, {Name: "status"}, {Name: "history"}, {Name: "update"}}},
		{"payment", 40 * time.Millisecond, 15 * time.Millisecond, 0.03, 30, []MethodInfo{{Name: "authorize"}, {Name: "capture"}, {Name: "refund"}, {Name: "void"}, {Name: "status"}}},
		{"fraud", 20 * time.Millisecond, 8 * time.Millisecond, 0.02, 40, []MethodInfo{{Name: "check"}, {Name: "score"}, {Name: "flag"}, {Name: "review"}}},
		{"loyalty", 10 * time.Millisecond, 3 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "award"}, {Name: "redeem"}}},
		{"invoice", 12 * time.Millisecond, 4 * time.Millisecond, 0.01, 60, []MethodInfo{{Name: "generate"}, {Name: "send"}, {Name: "void"}, {Name: "pdf"}}},
		{"billing", 15 * time.Millisecond, 5 * time.Millisecond, 0.015, 50, []MethodInfo{{Name: "charge"}, {Name: "credit"}, {Name: "statement"}, {Name: "balance"}}},
		{"refund", 25 * time.Millisecond, 10 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "process"}, {Name: "status"}, {Name: "approve"}, {Name: "reject"}}},

		// ═══════════════════════════════════════════════════════════
		// Shipping Domain
		// ═══════════════════════════════════════════════════════════
		{"shipping", 15 * time.Millisecond, 6 * time.Millisecond, 0.018, 50, []MethodInfo{{Name: "schedule"}, {Name: "track"}, {Name: "cancel"}, {Name: "rate"}}},
		{"courier", 20 * time.Millisecond, 8 * time.Millisecond, 0.025, 30, []MethodInfo{{Name: "dispatch"}, {Name: "track"}, {Name: "proof"}}},
		{"warehouse", 12 * time.Millisecond, 5 * time.Millisecond, 0.015, 40, []MethodInfo{{Name: "allocate"}, {Name: "pick"}, {Name: "pack"}, {Name: "ship"}}},

		// ═══════════════════════════════════════════════════════════
		// Notification Domain
		// ═══════════════════════════════════════════════════════════
		{"notification", 3 * time.Millisecond, 1 * time.Millisecond, 0.005, 500, []MethodInfo{{Name: "email"}, {Name: "sms"}, {Name: "push"}, {Name: "webhook"}}},
		{"email", 10 * time.Millisecond, 4 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "send"}, {Name: "template"}, {Name: "track"}, {Name: "bounce"}}},
		{"sms", 8 * time.Millisecond, 3 * time.Millisecond, 0.012, 200, []MethodInfo{{Name: "send"}, {Name: "status"}, {Name: "verify"}}},
		{"push", 5 * time.Millisecond, 2 * time.Millisecond, 0.008, 300, []MethodInfo{{Name: "send"}, {Name: "badge"}, {Name: "silent"}}},
		{"webhook", 6 * time.Millisecond, 3 * time.Millisecond, 0.01, 150, []MethodInfo{{Name: "deliver"}, {Name: "retry"}, {Name: "status"}}},

		// ═══════════════════════════════════════════════════════════
		// Analytics Domain
		// ═══════════════════════════════════════════════════════════
		{"analytics", 15 * time.Millisecond, 6 * time.Millisecond, 0.015, 40, []MethodInfo{{Name: "track"}, {Name: "query"}, {Name: "aggregate"}, {Name: "export"}}},
		{"audit", 8 * time.Millisecond, 3 * time.Millisecond, 0.008, 60, []MethodInfo{{Name: "log"}, {Name: "query"}, {Name: "export"}}},
		{"reporting", 20 * time.Millisecond, 8 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "generate"}, {Name: "schedule"}, {Name: "download"}}},

		// ═══════════════════════════════════════════════════════════
		// AI Domain
		// ═══════════════════════════════════════════════════════════
		{"ai", 50 * time.Millisecond, 20 * time.Millisecond, 0.03, 20, []MethodInfo{{Name: "predict"}, {Name: "classify"}, {Name: "generate"}}},
		{"llm", 100 * time.Millisecond, 40 * time.Millisecond, 0.05, 10, []MethodInfo{{Name: "complete"}, {Name: "chat"}, {Name: "embed"}}},
		{"ocr", 30 * time.Millisecond, 12 * time.Millisecond, 0.02, 20, []MethodInfo{{Name: "extract"}, {Name: "verify"}}},
		{"image", 25 * time.Millisecond, 10 * time.Millisecond, 0.018, 25, []MethodInfo{{Name: "resize"}, {Name: "crop"}, {Name: "analyze"}}},
		{"translation", 20 * time.Millisecond, 8 * time.Millisecond, 0.015, 30, []MethodInfo{{Name: "translate"}, {Name: "detect"}, {Name: "batch"}}},

		// ═══════════════════════════════════════════════════════════
		// Utility Domain
		// ═══════════════════════════════════════════════════════════
		{"currency", 5 * time.Millisecond, 2 * time.Millisecond, 0.008, 80, []MethodInfo{{Name: "convert"}, {Name: "rate"}, {Name: "history"}}},
		{"geo", 8 * time.Millisecond, 3 * time.Millisecond, 0.01, 60, []MethodInfo{{Name: "geocode"}, {Name: "reverse"}, {Name: "distance"}, {Name: "timezone"}}},
		{"weather", 12 * time.Millisecond, 5 * time.Millisecond, 0.015, 40, []MethodInfo{{Name: "current"}, {Name: "forecast"}, {Name: "alerts"}}},
		{"document", 10 * time.Millisecond, 4 * time.Millisecond, 0.012, 50, []MethodInfo{{Name: "create"}, {Name: "render"}, {Name: "sign"}, {Name: "archive"}}},

		// ═══════════════════════════════════════════════════════════
		// Platform Services
		// ═══════════════════════════════════════════════════════════
		{"validate", 0, 0, 0.0, 1000, []MethodInfo{{Name: "check"}}},
		{"metadata-server", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 20, []MethodInfo{{Name: "get"}, {Name: "put"}, {Name: "watch"}}},
		{"metrics-server", 1 * time.Millisecond, 500 * time.Microsecond, 0.001, 15, []MethodInfo{{Name: "record"}, {Name: "query"}}},
		{"trace-viewer", 3 * time.Millisecond, 1 * time.Millisecond, 0.001, 10, []MethodInfo{{Name: "get"}, {Name: "export"}}},
		{"log-viewer", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 25, []MethodInfo{{Name: "write"}, {Name: "read"}}},

		// ═══════════════════════════════════════════════════════════
		// Infrastructure
		// ═══════════════════════════════════════════════════════════
		{"database", 2 * time.Millisecond, 1 * time.Millisecond, 0.001, 50, []MethodInfo{{Name: "query"}, {Name: "execute"}, {Name: "transaction"}}},
		{"redis", 1 * time.Millisecond, 500 * time.Microsecond, 0.001, 100, []MethodInfo{{Name: "get"}, {Name: "set"}, {Name: "del"}, {Name: "ttl"}}},
		{"kafka", 5 * time.Millisecond, 2 * time.Millisecond, 0.001, 30, []MethodInfo{{Name: "publish"}, {Name: "consume"}, {Name: "offset"}}},
		{"payment-gateway", 50 * time.Millisecond, 20 * time.Millisecond, 0.05, 10, []MethodInfo{{Name: "authorize"}, {Name: "capture"}, {Name: "refund"}}},
		{"email-provider", 10 * time.Millisecond, 3 * time.Millisecond, 0.01, 100, []MethodInfo{{Name: "send"}, {Name: "bounce"}, {Name: "complaint"}}},
		{"sms-gateway", 8 * time.Millisecond, 2 * time.Millisecond, 0.01, 200, []MethodInfo{{Name: "send"}, {Name: "status"}}},
		{"warehouse-api", 12 * time.Millisecond, 4 * time.Millisecond, 0.02, 30, []MethodInfo{{Name: "stock"}, {Name: "allocate"}, {Name: "ship"}}},
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
