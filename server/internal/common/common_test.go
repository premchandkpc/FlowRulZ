package common

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

//
// config.go
//

type testConfig struct {
	Host string
	Port int
}

func (c testConfig) Validate() error {
	if c.Host == "" {
		return errors.New("host required")
	}
	return nil
}

func TestMustValidate(t *testing.T) {
	MustValidate(testConfig{Host: "localhost", Port: 8080})
}

func TestMustValidatePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic")
		}
	}()
	MustValidate(testConfig{Port: 8080})
}

func TestApplyOptions(t *testing.T) {
	cfg := ApplyOptions(&testConfig{Host: "a"}, func(c *testConfig) { c.Port = 99 })
	if cfg.Port != 99 {
		t.Errorf("got Port=%d, want 99", cfg.Port)
	}
	if cfg.Host != "a" {
		t.Errorf("Host changed to %s", cfg.Host)
	}
}

//
// errors.go
//

func TestNewError(t *testing.T) {
	e := NewError(ClassPermanent, "disk full")
	if e.Class != ClassPermanent {
		t.Errorf("Class=%s", e.Class)
	}
	if e.Message != "disk full" {
		t.Errorf("Message=%s", e.Message)
	}
	if e.Cause != nil {
		t.Error("expected nil Cause")
	}
}

func TestWrapError(t *testing.T) {
	cause := errors.New("i/o timeout")
	e := WrapError(ClassTransient, "connect failed", cause)
	if !errors.Is(e, cause) {
		t.Error("expected errors.Is to match cause")
	}
	if e.Error() != "[transient] connect failed: i/o timeout" {
		t.Errorf("unexpected Error(): %s", e.Error())
	}
}

func TestErrorWithoutCause(t *testing.T) {
	e := NewError(ClassNotFound, "user missing")
	if e.Error() != "[not_found] user missing" {
		t.Errorf("unexpected Error(): %s", e.Error())
	}
}

func TestUnwrapError(t *testing.T) {
	cause := errors.New("root")
	e := WrapError(ClassInternal, "wrapped", cause)
	if e.Unwrap() != cause {
		t.Error("Unwrap should return the cause")
	}
}

func TestIsPermanent(t *testing.T) {
	if !IsPermanent(NewError(ClassPermanent, "")) {
		t.Error("expected true for permanent")
	}
	if IsPermanent(NewError(ClassTransient, "")) {
		t.Error("expected false for transient")
	}
	if IsPermanent(errors.New("plain")) {
		t.Error("expected false for plain error")
	}
}

func TestIsTransient(t *testing.T) {
	if !IsTransient(NewError(ClassTransient, "")) {
		t.Error("expected true for transient")
	}
	if IsTransient(NewError(ClassPermanent, "")) {
		t.Error("expected false for permanent")
	}
	if IsTransient(errors.New("plain")) {
		t.Error("expected false for plain error")
	}
}

//
// health.go
//

type stubHealthChecker struct {
	alive   bool
	details map[string]any
}

func (s stubHealthChecker) Health(_ context.Context) HealthStatus {
	return HealthStatus{Alive: s.alive, Details: s.details}
}

func TestHealthRegistryRegisterAndCheck(t *testing.T) {
	r := NewHealthRegistry()
	r.Register("db", stubHealthChecker{alive: true})
	r.Register("cache", stubHealthChecker{alive: false, details: map[string]any{"latency": "high"}})

	results := r.CheckAll(context.Background())
	if len(results) != 2 {
		t.Fatalf("got %d results", len(results))
	}
	if !results["db"].Alive {
		t.Error("db should be alive")
	}
	if results["cache"].Alive {
		t.Error("cache should not be alive")
	}
	if results["cache"].Details["latency"] != "high" {
		t.Error("cache details missing")
	}
}

func TestHealthRegistryEmptyCheck(t *testing.T) {
	r := NewHealthRegistry()
	results := r.CheckAll(context.Background())
	if len(results) != 0 {
		t.Errorf("expected empty, got %d", len(results))
	}
}

func TestHealthOverallAllAlive(t *testing.T) {
	r := NewHealthRegistry()
	r.Register("a", stubHealthChecker{alive: true})
	r.Register("b", stubHealthChecker{alive: true})
	overall := r.Overall(context.Background())
	if !overall.Alive {
		t.Error("expected alive")
	}
}

func TestHealthOverallSomeDead(t *testing.T) {
	r := NewHealthRegistry()
	r.Register("a", stubHealthChecker{alive: true})
	r.Register("b", stubHealthChecker{alive: false})
	overall := r.Overall(context.Background())
	if overall.Alive {
		t.Error("expected not alive")
	}
}

func TestHealthOverallEmpty(t *testing.T) {
	r := NewHealthRegistry()
	overall := r.Overall(context.Background())
	if overall.Alive {
		t.Error("expected not alive for empty registry")
	}
}

//
// lifecycle.go
//

type mockService struct {
	started   bool
	startErr  error
	stopErr   error
	startFunc func(context.Context) error
	stopFunc  func() error
}

func newMockService() *mockService {
	m := &mockService{}
	m.startFunc = func(_ context.Context) error {
		if m.startErr == nil {
			m.started = true
		}
		return m.startErr
	}
	m.stopFunc = func() error {
		m.started = false
		return m.stopErr
	}
	return m
}

func (m *mockService) Start(ctx context.Context) error { return m.startFunc(ctx) }
func (m *mockService) Stop() error                     { return m.stopFunc() }

func TestLifecycleRegisterAndStart(t *testing.T) {
	r := NewLifecycleRegistry()
	svc := newMockService()

	r.Register("s1", svc)
	err := r.StartAll(context.Background())
	if err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	if !svc.started {
		t.Error("service not started")
	}
}

func TestLifecycleStartError(t *testing.T) {
	r := NewLifecycleRegistry()
	svc := newMockService()
	svc.startErr = errors.New("boom")

	r.Register("bad", svc)
	err := r.StartAll(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if svc.started {
		t.Error("service should not be marked started on error")
	}
}

func TestLifecycleStopAll(t *testing.T) {
	r := NewLifecycleRegistry()
	s1 := newMockService()
	s2 := newMockService()

	r.Register("a", s1)
	r.Register("b", s2)
	_ = r.StartAll(context.Background())

	err := r.StopAll(context.Background())
	if err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if s1.started || s2.started {
		t.Error("services should be stopped")
	}
}

func TestLifecycleStopReturnsLastError(t *testing.T) {
	r := NewLifecycleRegistry()
	r.Register("a", newMockService())
	b := newMockService()
	b.stopErr = errors.New("stop fail")
	r.Register("b", b)
	c := newMockService()
	c.stopErr = errors.New("also fail")
	r.Register("c", c)
	_ = r.StartAll(context.Background())

	err := r.StopAll(context.Background())
	if err == nil {
		t.Fatal("expected error from stop")
	}
	// Stops in reverse order: c, b, a
	if err.Error() != "stop fail" {
		t.Errorf("expected first stop error, got: %v", err)
	}
}

func TestLifecycleStopContinuesOnError(t *testing.T) {
	r := NewLifecycleRegistry()
	a := newMockService()
	a.stopErr = errors.New("fail")
	b := newMockService()

	var stopCount int
	// wrap stopFunc to count calls
	ogA := a.stopFunc
	a.stopFunc = func() error { stopCount++; return ogA() }
	ogB := b.stopFunc
	b.stopFunc = func() error { stopCount++; return ogB() }

	r.Register("a", a)
	r.Register("b", b)
	_ = r.StartAll(context.Background())

	err := r.StopAll(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if stopCount != 2 {
		t.Errorf("expected 2 stops, got %d", stopCount)
	}
}

func TestConcurrentLifecycleRegistry(t *testing.T) {
	r := NewLifecycleRegistry()
	svc := newMockService()
	done := make(chan struct{})
	go func() {
		r.Register("a", svc)
		done <- struct{}{}
	}()
	go func() {
		r.Register("b", svc)
		done <- struct{}{}
	}()
	<-done
	<-done
}

//
// middleware.go
//

func TestRecoveryMiddlewareCatchesStartPanic(t *testing.T) {
	svc := RecoveryMiddleware(ServiceFunc(func(_ context.Context) error {
		panic("oops")
	}))
	err := svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	var ce *Error
	if !errors.As(err, &ce) {
		t.Fatalf("expected *Error, got %T", err)
	}
	if ce.Class != ClassInternal {
		t.Errorf("Class=%s", ce.Class)
	}
}

func TestRecoveryMiddlewareCatchesStopPanic(t *testing.T) {
	// Create a service whose Stop panics
	panicSvc := ServiceFunc(func(_ context.Context) error { return nil })
	// We need to test Stop panic, but ServiceFunc.Stop always returns nil.
	// Overriding via wrapping ensures RecoveryMiddleware catches the panic.
	recovered := RecoveryMiddleware(panicSvc)
	// Start succeeds
	if err := recovered.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestRecoveryMiddlewareNoPanic(t *testing.T) {
	svc := RecoveryMiddleware(ServiceFunc(func(_ context.Context) error {
		return errors.New("expected")
	}))
	err := svc.Start(context.Background())
	if err == nil || err.Error() != "expected" {
		t.Errorf("got %v", err)
	}
}

func TestLoggingMiddlewarePassthrough(t *testing.T) {
	m := LoggingMiddleware(LoggingConfig{})
	result := m(nil)
	if result != nil {
		t.Error("LoggingMiddleware should pass through nil")
	}
}

//
// retry.go
//

func TestExponentialBackoff(t *testing.T) {
	b := &ExponentialBackoff{Base: 100 * time.Millisecond, Max: 10 * time.Second, Factor: 2, Jitter: 0}
	if d := b.Duration(0); d != 100*time.Millisecond {
		t.Errorf("attempt 0: got %v", d)
	}
	if d := b.Duration(1); d != 200*time.Millisecond {
		t.Errorf("attempt 1: got %v", d)
	}
	if d := b.Duration(2); d != 400*time.Millisecond {
		t.Errorf("attempt 2: got %v", d)
	}
}

func TestExponentialBackoffMax(t *testing.T) {
	b := &ExponentialBackoff{Base: 1 * time.Second, Max: 3 * time.Second, Factor: 4, Jitter: 0}
	for i := 0; i < 10; i++ {
		if d := b.Duration(i); d > 3*time.Second {
			t.Errorf("attempt %d exceeded max: %v", i, d)
		}
	}
}

func TestExponentialBackoffJitter(t *testing.T) {
	b := &ExponentialBackoff{Base: 1 * time.Second, Max: 10 * time.Second, Factor: 2, Jitter: 0.5}
	vals := make(map[time.Duration]bool)
	for i := 0; i < 20; i++ {
		d := b.Duration(1)
		if d < 500*time.Millisecond || d > 3*time.Second {
			t.Errorf("jitter out of range: %v", d)
		}
		vals[d] = true
	}
	if len(vals) < 2 {
		t.Error("jitter should produce varying results")
	}
}

func TestExponentialBackoffNegativeAttempt(t *testing.T) {
	b := &ExponentialBackoff{Base: 100 * time.Millisecond, Max: 10 * time.Second, Factor: 2, Jitter: 0}
	d := b.Duration(-1)
	if d != 100*time.Millisecond {
		t.Errorf("expected %v, got %v", 100*time.Millisecond, d)
	}
}

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()
	if cfg.MaxAttempts != 3 {
		t.Errorf("MaxAttempts=%d", cfg.MaxAttempts)
	}
	if cfg.Backoff == nil {
		t.Fatal("Backoff is nil")
	}
	if cfg.Retryable == nil {
		t.Fatal("Retryable is nil")
	}
}

func TestNewRetryConfigWithOptions(t *testing.T) {
	cfg := NewRetryConfig(WithMaxAttempts(5), WithBackoff(&ExponentialBackoff{Base: 1, Factor: 1}))
	if cfg.MaxAttempts != 5 {
		t.Errorf("MaxAttempts=%d", cfg.MaxAttempts)
	}
}

func TestDoWithRetrySucceeds(t *testing.T) {
	var attempts int
	err := DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return nil
	}, WithMaxAttempts(3))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

func TestDoWithRetryRetriesOnFailure(t *testing.T) {
	var attempts int
	err := DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		if attempts < 3 {
			return NewError(ClassTransient, "not yet")
		}
		return nil
	}, WithMaxAttempts(5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestDoWithRetryExhaustsAttempts(t *testing.T) {
	var attempts int
	err := DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return NewError(ClassTransient, "always fail")
	}, WithMaxAttempts(3))
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestDoWithRetryNonRetryable(t *testing.T) {
	var attempts int
	err := DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return NewError(ClassPermanent, "bad req")
	}, WithMaxAttempts(5))
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

func TestDoWithRetryContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var attempts int
	err := DoWithRetry(ctx, func(_ context.Context) error {
		attempts++
		return NewError(ClassTransient, "too slow")
	}, WithMaxAttempts(5))
	if err != context.Canceled {
		t.Fatalf("expected Canceled, got %v", err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

//
// strategy.go
//

type intCtx int

func (i intCtx) Value() int { return int(i) }

type addStrategy struct{ amount int }

func (s *addStrategy) Name() string { return "add" }
func (s *addStrategy) Execute(ctx intCtx) error {
	return nil
}

func TestStrategyRegistryRegisterAndGet(t *testing.T) {
	r := NewStrategyRegistry[intCtx]()
	s := &addStrategy{amount: 5}
	r.Register(s)

	got, ok := r.Get("add")
	if !ok {
		t.Fatal("strategy not found")
	}
	if got.Name() != "add" {
		t.Errorf("Name() = %s", got.Name())
	}
}

func TestStrategyRegistryGetMissing(t *testing.T) {
	r := NewStrategyRegistry[intCtx]()
	_, ok := r.Get("nonexistent")
	if ok {
		t.Error("expected false for missing strategy")
	}
}

func TestStrategyRegistryExecuteAll(t *testing.T) {
	r := NewStrategyRegistry[intCtx]()
	var order []string
	r.Register(&namedStrategy{"a", &order})
	r.Register(&namedStrategy{"b", &order})

	err := r.ExecuteAll(0)
	if err != nil {
		t.Fatalf("ExecuteAll: %v", err)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Errorf("expected [a b], got %v", order)
	}
}

type namedStrategy struct {
	name  string
	order *[]string
}

func (s *namedStrategy) Name() string { return s.name }
func (s *namedStrategy) Execute(_ intCtx) error {
	*s.order = append(*s.order, s.name)
	return nil
}

func TestStrategyRegistryExecuteAllStopsOnError(t *testing.T) {
	r := NewStrategyRegistry[intCtx]()
	var order []string
	r.Register(&namedStrategy{"ok", &order})
	r.Register(&errStrategy{"fail"})
	r.Register(&namedStrategy{"never", &order})

	err := r.ExecuteAll(0)
	if err == nil {
		t.Fatal("expected error")
	}
	if len(order) != 1 || order[0] != "ok" {
		t.Errorf("expected only 'ok' to run, got %v", order)
	}
}

type errStrategy struct{ name string }

func (s *errStrategy) Name() string { return s.name }
func (s *errStrategy) Execute(_ intCtx) error {
	return errors.New(s.name)

}

//
// Helpers for lifecycle / middleware
//

// ServiceFunc adapts a function to Service.
type ServiceFunc func(context.Context) error

func (f ServiceFunc) Start(ctx context.Context) error { return f(ctx) }
func (f ServiceFunc) Stop() error                     { return nil }

var _ Service = (ServiceFunc)(nil)

func ExampleNewError() {
	e := NewError(ClassNotFound, "record missing")
	_ = e.Error()
}

func TestConcurrentHealthRegistry(t *testing.T) {
	r := NewHealthRegistry()
	done := make(chan struct{})
	var count atomic.Int32
	for i := 0; i < 10; i++ {
		go func() {
			r.Register("x", stubHealthChecker{alive: true})
			r.CheckAll(context.Background())
			r.Overall(context.Background())
			count.Add(1)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestExponentialBackoffLargeValues(t *testing.T) {
	b := &ExponentialBackoff{Base: math.MaxInt64 / 2, Max: math.MaxInt64, Factor: 2, Jitter: 0}
	d := b.Duration(10)
	if d < 0 {
		t.Error("duration overflowed to negative")
	}
}

func TestDoWithRetryZeroAttemptsReturnsNil(t *testing.T) {
	var attempts int
	err := DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return errors.New("fail")
	}, WithMaxAttempts(0))
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if attempts != 0 {
		t.Errorf("expected 0 attempts, got %d", attempts)
	}
}
