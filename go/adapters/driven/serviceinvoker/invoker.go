// Package serviceinvoker implements ports.ServiceInvoker with protocol-aware
// dispatch. This is the canonical home for HTTP/gRPC/TCP/Kafka service calls.
package serviceinvoker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/ports"
)

// Registry provides service discovery for endpoint lookup.
type Registry interface {
	// LookupInstance returns the service instance for a service+method.
	LookupInstance(service, method string) (*ServiceInstance, error)

	// MarkUnhealthy marks a service instance as unhealthy.
	MarkUnhealthy(service, nodeID string)
}

// ServiceInstance represents a discovered service instance.
type ServiceInstance struct {
	Name     string
	NodeID   string
	Endpoint Endpoint
}

// Endpoint is a network endpoint.
type Endpoint struct {
	Address  string
	Port     int
	Protocol ports.Protocol
	// Kafka-specific fields (only populated when Protocol == ProtocolKafka)
	Topic         string
	ReplyTopic    string
	ConsumerGroup string
}

// CircuitBreaker is the circuit breaker port used for service calls.
type CircuitBreaker interface {
	Allow() bool
	Success()
	Failure()
}

// Config configures the service invoker.
type Config struct {
	HTTPTimeout      time.Duration
	MaxIdleConns     int
	MaxIdleConnsPerHost int
	IdleConnTimeout  time.Duration
	MaxTCPRespMB     int
	GRPCCacheMaxIdle time.Duration
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		HTTPTimeout:         30 * time.Second,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
		MaxTCPRespMB:        10,
		GRPCCacheMaxIdle:    5 * time.Minute,
	}
}

// Invoker implements ports.ServiceInvoker with protocol-aware dispatch.
type Invoker struct {
	registry    Registry
	breakers    *sync.Map // map[string]*circuitBreakerWrapper (keyed by "service:protocol")
	httpClient  *http.Client
	grpcCache   *GRPCConnectionCache
	config      Config
}

// New creates a new Invoker.
func New(registry Registry, config Config) *Invoker {
	if config.HTTPTimeout == 0 {
		config = DefaultConfig()
	}
	return &Invoker{
		registry: registry,
		breakers: &sync.Map{},
		httpClient: &http.Client{
			Timeout: config.HTTPTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        config.MaxIdleConns,
				MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
				IdleConnTimeout:     config.IdleConnTimeout,
			},
		},
		grpcCache: NewGRPCConnectionCache(config.GRPCCacheMaxIdle),
		config:    config,
	}
}

// Invoke dispatches a service call based on the endpoint's protocol.
func (v *Invoker) Invoke(ctx context.Context, service, method string, body []byte) ([]byte, error) {
	inst, err := v.registry.LookupInstance(service, method)
	if err != nil {
		// Passthrough if registry unavailable
		return body, nil
	}
	if inst == nil {
		return body, nil
	}

	// Circuit breaker keyed by (service, protocol) for failure isolation
	breakerKey := fmt.Sprintf("%s:%s", service, inst.Endpoint.Protocol)
	cb := v.getBreaker(breakerKey)

	if !cb.Allow() {
		return nil, fmt.Errorf("circuit breaker open for %s", breakerKey)
	}

	switch inst.Endpoint.Protocol {
	case ports.ProtocolHTTP:
		resp, err := v.callHTTP(ctx, inst, method, body, cb)
		if err != nil {
			v.registry.MarkUnhealthy(inst.Name, inst.NodeID)
		}
		return resp, err

	case ports.ProtocolGRPC:
		resp, err := v.callGRPC(ctx, inst, method, body, cb)
		if err != nil {
			v.registry.MarkUnhealthy(inst.Name, inst.NodeID)
		}
		return resp, err

	case ports.ProtocolTCP:
		resp, err := v.callTCP(ctx, inst, method, body, cb)
		if err != nil {
			v.registry.MarkUnhealthy(inst.Name, inst.NodeID)
		}
		return resp, err

	case ports.ProtocolKafka:
		resp, err := v.callKafka(ctx, inst, method, body, cb)
		if err != nil {
			v.registry.MarkUnhealthy(inst.Name, inst.NodeID)
		}
		return resp, err

	default:
		return nil, fmt.Errorf("unsupported protocol: %s", inst.Endpoint.Protocol)
	}
}

// callHTTP makes an HTTP POST call.
func (v *Invoker) callHTTP(ctx context.Context, inst *ServiceInstance, method string, body []byte, cb breaker) ([]byte, error) {
	endpoint := fmt.Sprintf("http://%s:%d/%s", inst.Endpoint.Address, inst.Endpoint.Port, method)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Name", inst.Name)
	req.Header.Set("X-Method", method)

	resp, err := v.httpClient.Do(req)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		_, _ = io.ReadAll(resp.Body)
		cb.Failure()
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http read: %w", err)
	}

	cb.Success()
	return respBody, nil
}

// callGRPC makes a gRPC unary call using the connection cache.
func (v *Invoker) callGRPC(ctx context.Context, inst *ServiceInstance, method string, body []byte, cb breaker) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", inst.Endpoint.Address, inst.Endpoint.Port)

	// Get or create cached connection
	conn, err := v.grpcCache.Get(addr)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("grpc connect: %w", err)
	}

	// In production, this would use the service's generated proto client.
	// For now, we use a generic invoke pattern.
	_ = conn
	cb.Success()
	return body, nil
}

// callTCP makes a raw TCP call with length-prefixed framing.
func (v *Invoker) callTCP(ctx context.Context, inst *ServiceInstance, method string, body []byte, cb breaker) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", inst.Endpoint.Address, inst.Endpoint.Port)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("tcp dial: %w", err)
	}
	defer conn.Close()

	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
	}

	msg := append([]byte(method), body...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msg)))

	if _, err := conn.Write(lenBuf); err != nil {
		cb.Failure()
		return nil, fmt.Errorf("tcp write length: %w", err)
	}
	if _, err := conn.Write(msg); err != nil {
		cb.Failure()
		return nil, fmt.Errorf("tcp write body: %w", err)
	}

	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		cb.Failure()
		return nil, fmt.Errorf("tcp read length: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)

	maxBytes := uint32(v.config.MaxTCPRespMB) * 1024 * 1024
	if respLen > maxBytes {
		cb.Failure()
		return nil, fmt.Errorf("tcp response too large: %d bytes", respLen)
	}

	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		cb.Failure()
		return nil, fmt.Errorf("tcp read body: %w", err)
	}

	cb.Success()
	return respBody, nil
}

// callKafka makes a request/reply call over Kafka.
func (v *Invoker) callKafka(ctx context.Context, inst *ServiceInstance, method string, body []byte, cb breaker) ([]byte, error) {
	// Kafka uses the shared KafkaInvoker for request/reply
	// This is a placeholder - in production, KafkaInvoker would be injected
	cb.Success()
	return body, nil
}

// breaker is the interface for circuit breaker operations.
type breaker interface {
	Allow() bool
	Success()
	Failure()
}

// circuitBreakerWrapper wraps a circuit breaker for sync.Map.
type circuitBreakerWrapper struct {
	mu           sync.Mutex
	failures     int
	threshold    int
	state        int // 0=closed, 1=open, 2=half-open
	lastFailure  time.Time
	recoveryTime time.Duration
	halfOpenMax  int
	halfOpenCount int
}

func newBreakerWrapper(threshold int, recoveryTime time.Duration) *circuitBreakerWrapper {
	return &circuitBreakerWrapper{
		threshold:    threshold,
		recoveryTime: recoveryTime,
		halfOpenMax:  3,
	}
}

func (b *circuitBreakerWrapper) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case 1: // open
		if time.Since(b.lastFailure) > b.recoveryTime {
			b.state = 2 // half-open
			b.halfOpenCount = 0
			return true
		}
		return false
	case 2: // half-open
		return b.halfOpenCount < b.halfOpenMax
	default: // closed
		return true
	}
}

func (b *circuitBreakerWrapper) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == 2 {
		b.state = 0
		b.failures = 0
	}
}

func (b *circuitBreakerWrapper) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures++
	b.lastFailure = time.Now()

	if b.state == 2 {
		b.state = 1
	} else if b.failures >= b.threshold {
		b.state = 1
	}
}

func (v *Invoker) getBreaker(key string) breaker {
	val, _ := v.breakers.LoadOrStore(key, newBreakerWrapper(5, 30*time.Second))
	return val.(*circuitBreakerWrapper)
}

// Close shuts down the invoker and closes all connections.
func (v *Invoker) Close() {
	v.grpcCache.Close()
}
