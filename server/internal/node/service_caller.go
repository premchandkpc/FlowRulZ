package node

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	pkgreliability "github.com/premchandkpc/FlowRulZ/server/pkg/reliability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var (
	validServiceName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
	validMethodName  = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9/_-]{0,255}$`)
)

type traceKey struct{}

// ContextWithTraceID adds a trace ID to the context for distributed tracing correlation.
func ContextWithTraceID(ctx context.Context) context.Context {
	if v := ctx.Value(traceKey{}); v != nil {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, uuid.New().String())
}

// TraceIDFromContext extracts the trace ID from the context.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceKey{}).(string); ok {
		return v
	}
	return ""
}

// ServiceCaller handles protocol-aware service calls.
type ServiceCaller struct {
	httpClient    *http.Client
	grpcConns     map[string]*grpc.ClientConn
	grpcConnsMu   sync.Mutex
	tcpConns      map[string]*tcpConnPool
	tcpConnsMu    sync.Mutex
	tlsCertFile   string
	tlsKeyFile    string
}

type tcpConnPool struct {
	mu      sync.Mutex
	conns   chan net.Conn
	addr    string
	closed  bool
	closeMu sync.Mutex
}

// NewServiceCaller creates a new ServiceCaller with default HTTP client.
func NewServiceCaller() *ServiceCaller {
	return &ServiceCaller{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		grpcConns: make(map[string]*grpc.ClientConn),
		tcpConns:  make(map[string]*tcpConnPool),
	}
}

// NewServiceCallerWithTLS creates a new ServiceCaller with TLS for gRPC connections.
func NewServiceCallerWithTLS(certFile, keyFile string) *ServiceCaller {
	return &ServiceCaller{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		grpcConns:   make(map[string]*grpc.ClientConn),
		tcpConns:    make(map[string]*tcpConnPool),
		tlsCertFile: certFile,
		tlsKeyFile:  keyFile,
	}
}

const tcpPoolSize = 5

func (sc *ServiceCaller) getTCPPool(addr string) *tcpConnPool {
	sc.tcpConnsMu.Lock()
	defer sc.tcpConnsMu.Unlock()

	if pool, ok := sc.tcpConns[addr]; ok {
		return pool
	}

	pool := &tcpConnPool{
		conns: make(chan net.Conn, tcpPoolSize),
		addr:  addr,
	}
	sc.tcpConns[addr] = pool
	return pool
}

func (p *tcpConnPool) get() (net.Conn, error) {
	for {
		select {
		case conn := <-p.conns:
			if conn == nil {
				continue
			}
			if isConnAlive(conn) {
				return conn, nil
			}
			conn.Close()
		default:
			return net.DialTimeout("tcp", p.addr, 10*time.Second)
		}
	}
}

func isConnAlive(conn net.Conn) bool {
	_ = conn.SetReadDeadline(time.Now())
	one := make([]byte, 1)
	_, err := conn.Read(one)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return false
	}
	return true
}

func (p *tcpConnPool) put(conn net.Conn) {
	if conn == nil {
		return
	}
	p.closeMu.Lock()
	closed := p.closed
	p.closeMu.Unlock()
	if closed {
		conn.Close()
		return
	}
	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

func (p *tcpConnPool) close() {
	p.closeMu.Lock()
	p.closed = true
	p.closeMu.Unlock()

	p.mu.Lock()
	ch := p.conns
	p.conns = nil
	p.mu.Unlock()

	if ch != nil {
		close(ch)
		for conn := range ch {
			conn.Close()
		}
	}
}

// validateServiceName checks that a service name contains only safe characters.
func validateServiceName(name string) error {
	if !validServiceName.MatchString(name) {
		return fmt.Errorf("invalid service name: %q (must be 1-128 alphanumeric/dot/dash/underscore)", name)
	}
	return nil
}

// validateMethodName checks that a method name contains only safe characters.
func validateMethodName(method string) error {
	if !validMethodName.MatchString(method) {
		return fmt.Errorf("invalid method name: %q (must be 1-256 alphanumeric/slash/dash/underscore)", method)
	}
	return nil
}

// CallService dispatches a service call based on the endpoint's protocol.
func (sc *ServiceCaller) CallService(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg *registry.ServiceRegistry,
) ([]byte, error) {
	return sc.CallServiceWithRetry(ctx, inst, method, body, cb, reg, 0, 0)
}

// RetryConfig controls retry behavior for service calls.
type RetryConfig struct {
	MaxRetries int           // Maximum number of retries (0 = no retry)
	BaseDelay  time.Duration // Initial delay between retries
	MaxDelay   time.Duration // Cap on exponential backoff
}

// DefaultRetryConfig returns a sensible default: 3 retries, 100ms base, 5s max.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{MaxRetries: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second}
}

// CallServiceWithRetry dispatches a service call with configurable retry and exponential backoff.
// Retries on network errors and 5xx responses. Circuit breaker prevents cascading failures.
func (sc *ServiceCaller) CallServiceWithRetry(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg *registry.ServiceRegistry,
	maxRetries int,
	baseDelay time.Duration,
) ([]byte, error) {
	if inst == nil {
		return nil, fmt.Errorf("nil service instance")
	}

	if err := validateServiceName(inst.Name); err != nil {
		return nil, err
	}
	if err := validateMethodName(method); err != nil {
		return nil, err
	}

	if len(body) > 10*1024*1024 {
		return nil, fmt.Errorf("request body too large: %d bytes (max 10MB)", len(body))
	}

	if maxRetries <= 0 {
		maxRetries = 0
	}
	if baseDelay <= 0 {
		baseDelay = 100 * time.Millisecond
	}
	maxDelay := 5 * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(1<<(attempt-1))
			if delay > maxDelay {
				delay = maxDelay
			}
			slog.Info("service call retry", "service", inst.Name, "method", method, "attempt", attempt, "delay", delay)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		var resp []byte
		var err error
		switch inst.Endpoint.Protocol {
		case registry.ProtocolHTTP:
			resp, err = sc.callHTTP(ctx, inst, method, body, cb, reg)
		case registry.ProtocolGRPC:
			resp, err = sc.callGRPC(ctx, inst, method, body, cb, reg)
		case registry.ProtocolTCP:
			resp, err = sc.callTCP(ctx, inst, method, body, cb, reg)
		default:
			return nil, fmt.Errorf("unsupported protocol: %s", inst.Endpoint.Protocol)
		}

		if err == nil {
			return resp, nil
		}
		lastErr = err

		// Don't retry on client errors (4xx) — only retry on transient failures
		if !isRetryableError(err) {
			return nil, err
		}
	}

	return nil, fmt.Errorf("service call failed after %d attempts: %w", maxRetries+1, lastErr)
}

// isRetryableError determines if an error is transient and worth retrying.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Context cancellation/timeout — not retryable
	if ctxErr := context.DeadlineExceeded; err == ctxErr || err == context.Canceled {
		return false
	}
	// Circuit breaker open — not retryable (would fail immediately)
	if err == pkgreliability.ErrCircuitOpen {
		return false
	}
	// Network errors, DNS failures, connection refused — retryable
	errStr := err.Error()
	retryable := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"i/o timeout",
		"no such host",
		"http status 5",
		"tcp dial",
		"grpc connect",
	}
	for _, pattern := range retryable {
		if len(errStr) >= len(pattern) {
			for i := 0; i <= len(errStr)-len(pattern); i++ {
				if errStr[i:i+len(pattern)] == pattern {
					return true
				}
			}
		}
	}
	return false
}

// callHTTP makes an HTTP POST call to the service.
func (sc *ServiceCaller) callHTTP(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg *registry.ServiceRegistry,
) ([]byte, error) {
	endpoint := fmt.Sprintf("http://%s:%d/%s", inst.Endpoint.Address, inst.Endpoint.Port, method)
	
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Name", inst.Name)
	req.Header.Set("X-Method", method)
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}
	
	resp, err := sc.httpClient.Do(req)
	if err != nil {
		cb.Failure()
		reg.MarkUnhealthy(inst.Name, inst.Endpoint.NodeID)
		return nil, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode >= 500 {
		_, _ = io.ReadAll(io.LimitReader(resp.Body, 1024))
		cb.Failure()
		reg.MarkUnhealthy(inst.Name, inst.Endpoint.NodeID)
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http read: %w", err)
	}
	
	cb.Success()
	return respBody, nil
}

// callGRPC makes a gRPC unary call to the service.
// Returns an error if gRPC transport is not implemented rather than silently falling back to HTTP.
func (sc *ServiceCaller) callGRPC(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg *registry.ServiceRegistry,
) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", inst.Endpoint.Address, inst.Endpoint.Port)

	conn, err := sc.getGRPCConn(addr)
	if err != nil {
		cb.Failure()
		sc.evictGRPCConn(addr)
		return nil, fmt.Errorf("grpc connect: %w", err)
	}

	// Propagate trace ID via gRPC metadata
	md := metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		"x-trace-id":     TraceIDFromContext(ctx),
		"x-service-name": inst.Name,
		"x-method":       method,
	}))
	_ = conn
	_ = md

	return nil, fmt.Errorf("grpc transport: not implemented for service %s method %s at %s — configure generated proto client or use HTTP protocol", inst.Name, method, addr)
}

// callTCP makes a raw TCP call with length-prefixed framing.
func (sc *ServiceCaller) callTCP(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg *registry.ServiceRegistry,
) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", inst.Endpoint.Address, inst.Endpoint.Port)
	pool := sc.getTCPPool(addr)

	conn, err := pool.get()
	if err != nil {
		cb.Failure()
		reg.MarkUnhealthy(inst.Name, inst.Endpoint.NodeID)
		return nil, fmt.Errorf("tcp dial: %w", err)
	}

	// Set deadline
	deadline, ok := ctx.Deadline()
	if ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(30 * time.Second))
	}

	// Write length-prefixed message: [4 bytes length][method][body]
	msg := append([]byte(method), body...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msg)))

	if _, err := conn.Write(lenBuf); err != nil {
		conn.Close()
		cb.Failure()
		return nil, fmt.Errorf("tcp write length: %w", err)
	}
	if _, err := conn.Write(msg); err != nil {
		conn.Close()
		cb.Failure()
		return nil, fmt.Errorf("tcp write body: %w", err)
	}

	// Read response: [4 bytes length][response body]
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		conn.Close()
		cb.Failure()
		return nil, fmt.Errorf("tcp read length: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)

	if respLen > 10*1024*1024 { // 10MB max
		conn.Close()
		cb.Failure()
		return nil, fmt.Errorf("tcp response too large: %d bytes", respLen)
	}

	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		conn.Close()
		cb.Failure()
		return nil, fmt.Errorf("tcp read body: %w", err)
	}

	// Reset deadline and return connection to pool
	conn.SetDeadline(time.Time{})
	pool.put(conn)

	cb.Success()
	return respBody, nil
}

// getGRPCConn returns a cached gRPC connection or creates a new one.
func (sc *ServiceCaller) getGRPCConn(addr string) (*grpc.ClientConn, error) {
	sc.grpcConnsMu.Lock()
	defer sc.grpcConnsMu.Unlock()

	if conn, ok := sc.grpcConns[addr]; ok {
		return conn, nil
	}

	var opts []grpc.DialOption
	if sc.tlsCertFile != "" && sc.tlsKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(sc.tlsCertFile, sc.tlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load TLS cert: %w", err)
		}
		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	sc.grpcConns[addr] = conn
	return conn, nil
}

// evictGRPCConn removes a cached gRPC connection so the next call creates a fresh one.
func (sc *ServiceCaller) evictGRPCConn(addr string) {
	sc.grpcConnsMu.Lock()
	if conn, ok := sc.grpcConns[addr]; ok {
		conn.Close()
		delete(sc.grpcConns, addr)
	}
	sc.grpcConnsMu.Unlock()
}

// Close closes all gRPC connections and TCP pools.
func (sc *ServiceCaller) Close() {
	sc.grpcConnsMu.Lock()
	for addr, conn := range sc.grpcConns {
		conn.Close()
		delete(sc.grpcConns, addr)
	}
	sc.grpcConnsMu.Unlock()

	sc.tcpConnsMu.Lock()
	for addr, pool := range sc.tcpConns {
		pool.close()
		delete(sc.tcpConns, addr)
	}
	sc.tcpConnsMu.Unlock()
}
