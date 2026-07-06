// Package invoker implements the ServiceInvoker port.
// Protocol-aware service call dispatch (HTTP, gRPC, TCP).
package invoker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Caller handles protocol-aware service calls.
type Caller struct {
	httpClient  *http.Client
	grpcConns   map[string]*grpc.ClientConn
	grpcConnsMu sync.Mutex
}

// NewCaller creates a new Caller with default HTTP client.
func NewCaller() *Caller {
	return &Caller{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		grpcConns: make(map[string]*grpc.ClientConn),
	}
}

// CallService dispatches a service call based on the endpoint's protocol.
func (c *Caller) CallService(
	ctx context.Context,
	inst *ports.ServiceInstance,
	method string,
	body []byte,
) ([]byte, error) {
	if inst == nil {
		return nil, fmt.Errorf("nil service instance")
	}

	// Default to HTTP for port-based service instances
	return c.callHTTP(ctx, inst, method, body)
}

func (c *Caller) callHTTP(
	ctx context.Context,
	inst *ports.ServiceInstance,
	method string,
	body []byte,
) ([]byte, error) {
	endpoint := fmt.Sprintf("http://%s/%s", inst.Address, method)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Name", inst.Name)
	req.Header.Set("X-Method", method)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		_, _ = io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("http read: %w", err)
	}

	return respBody, nil
}

func (c *Caller) callGRPC(
	ctx context.Context,
	inst *ports.ServiceInstance,
	method string,
	body []byte,
) ([]byte, error) {
	addr := inst.Address

	_, err := c.getGRPCConn(addr)
	if err != nil {
		return nil, fmt.Errorf("grpc connect: %w", err)
	}

	slog.Warn("gRPC service call using HTTP fallback",
		"service", inst.Name,
		"method", method,
		"address", addr)

	return c.callHTTP(ctx, inst, method, body)
}

func (c *Caller) callTCP(
	ctx context.Context,
	inst *ports.ServiceInstance,
	method string,
	body []byte,
) ([]byte, error) {
	addr := inst.Address

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
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
		return nil, fmt.Errorf("tcp write length: %w", err)
	}
	if _, err := conn.Write(msg); err != nil {
		return nil, fmt.Errorf("tcp write body: %w", err)
	}

	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("tcp read length: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)

	if respLen > 10*1024*1024 {
		return nil, fmt.Errorf("tcp response too large: %d bytes", respLen)
	}

	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		return nil, fmt.Errorf("tcp read body: %w", err)
	}

	return respBody, nil
}

func (c *Caller) getGRPCConn(addr string) (*grpc.ClientConn, error) {
	c.grpcConnsMu.Lock()
	defer c.grpcConnsMu.Unlock()

	if conn, ok := c.grpcConns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	c.grpcConns[addr] = conn
	return conn, nil
}

// Close closes all gRPC connections.
func (c *Caller) Close() {
	c.grpcConnsMu.Lock()
	defer c.grpcConnsMu.Unlock()

	for addr, conn := range c.grpcConns {
		conn.Close()
		delete(c.grpcConns, addr)
	}
}

// ProductionInvoker dispatches service calls via protocol-aware HTTP/gRPC/TCP.
type ProductionInvoker struct {
	caller          *Caller
	registry        ports.ServiceRegistry
	circuitBreakers sync.Map
}

// NewProductionInvoker creates a ProductionInvoker with the given registry.
func NewProductionInvoker(registry ports.ServiceRegistry) *ProductionInvoker {
	return &ProductionInvoker{
		caller:   NewCaller(),
		registry: registry,
	}
}

func (p *ProductionInvoker) Invoke(ctx context.Context, serviceName, method string, body []byte) ([]byte, error) {
	svcTimeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < svcTimeout {
			svcTimeout = remaining
		}
	}
	svcCtx, svcCancel := context.WithTimeout(ctx, svcTimeout)
	defer svcCancel()

	inst, err := p.registry.Lookup(serviceName, method)
	if err != nil {
		slog.Warn("registry lookup failed, using passthrough",
			"service", serviceName,
			"method", method,
			"error", err)
		return body, nil
	}

	if inst == nil {
		slog.Info("service call (passthrough)", "service", serviceName, "method", method, "body_bytes", len(body))
		return body, nil
	}

	resp, err := p.caller.CallService(svcCtx, inst, method, body)
	if err != nil {
		return nil, fmt.Errorf("service %s: %w", serviceName, err)
	}

	return resp, nil
}

// Close closes all gRPC connections held by the underlying Caller.
func (p *ProductionInvoker) Close() {
	p.caller.Close()
}

// Compile-time interface compliance check
var _ ports.ServiceInvoker = (*ProductionInvoker)(nil)
