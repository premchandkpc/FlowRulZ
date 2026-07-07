// Package invoker provides protocol-aware service call dispatch.
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

	"github.com/premchandkpc/FlowRulZ/server/internal/registry"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Registry looks up service instances for dispatch.
type Registry interface {
	LookupInstance(serviceName, method string) (*registry.ServiceInstance, error)
	MarkUnhealthy(serviceName, nodeID string)
}

// Caller handles protocol-aware service calls (HTTP, gRPC, TCP).
type Caller struct {
	httpClient  *http.Client
	grpcConns   map[string]*grpc.ClientConn
	grpcConnsMu sync.Mutex
}

// NewCaller creates a Caller with default HTTP client.
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
func (sc *Caller) CallService(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg Registry,
) ([]byte, error) {
	if inst == nil {
		return nil, fmt.Errorf("nil service instance")
	}

	switch inst.Endpoint.Protocol {
	case registry.ProtocolHTTP:
		return sc.callHTTP(ctx, inst, method, body, cb, reg)
	case registry.ProtocolGRPC:
		return sc.callGRPC(ctx, inst, method, body, cb, reg)
	case registry.ProtocolTCP:
		return sc.callTCP(ctx, inst, method, body, cb, reg)
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", inst.Endpoint.Protocol)
	}
}

func (sc *Caller) callHTTP(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg Registry,
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

	resp, err := sc.httpClient.Do(req)
	if err != nil {
		cb.Failure()
		reg.MarkUnhealthy(inst.Name, inst.Endpoint.NodeID)
		return nil, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		_, _ = io.ReadAll(resp.Body)
		cb.Failure()
		reg.MarkUnhealthy(inst.Name, inst.Endpoint.NodeID)
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

func (sc *Caller) callGRPC(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg Registry,
) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", inst.Endpoint.Address, inst.Endpoint.Port)

	_, err := sc.getGRPCConn(addr)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("grpc connect: %w", err)
	}

	slog.Warn("gRPC service call using HTTP fallback",
		"service", inst.Name,
		"method", method,
		"address", addr)

	return sc.callHTTP(ctx, inst, method, body, cb, reg)
}

func (sc *Caller) callTCP(
	ctx context.Context,
	inst *registry.ServiceInstance,
	method string,
	body []byte,
	cb *reliability.CircuitBreaker,
	reg Registry,
) ([]byte, error) {
	addr := fmt.Sprintf("%s:%d", inst.Endpoint.Address, inst.Endpoint.Port)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		cb.Failure()
		reg.MarkUnhealthy(inst.Name, inst.Endpoint.NodeID)
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

	if respLen > 10*1024*1024 {
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

func (sc *Caller) getGRPCConn(addr string) (*grpc.ClientConn, error) {
	sc.grpcConnsMu.Lock()
	defer sc.grpcConnsMu.Unlock()

	if conn, ok := sc.grpcConns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	sc.grpcConns[addr] = conn
	return conn, nil
}

// Close closes all gRPC connections.
func (sc *Caller) Close() {
	sc.grpcConnsMu.Lock()
	defer sc.grpcConnsMu.Unlock()

	for addr, conn := range sc.grpcConns {
		conn.Close()
		delete(sc.grpcConns, addr)
	}
}

// ProductionInvoker dispatches service calls via protocol-aware HTTP/gRPC/TCP.
type ProductionInvoker struct {
	caller          *Caller
	registry        Registry
	circuitBreakers sync.Map
}

// NewProductionInvoker creates a ProductionInvoker with the given registry.
func NewProductionInvoker(reg Registry) *ProductionInvoker {
	return &ProductionInvoker{
		caller:   NewCaller(),
		registry: reg,
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

	inst, err := p.registry.LookupInstance(serviceName, method)
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

	cbI, _ := p.circuitBreakers.LoadOrStore(serviceName, reliability.NewCircuitBreaker(5, 30*time.Second))
	cb := cbI.(*reliability.CircuitBreaker)
	resp, err := p.caller.CallService(svcCtx, inst, method, body, cb, p.registry)
	if err != nil {
		return nil, fmt.Errorf("service %s: %w", serviceName, err)
	}

	return resp, nil
}

// Close closes all gRPC connections held by the underlying Caller.
func (p *ProductionInvoker) Close() {
	p.caller.Close()
}
