package execnode

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
	"github.com/premchandkpc/FlowRulZ/server/internal/reliability"
)

func (en *ExecutionNode) httpCall(endpoint string, body []byte, cb *reliability.CircuitBreaker) ([]byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: request: %w", err)
	}
	resp, err := en.httpClient.Do(req)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		cb.Failure()
		return nil, fmt.Errorf("http call: status %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		cb.Failure()
		return nil, fmt.Errorf("http call: read: %w", err)
	}

	cb.Success()
	return respBody, nil
}

func (en *ExecutionNode) callService(svcName, method string, body []byte, timeoutMs uint64) ([]byte, error) {
	observability.RecordExec("svc_call")

	svcTimeout := 10 * time.Second
	if timeoutMs > 0 {
		svcTimeout = time.Duration(timeoutMs) * time.Millisecond
	}
	svcCtx, svcCancel := context.WithTimeout(context.Background(), svcTimeout)
	defer svcCancel()

	cbI, _ := en.circuitBreakers.LoadOrStore(svcName, reliability.NewCircuitBreaker(5, 30*time.Second))
	cb := cbI.(*reliability.CircuitBreaker)

	if !cb.Allow() {
		observability.RecordError("circuit_breaker_open")
		slog.Warn("circuit breaker open for service", "service", svcName)
		return nil, fmt.Errorf("circuit breaker open for service %s", svcName)
	}

	inst, _ := en.Registry.LookupInstance(svcName, method)
	if inst != nil {
		endpoint := fmt.Sprintf("%s://%s:%d", inst.Endpoint.Protocol, inst.Endpoint.Address, inst.Endpoint.Port)
		req, err := http.NewRequestWithContext(svcCtx, "POST", endpoint, bytes.NewReader(body))
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: request: %w", svcName, err)
		}
		resp, err := en.httpClient.Do(req)
		if err != nil {
			cb.Failure()
			en.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
			return nil, fmt.Errorf("service %s: call: %w", svcName, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			cb.Failure()
			en.Registry.MarkUnhealthy(svcName, inst.Endpoint.NodeID)
			return nil, fmt.Errorf("service %s: status %d", svcName, resp.StatusCode)
		}

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: read: %w", svcName, err)
		}

		cb.Success()
		return respBody, nil
	}

	if en.serviceResolver != nil {
		endpoint, err := en.serviceResolver.Resolve(0, method)
		if err != nil {
			cb.Failure()
			return nil, fmt.Errorf("service %s: resolve: %w", svcName, err)
		}
		return en.httpCall(endpoint, body, cb)
	}

	slog.Info("service call", "service", svcName, "method", method, "body_bytes", len(body))
	cb.Success()
	return body, nil
}

func (en *ExecutionNode) handleIncomingMessage(ctx context.Context, msg []byte) ([]byte, error) {
	if !en.RateLimiter.Allow("ingress") {
		observability.RecordError("rate_limited")
		en.DLQ.Send(&reliability.DeadLetterEntry{
			ID:    "ratelimited",
			Body:  msg,
			Error: "rate limited",
		})
		return nil, nil
	}

	msgID := make([]byte, 16)
	if _, err := rand.Read(msgID); err != nil {
		return nil, fmt.Errorf("message id generation failed: %w", err)
	}
	msgIDStr := hex.EncodeToString(msgID)

	if en.Dedup.Seen(msgIDStr) {
		observability.RecordExec("dedup_skipped")
		return nil, nil
	}
	en.Dedup.Mark(msgIDStr)

	execCtx, execCancel := context.WithTimeout(ctx, 30*time.Second)
	defer execCancel()

	results, err := en.executeAll(execCtx, msg)
	if err != nil {
		observability.RecordError("exec")
		en.DLQ.Send(&reliability.DeadLetterEntry{
			ID:    "exec-error",
			Body:  msg,
			Error: err.Error(),
		})
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	observability.RecordExec("msg")
	return results[0], nil
}
