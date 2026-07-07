package pipeline

import (
	"context"
	"errors"
	"testing"
)

func TestChainOfResponsibility(t *testing.T) {
	t.Run("empty chain returns empty response", func(t *testing.T) {
		chain := NewChain()
		req := &Request{Body: []byte("test")}

		resp, err := chain.Execute(context.Background(), req)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if resp == nil {
			t.Error("expected non-nil response")
		}
	})

	t.Run("single handler processes request", func(t *testing.T) {
		executed := false
		handler := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			executed = true
			return &Response{Body: []byte("processed")}, nil
		})

		chain := NewChain(handler)
		req := &Request{Body: []byte("test")}

		resp, err := chain.Execute(context.Background(), req)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if !executed {
			t.Error("handler was not executed")
		}
		if string(resp.Body) != "processed" {
			t.Errorf("expected 'processed', got '%s'", string(resp.Body))
		}
	})

	t.Run("handler can short-circuit chain", func(t *testing.T) {
		secondExecuted := false

		first := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			return &Response{Body: []byte("short-circuited")}, nil
		})

		second := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			secondExecuted = true
			return next()
		})

		chain := NewChain(first, second)
		req := &Request{Body: []byte("test")}

		resp, err := chain.Execute(context.Background(), req)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if secondExecuted {
			t.Error("second handler should not have been executed")
		}
		if string(resp.Body) != "short-circuited" {
			t.Errorf("expected 'short-circuited', got '%s'", string(resp.Body))
		}
	})

	t.Run("handler can pass to next", func(t *testing.T) {
		order := []string{}

		first := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			order = append(order, "first-before")
			resp, err := next()
			order = append(order, "first-after")
			return resp, err
		})

		second := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			order = append(order, "second")
			return &Response{Body: []byte("done")}, nil
		})

		chain := NewChain(first, second)
		req := &Request{Body: []byte("test")}

		_, err := chain.Execute(context.Background(), req)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if len(order) != 3 {
			t.Errorf("expected 3 executions, got %d", len(order))
		}
		if order[0] != "first-before" || order[1] != "second" || order[2] != "first-after" {
			t.Errorf("wrong execution order: %v", order)
		}
	})

	t.Run("handler can modify request", func(t *testing.T) {
		modifier := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			req.Headers = map[string]string{"X-Modified": "true"}
			return next()
		})

		checker := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			if req.Headers["X-Modified"] != "true" {
				return &Response{Error: errors.New("request not modified")}, nil
			}
			return &Response{Body: []byte("ok")}, nil
		})

		chain := NewChain(modifier, checker)
		req := &Request{Body: []byte("test")}

		resp, err := chain.Execute(context.Background(), req)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if string(resp.Body) != "ok" {
			t.Errorf("expected 'ok', got '%s'", string(resp.Body))
		}
	})

	t.Run("handler can handle errors", func(t *testing.T) {
		errorHandler := HandlerFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
			return nil, errors.New("intentional error")
		})

		chain := NewChain(errorHandler)
		req := &Request{Body: []byte("test")}

		_, err := chain.Execute(context.Background(), req)

		if err == nil {
			t.Error("expected error, got nil")
		}
		if err.Error() != "intentional error" {
			t.Errorf("expected 'intentional error', got '%v'", err)
		}
	})
}

func TestBuilder(t *testing.T) {
	t.Run("builder creates chain", func(t *testing.T) {
		count := 0

		chain := NewBuilder().
			UseFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
				count++
				return next()
			}).
			UseFunc(func(ctx context.Context, req *Request, next func() (*Response, error)) (*Response, error) {
				count++
				return next()
			}).
			Build()

		req := &Request{Body: []byte("test")}
		_, err := chain.Execute(context.Background(), req)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 handlers executed, got %d", count)
		}
	})
}
