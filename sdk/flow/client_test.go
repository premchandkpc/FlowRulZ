package flow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewDefaults(t *testing.T) {
	c := New(Config{})
	if c.cfg.Address != "http://localhost:8080" {
		t.Errorf("expected default addr, got %s", c.cfg.Address)
	}
	if c.cfg.HTTPClient != http.DefaultClient {
		t.Error("expected default http client")
	}
}

func TestNewAddressNormalization(t *testing.T) {
	c := New(Config{Address: "localhost:9090"})
	if c.cfg.Address != "http://localhost:9090" {
		t.Errorf("expected http:// prefix, got %s", c.cfg.Address)
	}
}

func TestNewCustomHTTPClient(t *testing.T) {
	tr := &http.Transport{}
	hc := &http.Client{Transport: tr}
	c := New(Config{HTTPClient: hc})
	if c.cfg.HTTPClient != hc {
		t.Error("expected custom http client")
	}
}

func TestPublishSuccess(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/event" {
			t.Errorf("expected /event, got %s", r.URL.Path)
		}
		var evt Event
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			t.Fatal(err)
		}
		if evt.Mode != ModePublish {
			t.Errorf("expected ModePublish, got %d", evt.Mode)
		}
		got.Store(evt)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	err := c.Publish(context.Background(), "orders", map[string]int{"id": 42})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	evt := got.Load().(Event)
	if evt.Topic != "orders" {
		t.Errorf("expected topic 'orders', got %s", evt.Topic)
	}
	if string(evt.Payload) != `{"id":42}` {
		t.Errorf("expected payload, got %s", evt.Payload)
	}
}

func TestPublishServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	err := c.Publish(context.Background(), "orders", "data")
	if err != nil {
		t.Fatalf("Publish should not error on non-2xx: %v", err)
	}
}

func TestPublishMarshalError(t *testing.T) {
	c := New(Config{})
	err := c.Publish(context.Background(), "orders", func() {})
	if err == nil {
		t.Fatal("expected marshal error")
	}
	if !strings.Contains(err.Error(), "publish") {
		t.Errorf("expected 'publish' in error, got %s", err)
	}
}

func TestRequestSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-FlowRulZ-Mode") != "1" {
			t.Errorf("expected mode 1 (Request), got %s", r.Header.Get("X-FlowRulZ-Mode"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	resp, err := c.Request(context.Background(), "payment", map[string]string{"amount": "100"}, 0)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if string(resp) != `{"status":"ok"}` {
		t.Errorf("expected response, got %s", resp)
	}
}

func TestRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	ctx := context.Background()
	_, err := c.Request(ctx, "slow", "data", 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestRequestMarshalError(t *testing.T) {
	c := New(Config{})
	_, err := c.Request(context.Background(), "svc", func() {}, 0)
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestRequestServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`internal error`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	resp, err := c.Request(context.Background(), "svc", "data", 0)
	if err != nil {
		t.Fatalf("Request should not error on non-2xx: %v", err)
	}
	if string(resp) != "internal error" {
		t.Errorf("expected body, got %s", resp)
	}
}

func TestExecuteSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-FlowRulZ-Mode") != "4" {
			t.Errorf("expected mode 4 (Workflow), got %s", r.Header.Get("X-FlowRulZ-Mode"))
		}
		w.Write([]byte(`{"result":"completed"}`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	resp, err := c.Execute(context.Background(), "order-flow", map[string]string{"id": "abc"})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(resp) != `{"result":"completed"}` {
		t.Errorf("expected result, got %s", resp)
	}
}

func TestExecuteWithHeaders(t *testing.T) {
	var gotHeaders map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evt Event
		json.NewDecoder(r.Body).Decode(&evt)
		gotHeaders = evt.Headers
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	_, err := c.Execute(context.Background(), "rule-1", "data",
		WithExecuteHeaders(map[string]string{"x-trace": "abc123", "x-env": "prod"}))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if gotHeaders["x-trace"] != "abc123" {
		t.Errorf("expected header x-trace, got %v", gotHeaders)
	}
	if gotHeaders["x-env"] != "prod" {
		t.Errorf("expected header x-env, got %v", gotHeaders)
	}
}

func TestExecuteDefaultTimeout(t *testing.T) {
	// Execute should apply a 30s default timeout
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	_, err := c.Execute(context.Background(), "rule-1", "data")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
}

func TestExecuteMarshalError(t *testing.T) {
	c := New(Config{})
	_, err := c.Execute(context.Background(), "rule", func() {})
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestStreamSuccess(t *testing.T) {
	var mu sync.Mutex
	var received []string
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream/test-topic" {
			t.Errorf("expected /stream/test-topic, got %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.Encode("event1")
		enc.Encode("event2")
		enc.Encode("event3")
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	handler := func(data []byte) error {
		mu.Lock()
		received = append(received, string(data))
		mu.Unlock()
		if len(received) == 3 {
			close(done)
		}
		return nil
	}

	go func() {
		if err := c.Stream(context.Background(), "test-topic", handler); err != nil {
			t.Errorf("Stream failed: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream events")
	}

	mu.Lock()
	if len(received) != 3 {
		t.Fatalf("expected 3 events, got %d: %v", len(received), received)
	}
	if received[0] != `"event1"` || received[1] != `"event2"` || received[2] != `"event3"` {
		t.Errorf("unexpected events: %v", received)
	}
	mu.Unlock()
}

func TestStreamHandlerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode("data")
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	err := c.Stream(context.Background(), "topic", func(data []byte) error {
		return http.ErrAbortHandler
	})
	if err == nil {
		t.Fatal("expected handler error")
	}
}

func TestStreamConnectionError(t *testing.T) {
	c := New(Config{Address: "http://127.0.0.1:1"})
	err := c.Stream(context.Background(), "topic", func(data []byte) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected connection error")
	}
}

func TestAuthHeader(t *testing.T) {
	var auth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL, APIKey: "secret-key"})
	c.Publish(context.Background(), "t", "data")
	if v := auth.Load().(string); v != "Bearer secret-key" {
		t.Errorf("expected Bearer token, got %s", v)
	}
}

func TestWithExecuteTimeout(t *testing.T) {
	cfg := executeConfig{timeout: 30 * time.Second}
	WithExecuteTimeout(5 * time.Second)(&cfg)
	if cfg.timeout != 5*time.Second {
		t.Errorf("expected 5s, got %v", cfg.timeout)
	}
}

func TestWithExecuteHeaders(t *testing.T) {
	cfg := executeConfig{}
	WithExecuteHeaders(map[string]string{"k": "v"})(&cfg)
	if cfg.headers["k"] != "v" {
		t.Errorf("expected header k=v, got %v", cfg.headers)
	}
}

func TestModeConstants(t *testing.T) {
	if ModePublish != 0 {
		t.Errorf("ModePublish: expected 0, got %d", ModePublish)
	}
	if ModeRequest != 1 {
		t.Errorf("ModeRequest: expected 1, got %d", ModeRequest)
	}
	if ModeReply != 2 {
		t.Errorf("ModeReply: expected 2, got %d", ModeReply)
	}
	if ModeStream != 3 {
		t.Errorf("ModeStream: expected 3, got %d", ModeStream)
	}
	if ModeWorkflow != 4 {
		t.Errorf("ModeWorkflow: expected 4, got %d", ModeWorkflow)
	}
	if ModeInternal != 5 {
		t.Errorf("ModeInternal: expected 5, got %d", ModeInternal)
	}
}

func TestExecuteEmptyOpts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := New(Config{Address: srv.URL})
	_, err := c.Execute(context.Background(), "rule", "data")
	if err != nil {
		t.Fatalf("Execute with empty opts failed: %v", err)
	}
}
