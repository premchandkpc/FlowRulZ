// Package flow is the FlowRulZ client SDK.
//
// It provides four communication models:
//   - Publish:  fire-and-forget async messaging
//   - Request:  synchronous request/reply
//   - Execute:  run a rule or workflow and return the result
//   - Stream:   persistent subscription to an event stream
//
// Usage:
//
//	client := flow.New(flow.Config{Address: "localhost:8080"})
//
//	// Fire-and-forget
//	client.Publish(ctx, "orders", orderPayload)
//
//	// Synchronous RPC
//	resp, err := client.Request(ctx, "payment", paymentPayload, 5*time.Second)
//
//	// Rule execution
//	result, err := client.Execute(ctx, "order-flow", orderPayload)
//
//	// Stream
//	stream, err := client.Stream(ctx, "events", handler)
package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Mode represents the delivery semantics of a message.
type Mode int

const (
	ModePublish  Mode = 0
	ModeRequest  Mode = 1
	ModeReply    Mode = 2
	ModeStream   Mode = 3
	ModeWorkflow Mode = 4
	ModeInternal Mode = 5
)

// Event is the universal message type in FlowRulZ.
// Payload is opaque bytes — any serialization format works.
type Event struct {
	ID      string            `json:"id"`
	Topic   string            `json:"topic"`
	Payload json.RawMessage   `json:"payload"`
	Headers map[string]string `json:"headers,omitempty"`
	Mode    Mode              `json:"mode"`
}

// Config configures the FlowRulZ client.
type Config struct {
	Address    string
	APIKey     string
	HTTPClient *http.Client
}

// Client is the FlowRulZ SDK client.
type Client struct {
	cfg Config
}

// New creates a new FlowRulZ client.
func New(cfg Config) *Client {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Address == "" {
		cfg.Address = "http://localhost:8080"
	}
	if !strings.HasPrefix(cfg.Address, "http") {
		cfg.Address = "http://" + cfg.Address
	}
	return &Client{cfg: cfg}
}

// Publish sends an event fire-and-forget. No response expected.
func (c *Client) Publish(ctx context.Context, topic string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("flow publish marshal: %w", err)
	}
	evt := Event{
		Topic:   topic,
		Payload: data,
		Mode:    ModePublish,
	}
	return c.sendEvent(ctx, evt)
}

// Request sends a synchronous request and waits for a reply.
func (c *Client) Request(ctx context.Context, service string, payload any, timeout time.Duration) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("flow request marshal: %w", err)
	}
	evt := Event{
		Topic:   service,
		Payload: data,
		Mode:    ModeRequest,
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	resp, err := c.roundTrip(ctx, evt)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Execute runs a rule and returns the result.
func (c *Client) Execute(ctx context.Context, ruleID string, payload any, opts ...ExecuteOption) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("flow execute marshal: %w", err)
	}

	cfg := executeConfig{timeout: 30 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}

	evt := Event{
		Topic:   ruleID,
		Payload: data,
		Mode:    ModeWorkflow,
		Headers: cfg.headers,
	}

	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	return c.roundTrip(ctx, evt)
}

// Stream subscribes to a topic and invokes handler for each event.
func (c *Client) Stream(ctx context.Context, topic string, handler func([]byte) error) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.cfg.Address+"/stream/"+topic, nil)
	if err != nil {
		return fmt.Errorf("flow stream req: %w", err)
	}
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("flow stream do: %w", err)
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("flow stream decode: %w", err)
		}
		if err := handler(raw); err != nil {
			return err
		}
	}
}

// -- internal helpers --

func (c *Client) sendEvent(ctx context.Context, evt Event) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.Address+"/event", bytes.NewReader(body))
	if err != nil {
		return err
	}
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *Client) roundTrip(ctx context.Context, evt Event) ([]byte, error) {
	body, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.Address+"/event", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-FlowRulZ-Mode", fmt.Sprintf("%d", evt.Mode))
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// -- options --

type executeConfig struct {
	timeout time.Duration
	headers map[string]string
}

type ExecuteOption func(*executeConfig)

func WithExecuteTimeout(d time.Duration) ExecuteOption {
	return func(c *executeConfig) { c.timeout = d }
}

func WithExecuteHeaders(h map[string]string) ExecuteOption {
	return func(c *executeConfig) { c.headers = h }
}
