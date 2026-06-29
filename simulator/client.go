package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/pkg/transport"
	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

type Client struct {
	sim *Simulator
}

type SendResult struct {
	Body     []byte
	Duration time.Duration
}

func (s *Simulator) Client() *Client {
	return &Client{sim: s}
}

func (c *Client) Send(ctx context.Context, ruleID string, body []byte) (*SendResult, error) {
	if c.sim.Bus != nil {
		return c.sendViaBus(ctx, ruleID, body)
	}

	plan := c.sim.Nodes[0].Plans.Get(ruleID)
	if plan == nil {
		return nil, fmt.Errorf("rule %q not found", ruleID)
	}

	ec := execution.NewContext(plan, body)
	resultCh := make(chan *execution.Result, 1)
	ec.ResultCh = resultCh

	ec.Transition(execution.StateRunning, "client dispatch")
	c.sim.Dispatcher.Dispatch(ec)

	select {
	case res := <-resultCh:
		if res.Error != nil {
			return nil, res.Error
		}
		return &SendResult{Body: res.Body, Duration: ec.Duration}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) sendViaBus(ctx context.Context, ruleID string, body []byte) (*SendResult, error) {
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < timeout {
			timeout = d
		}
	}
	reply, err := c.sim.Bus.Request("execution", &transport.Message{
		Headers: map[string]string{"rule_id": ruleID},
		Body:    body,
	}, timeout)
	if err != nil {
		return nil, err
	}

	// Check for application-level error in reply body
	if len(reply.Body) > 0 && reply.Body[0] == '{' {
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(reply.Body, &errResp) == nil && errResp.Error != "" {
			return nil, fmt.Errorf("%s", errResp.Error)
		}
	}

	var dur time.Duration
	if d := reply.Headers["duration"]; d != "" {
		dur, _ = time.ParseDuration(d)
	}
	return &SendResult{Body: reply.Body, Duration: dur}, nil
}

func (c *Client) RegisterService(svc *services.MockService) {
	c.sim.Services.Register(svc)
}

func (c *Client) AddRule(id, dsl string) error {
	plan := &execution.Plan{ID: id}
	if err := compileDSL(plan, dsl); err != nil {
		return err
	}
	for _, node := range c.sim.Nodes {
		p := *plan
		node.Plans.Add(&p)
	}
	return nil
}

func (c *Client) Plans() []string {
	return c.sim.Nodes[0].Plans.List()
}

type ServiceInfo struct {
	Name          string                `json:"name"`
	Methods       []services.MethodInfo `json:"methods,omitempty"`
	BaseLatencyMs int                   `json:"base_latency_ms,omitempty"`
	FailureRate   float64               `json:"failure_rate,omitempty"`
	MaxConcurrent int                   `json:"max_concurrent,omitempty"`
}

func (c *Client) Services() []ServiceInfo {
	svcs := c.sim.Services.All()
	info := make([]ServiceInfo, len(svcs))
	for i, svc := range svcs {
		info[i] = ServiceInfo{
			Name:          svc.Name,
			Methods:       svc.Methods,
			BaseLatencyMs: int(svc.BaseLatency / time.Millisecond),
			FailureRate:   svc.FailureRate,
			MaxConcurrent: svc.MaxConcurrent,
		}
	}
	return info
}
