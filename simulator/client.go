package simulator

import (
	"context"
	"fmt"
	"time"

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

func (c *Client) Services() []string {
	return c.sim.Services.Names()
}
