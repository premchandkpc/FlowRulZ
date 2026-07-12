package engine

import (
	"context"
	"fmt"

	pkgengine "github.com/premchandkpc/FlowRulZ/server/pkg/engine"
	pkgscheduler "github.com/premchandkpc/FlowRulZ/server/pkg/scheduler"
)

var _ pkgengine.Engine = (*Engine)(nil)

func (e *Engine) Start(ctx context.Context) error {
	return nil
}

func (e *Engine) Stop() error {
	return nil
}

func (e *Engine) AddRule(ctx context.Context, rule *pkgengine.Rule) error {
	return e.Deploy(rule.ID, rule.DSL)
}

func (e *Engine) RemoveRule(ctx context.Context, ruleID string) error {
	e.Remove(ruleID)
	return nil
}

func (e *Engine) GetRule(ctx context.Context, ruleID string) (*pkgengine.Rule, error) {
	for _, r := range e.Rules() {
		if r.ID == ruleID {
			active := r.ActiveVersion >= 0 && r.ActiveVersion < len(r.Versions)
			lane := pkgscheduler.LaneNormal
			if active {
				vp := r.Versions[r.ActiveVersion]
				switch vp.Lane {
				case LaneFast:
					lane = pkgscheduler.LaneFast
				case LaneHeavy:
					lane = pkgscheduler.LaneHeavy
				}
			}
			return &pkgengine.Rule{
				ID:     r.ID,
				Active: active,
				Lane:   lane,
			}, nil
		}
	}
	return nil, fmt.Errorf("rule %s not found", ruleID)
}

func (e *Engine) ListRules(ctx context.Context) ([]*pkgengine.Rule, error) {
	rules := e.Rules()
	result := make([]*pkgengine.Rule, 0, len(rules))
	for _, r := range rules {
		rule, _ := e.GetRule(ctx, r.ID)
		if rule != nil {
			result = append(result, rule)
		}
	}
	return result, nil
}

func (e *Engine) Execute(ctx context.Context, ruleID string, body []byte, opts *pkgengine.ExecuteOptions) (*pkgscheduler.Result, error) {
	rules := e.Rules()
	for _, r := range rules {
		if r.ID == ruleID {
			vp := r.ActivePlan()
			if vp == nil {
				return nil, fmt.Errorf("rule %s has no active version", ruleID)
			}
			plans := [][]byte{vp.Plan}
			results := e.executePlanOutputs(plans, body)
			if len(results) > 0 && results[0] != nil {
				return &pkgscheduler.Result{Body: results[0]}, nil
			}
			return &pkgscheduler.Result{}, nil
		}
	}
	return nil, fmt.Errorf("rule %s not found", ruleID)
}

func (e *Engine) CompileRule(ctx context.Context, rule *pkgengine.Rule) error {
	return e.Deploy(rule.ID, rule.DSL)
}

func (e *Engine) InvalidateCompilation(ruleID string) {
}

func (e *Engine) executePlanOutputs(plans [][]byte, body []byte) [][]byte {
	results := make([][]byte, len(plans))
	for i, plan := range plans {
		vp := &VersionedPlan{}
		vp.ActiveExec.Add(1)

		out, err := bridgeExecutePlan(plan, body)
		if err != nil {
			results[i] = []byte(err.Error())
		} else {
			results[i] = out
		}

		vp.ActiveExec.Done()
	}
	return results
}

func bridgeExecutePlan(plan []byte, _ []byte) ([]byte, error) {
	if len(plan) == 0 {
		return nil, nil
	}
	return plan, nil
}
