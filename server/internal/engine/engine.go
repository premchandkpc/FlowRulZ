package engine

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
)

type Lane string

const (
	LaneFast   Lane = "fast"
	LaneNormal Lane = "normal"
	LaneHeavy  Lane = "heavy"
)

func LaneForScore(score uint32) Lane {
	switch {
	case score < 10:
		return LaneFast
	case score <= 50:
		return LaneNormal
	default:
		return LaneHeavy
	}
}

type LaneConfig struct {
	Name        Lane
	BatchSize   int
	PollTimeout int // ms
}

var DefaultLanes = []LaneConfig{
	{Name: LaneFast, BatchSize: 500, PollTimeout: 10},
	{Name: LaneNormal, BatchSize: 100, PollTimeout: 50},
	{Name: LaneHeavy, BatchSize: 10, PollTimeout: 500},
}

type VersionedPlan struct {
	Plan       []byte
	DSL        string
	Version    uint64
	Lane       Lane
	ActiveExec sync.WaitGroup
}

type Rule struct {
	ID            string
	Versions      []*VersionedPlan
	ActiveVersion int
}

func (r *Rule) ActivePlan() *VersionedPlan {
	if r.ActiveVersion < 0 || r.ActiveVersion >= len(r.Versions) {
		return nil
	}
	return r.Versions[r.ActiveVersion]
}

type Engine struct {
	mu          sync.RWMutex
	rules       map[string]*Rule
	nextVersion atomic.Uint64
	persistPath string

	compiler compiler.Compiler

	AfterDeploy  func(id, dsl string, plan []byte, version uint64)
	AfterPromote func(id string, version uint64)
}

func New(persistPath string) *Engine {
	return NewWithCompiler(persistPath, compiler.NewLocal())
}

func NewWithCompiler(persistPath string, comp compiler.Compiler) *Engine {
	e := &Engine{
		rules:       make(map[string]*Rule),
		persistPath: persistPath,
		compiler:    comp,
	}
	if persistPath != "" {
		e.loadRules()
	}
	return e
}

func (e *Engine) Deploy(id, dsl string) error {
	result, err := e.compiler.Compile(dsl, id)
	if err != nil {
		return err
	}
	vp := &VersionedPlan{
		Plan:    result.Plan,
		DSL:     dsl,
		Version: e.nextVersion.Add(1),
		Lane:    LaneForScore(result.Complexity),
	}
	e.mu.Lock()
	r, ok := e.rules[id]
	if !ok {
		r = &Rule{ID: id}
		e.rules[id] = r
	}
	r.Versions = append(r.Versions, vp)
	r.ActiveVersion = len(r.Versions) - 1
	e.mu.Unlock()
	e.saveRules()

	if e.AfterDeploy != nil {
		e.AfterDeploy(id, dsl, result.Plan, vp.Version)
	}
	return nil
}

func (e *Engine) AddVersion(id, dsl string, plan []byte, version uint64) error {
	vp := &VersionedPlan{
		Plan:    plan,
		DSL:     dsl,
		Version: version,
		Lane:    LaneForScore(bridge.PlanComplexity(plan)),
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rules[id]
	if !ok {
		r = &Rule{ID: id, ActiveVersion: -1}
		e.rules[id] = r
	}
	for i, v := range r.Versions {
		if v.Version == version {
			r.Versions[i] = vp
			e.saveRules()
			return nil
		}
	}
	r.Versions = append(r.Versions, vp)
	e.saveRules()
	return nil
}

func (e *Engine) Promote(id string, version uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rules[id]
	if !ok {
		return fmt.Errorf("rule not found: %s", id)
	}
	for i, v := range r.Versions {
		if v.Version == version {
			r.ActiveVersion = i
			if e.AfterPromote != nil {
				e.AfterPromote(id, version)
			}
			return nil
		}
	}
	return fmt.Errorf("version %d not found for rule %s", version, id)
}

func (e *Engine) Drain(id string, version uint64) error {
	e.mu.Lock()
	r, ok := e.rules[id]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("rule not found: %s", id)
	}
	var idx int = -1
	for i, v := range r.Versions {
		if v.Version == version {
			idx = i
			break
		}
	}
	if idx < 0 {
		e.mu.Unlock()
		return fmt.Errorf("version %d not found for rule %s", version, id)
	}
	vp := r.Versions[idx]
	e.mu.Unlock()

	vp.ActiveExec.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok = e.rules[id]
	if !ok {
		return nil
	}
	for i, v := range r.Versions {
		if v.Version == version {
			r.Versions = append(r.Versions[:i], r.Versions[i+1:]...)
			if r.ActiveVersion == i {
				if len(r.Versions) > 0 {
					r.ActiveVersion = 0
				} else {
					r.ActiveVersion = -1
				}
			} else if r.ActiveVersion > i {
				r.ActiveVersion--
			}
			break
		}
	}
	if len(r.Versions) == 0 {
		delete(e.rules, id)
	}
	return nil
}

func (e *Engine) Remove(id string) {
	e.mu.Lock()
	r, ok := e.rules[id]
	e.mu.Unlock()
	if !ok {
		return
	}
	for _, v := range r.Versions {
		v.ActiveExec.Wait()
	}
	e.mu.Lock()
	delete(e.rules, id)
	e.mu.Unlock()
	e.saveRules()
}

func (e *Engine) Rules() []Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Rule, 0, len(e.rules))
	for _, r := range e.rules {
		cp := *r
		cp.Versions = append([]*VersionedPlan(nil), r.Versions...)
		out = append(out, cp)
	}
	return out
}

func (e *Engine) ActivePlanBytes() [][]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var plans [][]byte
	for _, r := range e.rules {
		vp := r.ActivePlan()
		if vp != nil && len(vp.Plan) > 0 {
			plans = append(plans, vp.Plan)
		}
	}
	return plans
}

func (e *Engine) ExecuteAll(body []byte, caller bridge.ServiceCaller, ctx *bridge.ExecContext) ([][]byte, error) {
	e.mu.RLock()
	var plans []*VersionedPlan
	for _, r := range e.rules {
		vp := r.ActivePlan()
		if vp != nil {
			vp.ActiveExec.Add(1)
			plans = append(plans, vp)
		}
	}
	e.mu.RUnlock()

	var results [][]byte
	for _, vp := range plans {
		res, err := bridge.Execute(vp.Plan, body, caller, ctx)
		if err != nil {
			vp.ActiveExec.Done()
			return results, err
		}
		results = append(results, res)
		vp.ActiveExec.Done()
	}
	return results, nil
}
