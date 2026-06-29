package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/premchandkpc/FlowRulZ/go/internal/bridge"
)

type Lane string

const (
	LaneFast   Lane = "fast"
	LaneNormal Lane = "normal"
	LaneHeavy  Lane = "heavy"
)

func laneForScore(score uint32) Lane {
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
}

func New(persistPath string) *Engine {
	e := &Engine{
		rules:       make(map[string]*Rule),
		persistPath: persistPath,
	}
	if persistPath != "" {
		e.loadRules()
	}
	return e
}

type rulePersistence struct {
	ID       string               `json:"id"`
	Versions []versionPersistence `json:"versions"`
}

type versionPersistence struct {
	DSL     string `json:"dsl"`
	Version uint64 `json:"version"`
	Lane    Lane   `json:"lane"`
}

func (e *Engine) loadRules() {
	data, err := os.ReadFile(e.persistPath)
	if err != nil {
		return
	}
	var rules []rulePersistence
	if err := json.Unmarshal(data, &rules); err != nil {
		return
	}
	maxVer := e.nextVersion.Load()
	for _, r := range rules {
		rule := &Rule{
			ID:            r.ID,
			Versions:      make([]*VersionedPlan, len(r.Versions)),
			ActiveVersion: len(r.Versions) - 1,
		}
		for i, v := range r.Versions {
			plan, err := bridge.Compile(v.DSL, r.ID)
			if err != nil {
				continue
			}
			rule.Versions[i] = &VersionedPlan{
				Plan:    plan,
				DSL:     v.DSL,
				Version: v.Version,
				Lane:    v.Lane,
			}
			if v.Version > maxVer {
				maxVer = v.Version
			}
		}
		if len(rule.Versions) > 0 {
			e.rules[r.ID] = rule
		}
	}
	e.nextVersion.Store(maxVer)
}

func (e *Engine) saveRules() {
	if e.persistPath == "" {
		return
	}
	e.mu.RLock()
	rules := make([]rulePersistence, 0, len(e.rules))
	for _, r := range e.rules {
		vps := make([]versionPersistence, len(r.Versions))
		for i, v := range r.Versions {
			vps[i] = versionPersistence{
				DSL:     v.DSL,
				Version: v.Version,
				Lane:    v.Lane,
			}
		}
		rules = append(rules, rulePersistence{ID: r.ID, Versions: vps})
	}
	e.mu.RUnlock()
	data, err := json.Marshal(rules)
	if err != nil {
		return
	}
	// Atomic write: write to temp file then rename to prevent corruption on crash
	tmpPath := e.persistPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return
	}
	os.Rename(tmpPath, e.persistPath)
}

func (e *Engine) Deploy(id, dsl string) error {
	plan, err := bridge.Compile(dsl, id)
	if err != nil {
		return err
	}
	score := bridge.PlanComplexity(plan)
	vp := &VersionedPlan{
		Plan:    plan,
		DSL:     dsl,
		Version: e.nextVersion.Add(1),
		Lane:    laneForScore(score),
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
			return nil
		}
	}
	return fmt.Errorf("version %d not found for rule %s", version, id)
}

func (e *Engine) Rollback(id string, version uint64) error {
	return e.Promote(id, version)
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
		out = append(out, *r)
	}
	return out
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
