package admin

import (
	"fmt"
	"runtime"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
)

type ruleService struct {
	engine   *engine.Engine
	compiler compiler.Compiler
}

type ruleView struct {
	ID       string        `json:"id"`
	Versions []versionView `json:"versions"`
}

type versionView struct {
	Version uint64 `json:"version"`
	DSL     string `json:"dsl"`
	Active  bool   `json:"active"`
	Lane    string `json:"lane,omitempty"`
}

func newRuleService(eng *engine.Engine, comp compiler.Compiler) *ruleService {
	if comp == nil {
		comp = compiler.NewLocal()
	}
	return &ruleService{engine: eng, compiler: comp}
}

func (rs *ruleService) DeployRule(id, dsl string) error {
	if rs.engine == nil {
		return fmt.Errorf("engine not configured")
	}
	return rs.engine.Deploy(id, dsl)
}

func (rs *ruleService) RemoveRule(id string) {
	if rs.engine != nil {
		rs.engine.Remove(id)
	}
}

func (rs *ruleService) ListRules() []ruleView {
	if rs.engine == nil {
		return nil
	}
	rules := rs.engine.Rules()
	views := make([]ruleView, 0, len(rules))
	for _, ru := range rules {
		views = append(views, ruleView{ID: ru.ID, Versions: buildVersionViews(ru, false)})
	}
	return views
}

func (rs *ruleService) RuleDetail(id string) (map[string]interface{}, bool) {
	if rs.engine == nil {
		return nil, false
	}
	for _, ru := range rs.engine.Rules() {
		if ru.ID != id {
			continue
		}
		return map[string]interface{}{"id": ru.ID, "versions": buildVersionViews(ru, true)}, true
	}
	return nil, false
}

func (rs *ruleService) RuleVersions(id string) []versionView {
	if rs.engine == nil {
		return nil
	}
	for _, ru := range rs.engine.Rules() {
		if ru.ID == id {
			return buildVersionViews(ru, true)
		}
	}
	return []versionView{}
}

// buildVersionViews constructs versionView slices from a rule's versions.
// If includeDSL is true, the DSL source is included in each view.
func buildVersionViews(ru engine.Rule, includeDSL bool) []versionView {
	vvs := make([]versionView, len(ru.Versions))
	for i, v := range ru.Versions {
		vvs[i] = versionView{
			Version: v.Version,
			Active:  i == ru.ActiveVersion,
		}
		if includeDSL {
			vvs[i].DSL = v.DSL
			vvs[i].Lane = string(v.Lane)
		}
	}
	return vvs
}

func (rs *ruleService) ValidateDSL(dsl string) (map[string]interface{}, error) {
	if rs.compiler == nil {
		return nil, fmt.Errorf("compiler not configured")
	}
	result, err := rs.compiler.Compile(dsl, "validate")
	if err != nil {
		return map[string]interface{}{"valid": false, "error": err.Error()}, err
	}
	return map[string]interface{}{
		"valid":            true,
		"complexity_score": result.Complexity,
		"plan_bytes":       len(result.Plan),
	}, nil
}

func (rs *ruleService) PromoteVersion(id string, version uint64) error {
	if rs.engine == nil {
		return fmt.Errorf("engine not configured")
	}
	return rs.engine.Promote(id, version)
}

func (rs *ruleService) Lanes() []map[string]interface{} {
	lanes := make([]map[string]interface{}, len(engine.DefaultLanes))
	for i, l := range engine.DefaultLanes {
		lanes[i] = map[string]interface{}{
			"name":         string(l.Name),
			"batch_size":   l.BatchSize,
			"poll_timeout": l.PollTimeout,
		}
	}
	return lanes
}

func (rs *ruleService) HealthSnapshot() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	count := 0
	if rs.engine != nil {
		count = len(rs.engine.Rules())
	}
	return map[string]interface{}{
		"status":       "ok",
		"time":         time.Now().UTC().Format(time.RFC3339),
		"goroutines":   runtime.NumGoroutine(),
		"alloc_mb":     fmt.Sprintf("%.1f", float64(m.Alloc)/1024/1024),
		"heap_objects": m.HeapObjects,
		"num_rules":    count,
		"go_version":   runtime.Version(),
	}
}

func (rs *ruleService) DebugSnapshot() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	count := 0
	if rs.engine != nil {
		count = len(rs.engine.Rules())
	}
	return map[string]interface{}{
		"goroutines": runtime.NumGoroutine(),
		"cgo_calls":  runtime.NumCgoCall(),
		"memory": map[string]interface{}{
			"alloc":        m.Alloc,
			"total_alloc":  m.TotalAlloc,
			"sys":          m.Sys,
			"heap_alloc":   m.HeapAlloc,
			"heap_sys":     m.HeapSys,
			"heap_objects": m.HeapObjects,
			"gc_cycles":    m.NumGC,
			"gc_pause_ns":  m.PauseNs[(m.NumGC+255)%256],
		},
		"num_rules":  count,
		"go_version": runtime.Version(),
	}
}
