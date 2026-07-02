package engine

import (
	"encoding/json"
	"log/slog"
	"os"
)

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
			result, err := e.compiler.Compile(v.DSL, r.ID)
			if err != nil {
				slog.Warn("loadRules: compilation failed, skipping version", "rule", r.ID, "version", v.Version, "error", err)
				continue
			}
			rule.Versions[i] = &VersionedPlan{
				Plan:    result.Plan,
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
	tmpPath := e.persistPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		slog.Error("saveRules: write tmp failed", "error", err)
		return
	}
	if err := os.Rename(tmpPath, e.persistPath); err != nil {
		slog.Error("saveRules: rename failed", "error", err)
	}
}
