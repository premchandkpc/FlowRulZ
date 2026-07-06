package node

import (
	"context"
	"sync"
	"time"
)

type execEntry struct {
	cancel  context.CancelFunc
	started time.Time
	name    string
}

type execRegistry struct {
	mu   sync.RWMutex
	exec map[string]*execEntry
}

// ExecRegistry tracks in-flight executions for cancellation and observability.
type ExecRegistry interface {
	Register(id string, cancel context.CancelFunc, name string)
	Unregister(id string)
	Cancel(id string) bool
	CancelAll()
	List() map[string]time.Time
	Len() int
}

func NewExecRegistry() ExecRegistry {
	return &execRegistry{exec: make(map[string]*execEntry)}
}

func (er *execRegistry) Register(id string, cancel context.CancelFunc, name string) {
	er.mu.Lock()
	defer er.mu.Unlock()
	er.exec[id] = &execEntry{cancel: cancel, started: time.Now(), name: name}
}

func (er *execRegistry) Unregister(id string) {
	er.mu.Lock()
	defer er.mu.Unlock()
	delete(er.exec, id)
}

func (er *execRegistry) Cancel(id string) bool {
	er.mu.Lock()
	entry, ok := er.exec[id]
	if ok {
		delete(er.exec, id)
		entry.cancel()
	}
	er.mu.Unlock()
	return ok
}

func (er *execRegistry) CancelAll() {
	er.mu.Lock()
	defer er.mu.Unlock()
	for _, entry := range er.exec {
		entry.cancel()
	}
}

func (er *execRegistry) List() map[string]time.Time {
	er.mu.Lock()
	defer er.mu.Unlock()
	out := make(map[string]time.Time, len(er.exec))
	for id, entry := range er.exec {
		out[id] = entry.started
	}
	return out
}

func (er *execRegistry) Len() int {
	er.mu.RLock()
	defer er.mu.RUnlock()
	return len(er.exec)
}
