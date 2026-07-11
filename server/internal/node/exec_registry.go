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

type ExecRegistry struct {
	mu   sync.RWMutex
	exec map[string]*execEntry
}

func NewExecRegistry() *ExecRegistry {
	return &ExecRegistry{exec: make(map[string]*execEntry)}
}

func (er *ExecRegistry) Register(id string, cancel context.CancelFunc, name string) {
	er.mu.Lock()
	defer er.mu.Unlock()
	er.exec[id] = &execEntry{cancel: cancel, started: time.Now(), name: name}
}

func (er *ExecRegistry) Unregister(id string) {
	er.mu.Lock()
	defer er.mu.Unlock()
	delete(er.exec, id)
}

func (er *ExecRegistry) Cancel(id string) bool {
	er.mu.Lock()
	entry, ok := er.exec[id]
	if ok {
		delete(er.exec, id)
		entry.cancel()
	}
	er.mu.Unlock()
	return ok
}

func (er *ExecRegistry) CancelAll() {
	er.mu.Lock()
	defer er.mu.Unlock()
	for _, entry := range er.exec {
		entry.cancel()
	}
	er.exec = make(map[string]*execEntry)
}

func (er *ExecRegistry) List() map[string]time.Time {
	er.mu.RLock()
	defer er.mu.RUnlock()
	out := make(map[string]time.Time, len(er.exec))
	for id, entry := range er.exec {
		out[id] = entry.started
	}
	return out
}

func (er *ExecRegistry) Len() int {
	er.mu.RLock()
	defer er.mu.RUnlock()
	return len(er.exec)
}
