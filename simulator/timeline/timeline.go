package timeline

import (
	"encoding/json"
	"sync"
	"time"
)

type EventType int

const (
	EventCreated EventType = iota
	EventReady
	EventInstruction
	EventServiceCall
	EventServiceResponse
	EventServiceError
	EventSuspend
	EventResume
	EventCompleted
	EventFailed
	EventDropped
)

func (e EventType) String() string {
	switch e {
	case EventCreated:
		return "created"
	case EventReady:
		return "ready"
	case EventInstruction:
		return "instruction"
	case EventServiceCall:
		return "service_call"
	case EventServiceResponse:
		return "service_response"
	case EventServiceError:
		return "service_error"
	case EventSuspend:
		return "suspend"
	case EventResume:
		return "resume"
	case EventCompleted:
		return "completed"
	case EventFailed:
		return "failed"
	case EventDropped:
		return "dropped"
	default:
		return "unknown"
	}
}

type Event struct {
	ExecID    string        `json:"exec_id"`
	Timestamp time.Time     `json:"timestamp"`
	Type      EventType     `json:"type"`
	Op        string        `json:"op,omitempty"`
	Service   string        `json:"service,omitempty"`
	IP        int           `json:"ip,omitempty"`
	Elapsed   time.Duration `json:"elapsed_ms,omitempty"`
	Meta      string        `json:"meta,omitempty"`
	NodeID    string        `json:"node_id,omitempty"`
}

type Store struct {
	mu     sync.RWMutex
	events []Event
	byExec map[string][]Event
}

func NewStore() *Store {
	return &Store{
		events: make([]Event, 0, 100000),
		byExec: make(map[string][]Event),
	}
}

func (s *Store) Record(event Event) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.byExec[event.ExecID] = append(s.byExec[event.ExecID], event)
	s.mu.Unlock()
}

func (s *Store) ForExec(execID string) []Event {
	s.mu.RLock()
	events := make([]Event, len(s.byExec[execID]))
	copy(events, s.byExec[execID])
	s.mu.RUnlock()
	return events
}

func (s *Store) Recent(n int) []Event {
	s.mu.RLock()
	start := len(s.events) - n
	if start < 0 {
		start = 0
	}
	events := make([]Event, len(s.events)-start)
	copy(events, s.events[start:])
	s.mu.RUnlock()
	return events
}

func (s *Store) All() []Event {
	s.mu.RLock()
	events := make([]Event, len(s.events))
	copy(events, s.events)
	s.mu.RUnlock()
	return events
}

func (s *Store) Clear() {
	s.mu.Lock()
	s.events = make([]Event, 0, 100000)
	s.byExec = make(map[string][]Event)
	s.mu.Unlock()
}

func (s *Store) Stats() map[string]int {
	s.mu.RLock()
	stats := make(map[string]int)
	for _, e := range s.events {
		key := e.Type.String()
		stats[key]++
	}
	s.mu.RUnlock()
	return stats
}

func (s *Store) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.All())
}
