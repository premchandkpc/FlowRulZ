package execution

import "context"

type Executor interface {
	Execute(ctx context.Context, ruleID string, body []byte) ([]byte, error)
}

type Scheduler interface {
	Schedule(ctx context.Context, task *Task) error
	Cancel(ctx context.Context, taskID string) error
}

type Task struct {
	ID        string
	RuleID    string
	Body      []byte
	Priority  int
	Timeout   int64
	Metadata  map[string]string
}

type StateStore interface {
	Save(ctx context.Context, record *StateRecord) error
	Load(ctx context.Context, id string) (*StateRecord, error)
	List(ctx context.Context) ([]*StateRecord, error)
	Close() error
}

type StateRecord struct {
	ID          string
	State       string
	Body        []byte
	Output      []byte
	Error       string
	CreatedAt   int64
	CompletedAt int64
	NodeID      string
}
