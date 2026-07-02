package storage

import "context"

type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Close() error
}

type ExecutionStore interface {
	Save(ctx context.Context, record *ExecutionRecord) error
	Load(ctx context.Context, id string) (*ExecutionRecord, error)
	List(ctx context.Context) ([]*ExecutionRecord, error)
	Close() error
}
