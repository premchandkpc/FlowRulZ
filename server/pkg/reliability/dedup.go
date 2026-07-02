package reliability

import (
	"context"
	"errors"
	"time"
)

type Deduplicator interface {
	IsDuplicate(ctx context.Context, id string) bool
	MarkSeen(ctx context.Context, id string) error
	StartCleanup(ctx context.Context, interval time.Duration)
	StopCleanup()
}

var ErrDuplicate = errors.New("duplicate request")
