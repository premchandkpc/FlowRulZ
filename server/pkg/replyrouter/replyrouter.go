package replyrouter

import (
	"context"
	"errors"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/pkg/transport"
)

type ReplyRouter interface {
	Register(ctx context.Context, correlationID string, ch chan<- *transport.Message, timeout time.Duration) error
	Cancel(correlationID string)
	Deliver(ctx context.Context, correlationID string, msg *transport.Message) bool
	PendingCount() int
	StartCleanup(ctx context.Context)
	StopCleanup()
}

var (
	ErrReplyTimeout = errors.New("reply wait timed out")
	ErrNoReplyRoute = errors.New("no reply route registered")
)
