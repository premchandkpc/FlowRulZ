package messaging

import "context"

type Subscriber interface {
	Subscribe(ctx context.Context, topic string, handler Handler) (*Subscription, error)
	Unsubscribe(ctx context.Context, sub *Subscription) error
}

type Consumer interface {
	Topic() string
	Start(ctx context.Context) error
	Stop() error
}
