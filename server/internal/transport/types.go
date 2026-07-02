package transport

import "context"

type MessageHandler func(ctx context.Context, msg []byte) ([]byte, error)

type MessageConsumer interface {
	Topic() string
	Start(ctx context.Context)
	Stop()
}

type MessageProducer interface {
	Send(ctx context.Context, key, value []byte) error
	Close()
}

var _ MessageConsumer = (*Consumer)(nil)
var _ MessageProducer = (*Producer)(nil)
