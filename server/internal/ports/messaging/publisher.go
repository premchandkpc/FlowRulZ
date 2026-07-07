package messaging

import "context"

type Publisher interface {
	Publish(ctx context.Context, msg *Message) error
	PublishToPartition(ctx context.Context, topic, key string, msg *Message) error
}

type Producer interface {
	Send(ctx context.Context, key, value []byte) error
	Close() error
}
