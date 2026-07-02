package transport

import (
	"context"
	"log/slog"
)

type Producer struct {
	topic string
}

func NewProducer(topic string) *Producer {
	return &Producer{topic: topic}
}

func (p *Producer) Send(ctx context.Context, key, value []byte) error {
	slog.Debug("produced to topic", "topic", p.topic, "key", string(key), "bytes", len(value))
	return nil
}

func (p *Producer) Close() {}
