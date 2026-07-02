package transport

import (
	"context"
	"log/slog"
)

type Consumer struct {
	handler MessageHandler
	topic   string
	msgCh   chan []byte
	stopCh  chan struct{}
}

func (c *Consumer) Topic() string { return c.topic }

func NewConsumer(topic string, handler MessageHandler) *Consumer {
	return &Consumer{
		handler: handler,
		topic:   topic,
		msgCh:   make(chan []byte, 100),
		stopCh:  make(chan struct{}),
	}
}

func (c *Consumer) Start(ctx context.Context) {
	slog.Info("consumer started", "topic", c.topic)
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case msg := <-c.msgCh:
			_, err := c.handler(ctx, msg)
			if err != nil {
				slog.Error("consumer handler error", "error", err)
			}
		}
	}
}

func (c *Consumer) Inject(msg []byte) {
	select {
	case c.msgCh <- msg:
	default:
	}
}

func (c *Consumer) Stop() {
	close(c.stopCh)
}
