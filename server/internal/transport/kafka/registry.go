package kafka

import (
	"log/slog"

	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
)

// Config holds Kafka transport configuration for registration.
type RegistrationConfig struct {
	Brokers    []string
	GroupID    string
	Acks       string
	Idempotent bool
}

// RegisterKafka registers Kafka producer and consumer factories with the TransportFactory.
func RegisterKafka(factory *transport.TransportFactory, cfg RegistrationConfig) {
	if len(cfg.Brokers) == 0 {
		return
	}

	kafkaCfg := Config{
		Brokers:    cfg.Brokers,
		GroupID:    cfg.GroupID,
		Acks:       AcksLevelFromString(cfg.Acks),
		Idempotent: cfg.Idempotent,
	}

	factory.RegisterProducer(transport.KindKafka, func(topic string) transport.MessageProducer {
		return NewProducer(topic, kafkaCfg)
	})

	factory.RegisterConsumer(transport.KindKafka, func(topic string, handler transport.MessageHandler) transport.MessageConsumer {
		return NewConsumer(topic, handler, kafkaCfg)
	})

	slog.Info("transport: registered kafka backend", "brokers", cfg.Brokers)
}
