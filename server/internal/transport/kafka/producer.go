package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"
)

type Producer struct {
	topic    string
	cfg      Config
	producer sarama.SyncProducer
	closed   bool
	mu       sync.Mutex
}

func NewProducer(topic string, cfg Config) *Producer {
	return &Producer{topic: topic, cfg: cfg}
}

func (kp *Producer) Send(ctx context.Context, key, value []byte) error {
	kp.mu.Lock()
	if kp.closed {
		kp.mu.Unlock()
		return fmt.Errorf("kafka producer %s: closed", kp.topic)
	}
	if kp.producer == nil {
		if err := kp.initProducer(); err != nil {
			kp.mu.Unlock()
			return err
		}
	}
	kp.mu.Unlock()

	msg := &sarama.ProducerMessage{
		Topic: kp.topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	part, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("kafka produce %s: %w", kp.topic, err)
	}
	slog.Debug("kafka produce", "topic", kp.topic, "key", string(key), "partition", part, "offset", offset, "bytes", len(value))
	return nil
}

func kafkaAcksToSarama(acks AcksLevel) sarama.RequiredAcks {
	switch acks {
	case AcksNone:
		return sarama.NoResponse
	case AcksAll:
		return sarama.WaitForAll
	default:
		return sarama.WaitForLocal
	}
}

func (kp *Producer) initProducer() error {
	if len(kp.cfg.Brokers) == 0 {
		slog.Warn("kafka producer using log-only mode", "topic", kp.topic, "reason", "no_brokers")
		return nil
	}
	config := sarama.NewConfig()
	config.Version = sarama.MinVersion
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = kafkaAcksToSarama(kp.cfg.Acks)

	if kp.cfg.Idempotent {
		config.Producer.Idempotent = true
		config.Net.MaxOpenRequests = 1
		if kp.cfg.Acks != AcksAll {
			slog.Warn("kafka producer idempotent requires acks=all, overriding", "topic", kp.topic)
			config.Producer.RequiredAcks = sarama.WaitForAll
		}
	}

	producer, err := sarama.NewSyncProducer(kp.cfg.Brokers, config)
	if err != nil {
		return fmt.Errorf("kafka producer %s: create: %w", kp.topic, err)
	}
	kp.producer = producer
	return nil
}

func (kp *Producer) Close() {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if kp.closed {
		return
	}
	kp.closed = true
	if kp.producer != nil {
		kp.producer.Close()
	}
}
