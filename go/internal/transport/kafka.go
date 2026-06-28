package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type KafkaConfig struct {
	Brokers    []string
	GroupID    string
	ConsumerCh chan []byte
}

type KafkaConsumer struct {
	topic   string
	handler MessageHandler
	cfg     KafkaConfig
	msgCh   chan []byte
	stopCh  chan struct{}
	started bool
	mu      sync.Mutex
}

func NewKafkaConsumer(topic string, handler MessageHandler, cfg KafkaConfig) *KafkaConsumer {
	ch := cfg.ConsumerCh
	if ch == nil {
		ch = make(chan []byte, 1000)
	}
	return &KafkaConsumer{
		topic:   topic,
		handler: handler,
		cfg:     cfg,
		msgCh:   ch,
		stopCh:  make(chan struct{}),
	}
}

func (kc *KafkaConsumer) Topic() string { return kc.topic }

func (kc *KafkaConsumer) Start(ctx context.Context) {
	kc.mu.Lock()
	if kc.started {
		kc.mu.Unlock()
		return
	}
	kc.started = true
	kc.mu.Unlock()

	log.Printf("kafka consumer: topic=%s brokers=%v group=%s", kc.topic, kc.cfg.Brokers, kc.cfg.GroupID)
	for {
		select {
		case <-kc.stopCh:
			return
		case <-ctx.Done():
			return
		case msg := <-kc.msgCh:
			_, err := kc.handler(ctx, msg)
			if err != nil {
				log.Printf("kafka handler error: %v", err)
			}
		}
	}
}

func (kc *KafkaConsumer) Stop() {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	if !kc.started {
		return
	}
	close(kc.stopCh)
	kc.started = false
}

func (kc *KafkaConsumer) Inject(msg []byte) {
	select {
	case kc.msgCh <- msg:
	default:
		log.Printf("kafka consumer %s: dropping message (buffer full)", kc.topic)
	}
}

type KafkaProducer struct {
	topic   string
	cfg     KafkaConfig
	closed  bool
	mu      sync.Mutex
}

func NewKafkaProducer(topic string, cfg KafkaConfig) *KafkaProducer {
	return &KafkaProducer{topic: topic, cfg: cfg}
}

func (kp *KafkaProducer) Send(ctx context.Context, key, value []byte) error {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if kp.closed {
		return fmt.Errorf("kafka producer %s: closed", kp.topic)
	}
	log.Printf("kafka produce: topic=%s key=%s val=%d bytes", kp.topic, string(key), len(value))
	return nil
}

func (kp *KafkaProducer) Close() {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	kp.closed = true
}
