package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type AcksLevel int

const (
	AcksNone AcksLevel = 0
	AcksOne  AcksLevel = 1
	AcksAll  AcksLevel = -1
)

func AcksLevelFromString(s string) AcksLevel {
	switch s {
	case "0":
		return AcksNone
	case "all", "-1":
		return AcksAll
	default:
		return AcksOne
	}
}

type KafkaConfig struct {
	Brokers    []string
	GroupID    string
	ConsumerCh chan []byte
	Acks       AcksLevel
	Idempotent bool
}

type KafkaConsumer struct {
	topic       string
	handler     MessageHandler
	cfg         KafkaConfig
	client      sarama.ConsumerGroup
	msgCh       chan []byte
	stopCh      chan struct{}
	started     bool
	mu          sync.Mutex
	wg          sync.WaitGroup
	manualCommit bool
}

func NewKafkaConsumer(topic string, handler MessageHandler, cfg KafkaConfig) *KafkaConsumer {
	ch := cfg.ConsumerCh
	if ch == nil {
		ch = make(chan []byte, 1000)
	}
	return &KafkaConsumer{
		topic:       topic,
		handler:     handler,
		cfg:         cfg,
		msgCh:       ch,
		stopCh:      make(chan struct{}),
		manualCommit: len(cfg.Brokers) > 0,
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

	log.Printf("kafka consumer: topic=%s brokers=%v group=%s manual_commit=%t",
		kc.topic, kc.cfg.Brokers, kc.cfg.GroupID, kc.manualCommit)

	if len(kc.cfg.Brokers) == 0 {
		kc.runChannel(ctx)
		return
	}

	config := sarama.NewConfig()
	config.Version = sarama.MinVersion
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond

	if kc.manualCommit {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}

	client, err := sarama.NewConsumerGroup(kc.cfg.Brokers, kc.cfg.GroupID, config)
	if err != nil {
		log.Printf("kafka consumer %s: failed to create consumer group: %v", kc.topic, err)
		kc.runChannel(ctx)
		return
	}
	kc.client = client

	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()
		for {
			select {
			case <-kc.stopCh:
				return
			default:
			}
			err := client.Consume(ctx, []string{kc.topic}, kc)
			if err != nil {
				log.Printf("kafka consumer %s: consume error: %v", kc.topic, err)
				select {
				case <-kc.stopCh:
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()
}

func (kc *KafkaConsumer) runChannel(ctx context.Context) {
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
	if kc.client != nil {
		kc.client.Close()
	}
	kc.wg.Wait()
	kc.started = false
}

func (kc *KafkaConsumer) Inject(msg []byte) {
	select {
	case kc.msgCh <- msg:
	default:
		log.Printf("kafka consumer %s: dropping message (buffer full)", kc.topic)
	}
}

func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (kc *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

type offsetTracker struct {
	offset int64
	marked bool
}

func (kc *KafkaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		_, err := kc.handler(context.Background(), msg.Value)
		if err != nil {
			log.Printf("kafka consumer %s: handler error: %v", kc.topic, err)
		}
		if kc.manualCommit {
			sess.MarkMessage(msg, "")
			sess.Commit()
		} else {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}

type KafkaProducer struct {
	topic    string
	cfg      KafkaConfig
	producer sarama.SyncProducer
	closed   bool
	mu       sync.Mutex
}

func NewKafkaProducer(topic string, cfg KafkaConfig) *KafkaProducer {
	return &KafkaProducer{topic: topic, cfg: cfg}
}

func (kp *KafkaProducer) Send(ctx context.Context, key, value []byte) error {
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
	log.Printf("kafka produce: topic=%s key=%s partition=%d offset=%d bytes=%d", kp.topic, string(key), part, offset, len(value))
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

func (kp *KafkaProducer) initProducer() error {
	if len(kp.cfg.Brokers) == 0 {
		log.Printf("kafka producer %s: no brokers configured, using log-only mode", kp.topic)
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
			log.Printf("kafka producer %s: idempotent requires acks=all, overriding", kp.topic)
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

func (kp *KafkaProducer) Close() {
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
