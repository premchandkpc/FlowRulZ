package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
)

type Consumer struct {
	topic        string
	handler      transport.MessageHandler
	cfg          Config
	client       sarama.ConsumerGroup
	msgCh        chan []byte
	stopCh       chan struct{}
	started      bool
	mu           sync.Mutex
	wg           sync.WaitGroup
	manualCommit bool
}

func NewConsumer(topic string, handler transport.MessageHandler, cfg Config) *Consumer {
	ch := cfg.ConsumerCh
	if ch == nil {
		ch = make(chan []byte, 1000)
	}
	return &Consumer{
		topic:        topic,
		handler:      handler,
		cfg:          cfg,
		msgCh:        ch,
		stopCh:       make(chan struct{}),
		manualCommit: len(cfg.Brokers) > 0,
	}
}

func (kc *Consumer) Topic() string { return kc.topic }

func (kc *Consumer) Start(ctx context.Context) {
	kc.mu.Lock()
	if kc.started {
		kc.mu.Unlock()
		return
	}
	kc.started = true
	kc.mu.Unlock()

	slog.Info("kafka consumer started",
		"topic", kc.topic,
		"brokers", kc.cfg.Brokers,
		"group", kc.cfg.GroupID,
		"manual_commit", kc.manualCommit)

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
		slog.Error("kafka consumer failed to create consumer group", "topic", kc.topic, "error", err)
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
				slog.Error("kafka consumer consume error", "topic", kc.topic, "error", err)
				select {
				case <-kc.stopCh:
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()
}

func (kc *Consumer) runChannel(ctx context.Context) {
	for {
		select {
		case <-kc.stopCh:
			return
		case <-ctx.Done():
			return
		case msg := <-kc.msgCh:
			_, err := kc.handler(ctx, msg)
			if err != nil {
				slog.Error("kafka handler error", "error", err)
			}
		}
	}
}

func (kc *Consumer) Stop() {
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

func (kc *Consumer) Inject(msg []byte) {
	select {
	case kc.msgCh <- msg:
	default:
		slog.Warn("kafka consumer dropping message", "topic", kc.topic, "reason", "buffer_full")
	}
}

func (kc *Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (kc *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (kc *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		_, err := kc.handler(context.Background(), msg.Value)
		if err != nil {
			slog.Error("kafka consumer handler error", "topic", kc.topic, "error", err)
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
