// Package serviceinvoker implements ports.ServiceInvoker with protocol-aware
// dispatch. This is the canonical home for HTTP/gRPC/TCP/Kafka service calls.
package serviceinvoker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// KafkaProducer sends messages to a topic.
type KafkaProducer interface {
	Send(ctx context.Context, key, value []byte) error
	Close()
}

// KafkaConsumer receives messages from a topic.
type KafkaConsumer interface {
	Start(ctx context.Context)
	Stop()
	Inject(msg []byte)
}

// KafkaConfig configures Kafka connections.
type KafkaConfig struct {
	Brokers       []string
	ConsumerGroup string
	Acks          int // -1=all, 0=none, 1=one
	Idempotent    bool
	RequestTimeout time.Duration
}

// KafkaInvoker implements request/reply over Kafka using correlation IDs.
// It reuses shared, long-lived producers and consumers per topic.
type KafkaInvoker struct {
	producers sync.Map // map[string]KafkaProducer (topic -> producer)
	consumers sync.Map // map[string]*kafkaReplyConsumer (reply_topic -> consumer)
	config    KafkaConfig
	mu        sync.Mutex
}

// NewKafkaInvoker creates a new KafkaInvoker.
func NewKafkaInvoker(config KafkaConfig) *KafkaInvoker {
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 30 * time.Second
	}
	return &KafkaInvoker{
		config: config,
	}
}

// kafkaReplyConsumer manages a reply subscription for request/reply.
type kafkaReplyConsumer struct {
	replyCh chan []byte
	cancel  context.CancelFunc
}

// Invoke sends a request to a Kafka topic and waits for a correlated reply.
func (k *KafkaInvoker) Invoke(ctx context.Context, service, method string, body []byte, endpoint *Endpoint) ([]byte, error) {
	if endpoint == nil {
		return nil, fmt.Errorf("kafka: nil endpoint")
	}
	if endpoint.Topic == "" {
		return nil, fmt.Errorf("kafka: empty topic for service %s", service)
	}

	// Generate correlation ID for request/reply
	correlationID := uuid.New().String()
	replyTopic := endpoint.ReplyTopic
	if replyTopic == "" {
		replyTopic = fmt.Sprintf("__reply_%s", endpoint.Topic)
	}

	// Create request envelope
	request := &KafkaRequest{
		CorrelationID: correlationID,
		Service:       service,
		Method:        method,
		Body:          body,
		ReplyTopic:    replyTopic,
		CreatedAt:     time.Now(),
	}

	reqBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("kafka: marshal request: %w", err)
	}

	// Get or create producer for the request topic
	producer, err := k.getOrCreateProducer(endpoint.Topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: get producer: %w", err)
	}

	// Set up reply subscription before sending request
	replyCh, cancel := k.subscribeToReply(ctx, replyTopic, correlationID)
	defer cancel()

	// Send request
	if err := producer.Send(ctx, []byte(correlationID), reqBytes); err != nil {
		return nil, fmt.Errorf("kafka: send request: %w", err)
	}

	slog.Debug("kafka: request sent",
		"service", service,
		"method", method,
		"topic", endpoint.Topic,
		"correlation_id", correlationID)

	// Wait for reply with timeout
	select {
	case reply := <-replyCh:
		var resp KafkaResponse
		if err := json.Unmarshal(reply, &resp); err != nil {
			return nil, fmt.Errorf("kafka: unmarshal response: %w", err)
		}
		if resp.Error != "" {
			return nil, fmt.Errorf("kafka: service error: %s", resp.Error)
		}
		return resp.Body, nil

	case <-ctx.Done():
		return nil, fmt.Errorf("kafka: request timed out: %w", ctx.Err())

	case <-time.After(k.config.RequestTimeout):
		return nil, fmt.Errorf("kafka: request timed out after %s", k.config.RequestTimeout)
	}
}

// KafkaRequest is the request envelope for Kafka request/reply.
type KafkaRequest struct {
	CorrelationID string    `json:"correlation_id"`
	Service       string    `json:"service"`
	Method        string    `json:"method"`
	Body          []byte    `json:"body"`
	ReplyTopic    string    `json:"reply_topic"`
	CreatedAt     time.Time `json:"created_at"`
}

// KafkaResponse is the response envelope for Kafka request/reply.
type KafkaResponse struct {
	CorrelationID string `json:"correlation_id"`
	Body          []byte `json:"body"`
	Error         string `json:"error,omitempty"`
}

// getOrCreateProducer returns a shared producer for the topic.
func (k *KafkaInvoker) getOrCreateProducer(topic string) (KafkaProducer, error) {
	if val, ok := k.producers.Load(topic); ok {
		return val.(KafkaProducer), nil
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	// Double-check after acquiring lock
	if val, ok := k.producers.Load(topic); ok {
		return val.(KafkaProducer), nil
	}

	// Create new producer (in real implementation, this would use sarama)
	// For now, return a placeholder that will be replaced with real Kafka
	producer := &noopProducer{topic: topic}
	k.producers.Store(topic, producer)
	return producer, nil
}

// subscribeToReply sets up a temporary subscription for the correlation reply.
func (k *KafkaInvoker) subscribeToReply(ctx context.Context, replyTopic, correlationID string) (<-chan []byte, context.CancelFunc) {
	replyCh := make(chan []byte, 1)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(replyCh)
		defer cancel()

		// In real implementation, this would subscribe to the reply topic
		// and filter by correlationID. For now, this is a placeholder.
		select {
		case <-ctx.Done():
			return
		}
	}()

	return replyCh, cancel
}

// noopProducer is a placeholder for testing.
type noopProducer struct {
	topic string
}

func (p *noopProducer) Send(ctx context.Context, key, value []byte) error {
	slog.Debug("kafka: noop producer send", "topic", p.topic, "key", string(key))
	return nil
}

func (p *noopProducer) Close() {}
