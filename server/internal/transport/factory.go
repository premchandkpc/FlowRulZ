package transport

import (
	"context"
	"sync"
)

// TransportKind identifies a transport backend type.
type TransportKind string

const (
	KindKafka   TransportKind = "kafka"
	KindCluster TransportKind = "cluster"
	KindMemory  TransportKind = "memory"
	KindNoop    TransportKind = "noop"
)

// ProducerFactory creates MessageProducer instances for a given topic.
type ProducerFactory func(topic string) MessageProducer

// ConsumerFactory creates MessageConsumer instances for a given topic.
type ConsumerFactory func(topic string, handler MessageHandler) MessageConsumer

// TransportFactory creates transport producers and consumers based on config.
type TransportFactory struct {
	mu                sync.RWMutex
	producerFactories map[TransportKind]ProducerFactory
	consumerFactories map[TransportKind]ConsumerFactory
	kind              TransportKind
}

// NewTransportFactory creates a TransportFactory with the given backend kind.
func NewTransportFactory(kind TransportKind) *TransportFactory {
	return &TransportFactory{
		producerFactories: make(map[TransportKind]ProducerFactory),
		consumerFactories: make(map[TransportKind]ConsumerFactory),
		kind:              kind,
	}
}

// RegisterProducer registers a producer factory for a transport kind.
func (f *TransportFactory) RegisterProducer(kind TransportKind, factory ProducerFactory) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.producerFactories[kind] = factory
}

// RegisterConsumer registers a consumer factory for a transport kind.
func (f *TransportFactory) RegisterConsumer(kind TransportKind, factory ConsumerFactory) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumerFactories[kind] = factory
}

// SetKind changes the active transport kind.
func (f *TransportFactory) SetKind(kind TransportKind) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kind = kind
}

// Kind returns the current transport kind.
func (f *TransportFactory) Kind() TransportKind {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.kind
}

// NewProducer creates a producer using the active transport kind.
func (f *TransportFactory) NewProducer(topic string) MessageProducer {
	f.mu.RLock()
	factory, ok := f.producerFactories[f.kind]
	f.mu.RUnlock()

	if !ok {
		// Fallback to noop
		return &noopProducer{topic: topic}
	}
	return factory(topic)
}

// NewConsumer creates a consumer using the active transport kind.
func (f *TransportFactory) NewConsumer(topic string, handler MessageHandler) MessageConsumer {
	f.mu.RLock()
	factory, ok := f.consumerFactories[f.kind]
	f.mu.RUnlock()

	if !ok {
		// Fallback to noop
		return &noopConsumer{topic: topic}
	}
	return factory(topic, handler)
}

// noopProducer is a no-op producer that discards messages.
type noopProducer struct {
	topic string
}

func (p *noopProducer) Send(_ context.Context, _ []byte, _ []byte) error { return nil }
func (p *noopProducer) Close()                                           {}

// noopConsumer is a no-op consumer that does nothing.
type noopConsumer struct {
	topic string
}

func (c *noopConsumer) Topic() string           { return c.topic }
func (c *noopConsumer) Start(_ context.Context) {}
func (c *noopConsumer) Stop()                   {}
