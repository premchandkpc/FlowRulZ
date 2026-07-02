package messaging

import "fmt"

type PublisherFactory func(Config) (Publisher, error)
type SubscriberFactory func(Config) (Subscriber, error)
type ConsumerFactory func(topic string, handler Handler, cfg Config) (Consumer, error)
type BusFactory func(Config) (Bus, error)

var (
	publisherFactories = map[string]PublisherFactory{}
	subscriberFactories = map[string]SubscriberFactory{}
	consumerFactories   = map[string]ConsumerFactory{}
	busFactories        = map[string]BusFactory{}
)

func RegisterPublisher(typ string, factory PublisherFactory) {
	publisherFactories[typ] = factory
}

func RegisterSubscriber(typ string, factory SubscriberFactory) {
	subscriberFactories[typ] = factory
}

func RegisterConsumer(typ string, factory ConsumerFactory) {
	consumerFactories[typ] = factory
}

func RegisterBus(typ string, factory BusFactory) {
	busFactories[typ] = factory
}

func NewPublisher(cfg Config) (Publisher, error) {
	fn, ok := publisherFactories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("messaging: no publisher registered for %q", cfg.Type)
	}
	return fn(cfg)
}

func NewSubscriber(cfg Config) (Subscriber, error) {
	fn, ok := subscriberFactories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("messaging: no subscriber registered for %q", cfg.Type)
	}
	return fn(cfg)
}

func NewConsumer(topic string, handler Handler, cfg Config) (Consumer, error) {
	fn, ok := consumerFactories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("messaging: no consumer registered for %q", cfg.Type)
	}
	return fn(topic, handler, cfg)
}

func NewBus(cfg Config) (Bus, error) {
	fn, ok := busFactories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("messaging: no bus registered for %q", cfg.Type)
	}
	return fn(cfg)
}
