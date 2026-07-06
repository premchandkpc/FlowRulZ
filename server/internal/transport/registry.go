package transport

// RegisterMemory registers in-memory producer and consumer factories.
func RegisterMemory(factory *TransportFactory) {
	factory.RegisterProducer(KindMemory, func(topic string) MessageProducer {
		return NewProducer(topic)
	})

	factory.RegisterConsumer(KindMemory, func(topic string, handler MessageHandler) MessageConsumer {
		return NewConsumer(topic, handler)
	})
}
