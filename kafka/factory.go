package kafka

// NewPublisher builds a Publisher from Config, using log for publish
// activity. A nil log discards output.
func NewPublisher(conf *Config, log Logger) (Publisher, error) {
	producerConfig, err := NewProducerConfigBy(conf)
	if err != nil {
		return nil, err
	}

	return NewProducer(producerConfig, WithProducerLogger(log))
}

// NewSubscriber builds a Subscriber from Config, using log for consume
// activity. A nil log discards output. When Config.Consumer.Concurrency is
// greater than 1, it returns a ConcurrentConsumer running that many consumer
// instances in the same group.
func NewSubscriber(conf *Config, log Logger) (Subscriber, error) {
	consumerConfig, err := NewConsumerConfigBy(conf)
	if err != nil {
		return nil, err
	}

	if consumerConfig.Concurrency <= 1 {
		return NewConsumer(consumerConfig, WithConsumerLogger(log))
	}

	return NewConcurrentConsumer(consumerConfig, WithConsumerLogger(log))
}
