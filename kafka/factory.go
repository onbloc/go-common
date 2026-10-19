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
// activity. A nil log discards output. Further options, such as
// WithConsumerErrorHandler, are applied after the logger. When
// Config.Consumer.Concurrency is greater than 1, it returns a
// ConcurrentConsumer running that many consumer instances in the same group,
// each with the same options.
func NewSubscriber(conf *Config, log Logger, opts ...Options[*Consumer]) (Subscriber, error) {
	consumerConfig, err := NewConsumerConfigBy(conf)
	if err != nil {
		return nil, err
	}

	opts = append([]Options[*Consumer]{WithConsumerLogger(log)}, opts...)

	if consumerConfig.Concurrency <= 1 {
		return NewConsumer(consumerConfig, opts...)
	}

	return NewConcurrentConsumer(consumerConfig, opts...)
}
