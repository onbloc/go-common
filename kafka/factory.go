package kafka

// NewPublisher builds a Publisher from Config, using log for publish
// activity. A nil log discards output. On error the Publisher is nil.
func NewPublisher(conf *Config, log Logger) (Publisher, error) {
	producerConfig, err := NewProducerConfigBy(conf)
	if err != nil {
		return nil, err
	}

	// Return the concrete producer only on success: returning NewProducer's
	// result directly would wrap a nil *Producer into a non-nil Publisher.
	producer, err := NewProducer(producerConfig, WithProducerLogger(log))
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// NewSubscriber builds a Subscriber from Config, using log for consume
// activity. A nil log discards output. Further options, such as
// WithConsumerErrorHandler, are applied after the logger. When
// Config.Consumer.Concurrency is greater than 1, it returns a
// ConcurrentConsumer running that many consumer instances in the same group,
// each with the same options. On error the Subscriber is nil.
func NewSubscriber(conf *Config, log Logger, opts ...Options[*Consumer]) (Subscriber, error) {
	consumerConfig, err := NewConsumerConfigBy(conf)
	if err != nil {
		return nil, err
	}

	opts = append([]Options[*Consumer]{WithConsumerLogger(log)}, opts...)

	// As in NewPublisher, never wrap a nil concrete pointer into the interface.
	if consumerConfig.Concurrency <= 1 {
		var consumer *Consumer
		if consumer, err = NewConsumer(consumerConfig, opts...); err != nil {
			return nil, err
		}

		return consumer, nil
	}

	consumers, err := NewConcurrentConsumer(consumerConfig, opts...)
	if err != nil {
		return nil, err
	}

	return consumers, nil
}
