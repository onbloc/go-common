package kafka

// Options configures a *Producer or *Consumer after construction.
type Options[T any] func(T)

func applyOptions[T any](t T, opts []Options[T]) {
	for _, opt := range opts {
		opt(t)
	}
}

// WithProducerLogger sets the logger used for publish activity. A nil logger
// is ignored, leaving the default no-op logger in place.
func WithProducerLogger(logger Logger) Options[*Producer] {
	return func(p *Producer) {
		if logger != nil {
			p.logger = logger
		}
	}
}

// WithConsumerLogger sets the logger used for consume activity. A nil logger
// is ignored, leaving the default no-op logger in place.
func WithConsumerLogger(logger Logger) Options[*Consumer] {
	return func(c *Consumer) {
		if logger != nil {
			c.logger = logger
		}
	}
}
