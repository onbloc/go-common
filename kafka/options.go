package kafka

// Options configures a *Producer or *Consumer after construction.
type Options[T any] func(T)

func applyOptions[T any](t T, opts []Options[T]) {
	for _, opt := range opts {
		opt(t)
	}
}

// Logger is the logging surface this package needs. Any application logger
// with these methods (e.g. zap's SugaredLogger, logrus) satisfies it without an
// adapter. Per-message and per-batch activity is logged through Debugf;
// failures are logged through Errorf.
type Logger interface {
	Errorf(format string, args ...any)
	Debugf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Errorf(string, ...any) {}
func (noopLogger) Debugf(string, ...any) {}

func defaultLogger() Logger {
	return noopLogger{}
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

// WithConsumerErrorHandler registers fn to observe the errors a consumer meets
// while consuming: errors from the consumer group (retried every second), errors
// Sarama reports for a partition, and errors returned by your own handler.
// A nil fn is ignored.
//
// It is for observation only (metrics, alerts, deciding to cancel the context):
// it does not change how the consumer retries, commits or ends a session.
// Without a handler these errors are only logged through the consumer's
// Logger, and Sarama's own errors only reach Sarama's package logger.
//
// fn is called from the consumer's goroutines, concurrently with your message
// handler and, for a ConcurrentConsumer or several partitions, with itself. It
// must be safe for concurrent use and return quickly: a slow fn delays
// consuming, and errors Sarama has no room to queue are dropped.
func WithConsumerErrorHandler(fn func(*ConsumerError)) Options[*Consumer] {
	return func(c *Consumer) {
		if fn != nil {
			c.errorHandler = fn
		}
	}
}
