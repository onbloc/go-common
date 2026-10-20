package kafka

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// noPartition is ConsumerError.Partition for errors that belong to the
// consumer group as a whole rather than to one partition.
const noPartition int32 = -1

// ConsumerError is an error met while consuming. Topic and Partition name the
// partition it belongs to; for an error of the consumer group as a whole (for
// example a failed join) Topic is empty and Partition is -1.
//
// It wraps the underlying error, so errors.Is and errors.As see through it. A
// handler error returned by your own handler arrives wrapped in a ConsumerError
// that names the partition the batch came from.
type ConsumerError struct {
	Topic     string
	Partition int32
	Err       error
}

func (e *ConsumerError) Error() string {
	if e.Topic == "" {
		return fmt.Sprintf("kafka consumer: %v", e.Err)
	}

	return fmt.Sprintf("kafka consumer %s[%d]: %v", e.Topic, e.Partition, e.Err)
}

func (e *ConsumerError) Unwrap() error { return e.Err }

func newGroupError(err error) *ConsumerError {
	return &ConsumerError{Partition: noPartition, Err: err}
}

// toConsumerError converts an error from Sarama's consumer group error stream.
func toConsumerError(err error) *ConsumerError {
	var saramaErr *sarama.ConsumerError
	if errors.As(err, &saramaErr) {
		return &ConsumerError{Topic: saramaErr.Topic, Partition: saramaErr.Partition, Err: saramaErr.Err}
	}

	return newGroupError(err)
}

// groupConfig returns the Sarama config to create the consumer group with.
// Sarama reports its own consume errors (fetch failures, errors returned by a
// handler) on the group's error stream only when Consumer.Return.Errors is set;
// otherwise they go to Sarama's package logger. It is enabled, on a copy that
// leaves the caller's config untouched, only when an error handler is
// registered, so without one nothing changes.
func (c *Consumer) groupConfig(sc *sarama.Config) *sarama.Config {
	if c.errorHandler == nil {
		return sc
	}

	if sc == nil {
		sc = sarama.NewConfig()
	} else {
		cp := *sc
		sc = &cp
	}

	sc.Consumer.Return.Errors = true

	return sc
}

func (c *Consumer) reportError(err *ConsumerError) {
	if c.errorHandler != nil {
		c.errorHandler(err)
	}
}

// forwardGroupErrors passes the group's error stream to the error handler. It
// ends when the group is closed, which closes the stream.
func (c *Consumer) forwardGroupErrors() {
	for err := range c.consumerGroup.Errors() {
		c.reportError(toConsumerError(err))
	}
}
