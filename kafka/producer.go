package kafka

import (
	"errors"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

// ErrProducerClosed is returned when publishing through a Producer that has
// been closed.
var ErrProducerClosed = errors.New("kafka producer is closed")

// Producer publishes messages synchronously. It is safe for concurrent use:
// the underlying Sarama sync producer is, so calls are not serialized and
// concurrent publishers share its batching and in-flight requests.
type Producer struct {
	logger   Logger
	producer sarama.SyncProducer

	// mu guards closed. Publishers hold it for reading, so they do not block
	// each other; Close holds it for writing, so it waits for publishes in
	// flight, and none can start once it has run. Sarama panics on a send to a
	// closed producer and again on a second close, so neither may happen.
	mu     sync.RWMutex
	closed bool
}

func NewProducer(config *ProducerConfig, opts ...Options[*Producer]) (*Producer, error) {
	if config == nil {
		return nil, errors.New("kafka producer config is required")
	}

	producer, err := sarama.NewSyncProducer(config.Brokers, config.ProducerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	p := &Producer{
		producer: producer,
		logger:   defaultLogger(),
	}

	applyOptions(p, opts)

	return p, nil
}

func (p *Producer) PublishMessage(message Message) error {
	_, err := p.PublishMessageWithMetadata(message)

	return err
}

// PublishMessageWithMetadata is PublishMessage that also reports where the
// message was written. It is safe for concurrent use, and returns
// ErrProducerClosed once the producer is closed.
func (p *Producer) PublishMessageWithMetadata(message Message) (Metadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return Metadata{}, ErrProducerClosed
	}

	msg, err := toProducerMessage(message)
	if err != nil {
		return Metadata{}, err
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return Metadata{}, err
	}

	// Monitoring: message published to the broker.
	p.logger.Debugf("kafka published message: topic=%s key=%q partition=%d offset=%d", message.Topic, message.Key, partition, offset)

	return Metadata{
		Topic:     message.Topic,
		Key:       message.Key,
		Partition: partition,
		Offset:    offset,
	}, nil
}

// PublishMessages sends messages in one call, to any mix of topics. Every
// message is validated before anything is sent, so an invalid message sends
// none of them. It returns ErrProducerClosed once the producer is closed.
//
// A send failure returns a sarama.ProducerErrors listing the messages that
// failed; the others in the call may already have been written. Retrying the
// whole call after such an error can duplicate the messages that succeeded.
func (p *Producer) PublishMessages(messages []Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrProducerClosed
	}

	if len(messages) == 0 {
		return nil
	}

	kafkaMessages := make([]*sarama.ProducerMessage, 0, len(messages))
	for _, message := range messages {
		msg, err := toProducerMessage(message)
		if err != nil {
			return err
		}

		kafkaMessages = append(kafkaMessages, msg)
	}

	if err := p.producer.SendMessages(kafkaMessages); err != nil {
		return err
	}

	// Monitoring: message batch published to the broker.
	p.logger.Debugf("kafka published message batch: count=%d", len(messages))

	return nil
}

// Close waits for the publishes in flight, then closes the underlying
// producer. It is safe to call more than once and from several goroutines;
// calls after the first do nothing and return nil. Publishing after Close
// returns ErrProducerClosed.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			return fmt.Errorf("failed to close producer: %w", err)
		}
	}

	return nil
}
