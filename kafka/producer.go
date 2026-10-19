package kafka

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// Producer publishes messages synchronously. It is safe for concurrent use:
// the underlying Sarama sync producer is, so calls are not serialized and
// concurrent publishers share its batching and in-flight requests.
type Producer struct {
	logger   Logger
	producer sarama.SyncProducer
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
// message was written. It is safe for concurrent use.
func (p *Producer) PublishMessageWithMetadata(message Message) (Metadata, error) {
	msg, err := toProducerMessage(message)
	if err != nil {
		return Metadata{}, err
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return Metadata{}, err
	}

	// Monitoring: message published to the broker.
	debugf(p.logger, "kafka published message: topic=%s key=%s partition=%d offset=%d", message.Topic, message.Key, partition, offset)

	return Metadata{
		Topic:     message.Topic,
		Key:       message.Key,
		Partition: partition,
		Offset:    offset,
	}, nil
}

// PublishMessages sends messages in one call, to any mix of topics. Every
// message is validated before anything is sent, so an invalid message sends
// none of them.
//
// A send failure returns a sarama.ProducerErrors listing the messages that
// failed; the others in the call may already have been written. Retrying the
// whole call after such an error can duplicate the messages that succeeded.
func (p *Producer) PublishMessages(messages []Message) error {
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
	if debugEnabled(p.logger) {
		debugf(p.logger, "kafka published message batch: topics=%d count=%d", distinctTopics(messages), len(messages))
	}

	return nil
}

func (p *Producer) Close() error {
	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			return fmt.Errorf("failed to close producer: %w", err)
		}
	}

	return nil
}

func distinctTopics(messages []Message) int {
	topics := make(map[string]struct{}, 1)
	for _, message := range messages {
		topics[message.Topic] = struct{}{}
	}

	return len(topics)
}
