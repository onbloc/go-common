package kafka

import (
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

type Producer struct {
	logger   Logger
	producer sarama.SyncProducer
	mu       sync.Mutex
}

func NewProducer(config *ProducerConfig, opts ...Options[*Producer]) (*Producer, error) {
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

func (p *Producer) PublishMessageWithMetadata(message Message) (Metadata, error) {
	if message.Value == "" {
		return Metadata{}, fmt.Errorf("message value cannot be empty")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if message.Topic == "" {
		return Metadata{}, fmt.Errorf("kafka topic is required")
	}

	msg := &sarama.ProducerMessage{
		Topic: message.Topic,
		Value: sarama.StringEncoder(message.Value),
	}

	if message.Key != "" {
		msg.Key = sarama.StringEncoder(message.Key)
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return Metadata{}, err
	}

	// Monitoring: message published to the broker.
	p.logger.Infof("kafka published message: topic=%s key=%s partition=%d offset=%d", message.Topic, message.Key, partition, offset)

	return Metadata{
		Topic:     message.Topic,
		Key:       message.Key,
		Partition: partition,
		Offset:    offset,
	}, nil
}

func (p *Producer) PublishMessages(messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	kafkaMessages := make([]*sarama.ProducerMessage, 0, len(messages))
	for _, message := range messages {
		if message.Topic == "" {
			return fmt.Errorf("kafka topic is required")
		}

		msg := &sarama.ProducerMessage{
			Topic: message.Topic,
			Value: sarama.StringEncoder(message.Value),
		}

		if message.Key != "" {
			msg.Key = sarama.StringEncoder(message.Key)
		}

		kafkaMessages = append(kafkaMessages, msg)
	}

	if err := p.producer.SendMessages(kafkaMessages); err != nil {
		return err
	}

	// Monitoring: message batch published to the broker.
	p.logger.Infof("kafka published message batch: topic=%s count=%d", messages[0].Topic, len(messages))

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
