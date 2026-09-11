package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	logger        Logger
	consumerGroup sarama.ConsumerGroup
	ctx           context.Context
	cancel        context.CancelFunc
	topics        []string
}

func NewConsumer(config *ConsumerConfig, opts ...Options[*Consumer]) (*Consumer, error) {
	consumerGroup, err := sarama.NewConsumerGroup(config.Brokers, config.ConsumerGroup, config.ConsumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		consumerGroup: consumerGroup,
		topics:        config.Topics,
		logger:        defaultLogger(),
		ctx:           ctx,
		cancel:        cancel,
	}

	applyOptions(c, opts)

	return c, nil
}

func (c *Consumer) StartListening(ctx context.Context, handler MessageHandler) error {
	return c.StartBatchListening(ctx, BatchConfig{MaxMessages: 1}, func(messages []BatchMessage, ack func() error) error {
		for _, message := range messages {
			if err := handler(message.Value, message.Metadata, ack); err != nil {
				return err
			}
		}

		return nil
	})
}

func (c *Consumer) StartBatchListening(ctx context.Context, config BatchConfig, handler BatchMessageHandler) error {
	listenerCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() {
		select {
		case <-c.ctx.Done():
			stop()
		case <-listenerCtx.Done():
		}
	}()

	consumerHandler := &batchConsumerHandler{
		batchConfig:     config.normalized(),
		metadataHandler: handler,
		logger:          c.logger,
	}

	for {
		select {
		case <-listenerCtx.Done():
			return listenerCtx.Err()
		default:
			err := c.consumerGroup.Consume(listenerCtx, c.topics, consumerHandler)
			if err != nil {
				if listenerCtx.Err() != nil {
					return listenerCtx.Err()
				}

				c.logger.Errorf("Error consuming batch messages: %v", err)

				select {
				case <-listenerCtx.Done():
					return listenerCtx.Err()
				case <-time.After(1 * time.Second):
				}
			}
		}
	}
}

func (c *Consumer) Close() error {
	c.cancel()

	if c.consumerGroup != nil {
		if err := c.consumerGroup.Close(); err != nil {
			return fmt.Errorf("failed to close consumer group: %w", err)
		}
	}

	return nil
}

type ConcurrentConsumer struct {
	consumers []*Consumer
}

func NewConcurrentConsumer(config *ConsumerConfig, opts ...Options[*Consumer]) (*ConcurrentConsumer, error) {
	concurrency := config.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	consumers := make([]*Consumer, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		consumer, err := NewConsumer(config, opts...)
		if err != nil {
			for _, created := range consumers {
				_ = created.Close()
			}

			return nil, fmt.Errorf("failed to create Kafka consumer %d/%d: %w", i+1, concurrency, err)
		}

		consumers = append(consumers, consumer)
	}

	return &ConcurrentConsumer{consumers: consumers}, nil
}

func (c *ConcurrentConsumer) StartListening(ctx context.Context, handler MessageHandler) error {
	return c.StartBatchListening(ctx, BatchConfig{MaxMessages: 1}, func(messages []BatchMessage, ack func() error) error {
		for _, message := range messages {
			if err := handler(message.Value, message.Metadata, ack); err != nil {
				return err
			}
		}

		return nil
	})
}

func (c *ConcurrentConsumer) StartBatchListening(ctx context.Context, config BatchConfig, handler BatchMessageHandler) error {
	if len(c.consumers) == 0 {
		return fmt.Errorf("at least one Kafka consumer is required")
	}

	listenerCtx, stop := context.WithCancel(ctx)
	defer stop()

	errCh := make(chan error, len(c.consumers))
	for _, consumer := range c.consumers {
		go func() {
			errCh <- consumer.StartBatchListening(listenerCtx, config, handler)
		}()
	}

	var firstErr error
	for remaining := len(c.consumers); remaining > 0; remaining-- {
		err := <-errCh
		if err != nil && firstErr == nil {
			firstErr = err
			stop()
		}
	}

	return firstErr
}

func (c *ConcurrentConsumer) Close() error {
	var closeErr error
	for _, consumer := range c.consumers {
		if err := consumer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

type batchConsumerHandler struct {
	logger          Logger
	batchConfig     BatchConfig
	metadataHandler BatchMessageHandler
}

func (h *batchConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *batchConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *batchConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	config := h.batchConfig.normalized()
	batch := make([]*sarama.ConsumerMessage, 0, config.MaxMessages)
	timer := time.NewTimer(config.FlushInterval)
	stopTimer(timer)
	defer timer.Stop()

	resetTimer := func() {
		stopTimer(timer)
		timer.Reset(config.FlushInterval)
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := h.consumeBatch(session, batch); err != nil {
			return err
		}

		batch = batch[:0]
		stopTimer(timer)

		return nil
	}

	for {
		if session.Context().Err() != nil {
			return session.Context().Err()
		}

		var timerC <-chan time.Time
		if len(batch) > 0 {
			timerC = timer.C
		}

		select {
		case <-session.Context().Done():
			return session.Context().Err()
		case <-timerC:
			if err := flush(); err != nil {
				return err
			}
		case msg, ok := <-claim.Messages():
			if !ok {
				return flush()
			}

			if len(batch) == 0 {
				resetTimer()
			}

			batch = append(batch, msg)
			if len(batch) >= config.MaxMessages {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (h *batchConsumerHandler) consumeBatch(session sarama.ConsumerGroupSession, messages []*sarama.ConsumerMessage) error {
	batchMessages := make([]BatchMessage, 0, len(messages))
	for _, msg := range messages {
		batchMessages = append(batchMessages, BatchMessage{
			Value: string(msg.Value),
			Metadata: Metadata{
				Topic:     msg.Topic,
				Key:       string(msg.Key),
				Partition: msg.Partition,
				Offset:    msg.Offset,
			},
		})
	}

	topic := messages[0].Topic
	firstOffset := messages[0].Offset
	lastOffset := messages[len(messages)-1].Offset

	// Monitoring: message batch received from the broker (before handling).
	h.logger.Infof("kafka received message batch: topic=%s count=%d offsets=%d..%d", topic, len(messages), firstOffset, lastOffset)

	ack := func() error {
		for _, msg := range messages {
			session.MarkMessage(msg, "")
		}
		session.Commit()

		return nil
	}

	if err := h.metadataHandler(batchMessages, ack); err != nil {
		h.logger.Errorf("Failed to process message batch: topic=%s count=%d offsets=%d..%d error=%v", topic, len(messages), firstOffset, lastOffset, err)

		return err
	}

	// Monitoring: message batch handled successfully (function processed).
	h.logger.Infof("kafka processed message batch: topic=%s count=%d offsets=%d..%d", topic, len(messages), firstOffset, lastOffset)

	return nil
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
