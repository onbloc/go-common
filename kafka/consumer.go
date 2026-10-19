package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// defaultRetryBackoff is how long a consumer waits before it consumes again
// after an error or a failed handler.
const defaultRetryBackoff = time.Second

type Consumer struct {
	logger        Logger
	consumerGroup sarama.ConsumerGroup
	ctx           context.Context
	cancel        context.CancelFunc
	topics        []string
	errorHandler  func(*ConsumerError)
	retryBackoff  time.Duration
}

func NewConsumer(config *ConsumerConfig, opts ...Options[*Consumer]) (*Consumer, error) {
	if config == nil {
		return nil, errors.New("kafka consumer config is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		topics:       config.Topics,
		logger:       defaultLogger(),
		ctx:          ctx,
		cancel:       cancel,
		retryBackoff: defaultRetryBackoff,
	}

	// Options come first: the error handler decides how the group is configured.
	applyOptions(c, opts)

	consumerGroup, err := sarama.NewConsumerGroup(config.Brokers, config.ConsumerGroup, c.groupConfig(config.ConsumerConfig))
	if err != nil {
		cancel()

		return nil, fmt.Errorf("failed to create Kafka consumer group: %w", err)
	}

	c.consumerGroup = consumerGroup

	if c.errorHandler != nil {
		go c.forwardGroupErrors()
	}

	return c, nil
}

func (c *Consumer) StartListening(ctx context.Context, handler MessageHandler) error {
	return c.StartBatchListening(ctx, BatchConfig{MaxMessages: 1}, func(messages []BatchMessage, commit func() error) error {
		for _, message := range messages {
			if err := handler(message.Value, message.Metadata, commit); err != nil {
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
			handlerFailed := consumerHandler.failed.Swap(false)

			if err != nil {
				if stopErr := c.stopErr(listenerCtx); stopErr != nil {
					return stopErr
				}

				c.logger.Errorf("Error consuming batch messages: %v", err)
				c.reportError(newGroupError(err))
			}

			// A normal rebalance rejoins at once. After a failure, wait: a handler
			// error ends the session without Consume returning an error, and a
			// message that always fails would otherwise rejoin and rebalance the
			// group in a tight loop.
			if err != nil || handlerFailed {
				select {
				case <-listenerCtx.Done():
					return listenerCtx.Err()
				case <-time.After(c.retryBackoff):
				}
			}
		}
	}
}

// stopErr reports why listening must end, or nil. Close cancels c.ctx before it
// closes the consumer group, but listenerCtx only follows c.ctx through a
// separate goroutine, so it may not be cancelled yet when Consume reports the
// closed group. Either one is a stop, not a consume failure.
func (c *Consumer) stopErr(listenerCtx context.Context) error {
	if err := listenerCtx.Err(); err != nil {
		return err
	}

	return c.ctx.Err()
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
	if config == nil {
		return nil, errors.New("kafka consumer config is required")
	}

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
	return c.StartBatchListening(ctx, BatchConfig{MaxMessages: 1}, func(messages []BatchMessage, commit func() error) error {
		for _, message := range messages {
			if err := handler(message.Value, message.Metadata, commit); err != nil {
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
	// failed is set when the message handler returned an error, which ends the
	// group session. The consume loop reads it to back off before rejoining.
	failed atomic.Bool
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
			return nil
		}

		var timerC <-chan time.Time
		if len(batch) > 0 {
			timerC = timer.C
		}

		select {
		case <-session.Context().Done():
			// The session ended (rebalance or shutdown). Any unflushed batch is
			// dropped without a commit, so it is redelivered. This is the normal
			// way a claim stops, not an error.
			return nil
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
			Value: msg.Value,
			Metadata: Metadata{
				Topic:     msg.Topic,
				Key:       msg.Key,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Headers:   fromRecordHeaders(msg.Headers),
				Timestamp: msg.Timestamp,
			},
		})
	}

	topic := messages[0].Topic
	firstOffset := messages[0].Offset
	lastOffset := messages[len(messages)-1].Offset

	// Monitoring: message batch received from the broker (before handling).
	h.logger.Debugf("kafka received message batch: topic=%s count=%d offsets=%d..%d", topic, len(messages), firstOffset, lastOffset)

	// A batch holds consecutive messages of one topic partition, and marking a
	// message commits every offset before it too, so marking the last one
	// commits the batch. Capture that message now: messages is the claim's
	// buffer, which is reused for the next batch once the handler returns, so a
	// commit that outlives this call must not read it.
	last := messages[len(messages)-1]
	commit := func() error {
		session.MarkMessage(last, "")
		session.Commit()

		return nil
	}

	if err := h.metadataHandler(batchMessages, commit); err != nil {
		h.logger.Errorf("Failed to process message batch: topic=%s count=%d offsets=%d..%d error=%v", topic, len(messages), firstOffset, lastOffset, err)
		h.failed.Store(true)

		return err
	}

	// Monitoring: message batch handled successfully (function processed).
	h.logger.Debugf("kafka processed message batch: topic=%s count=%d offsets=%d..%d", topic, len(messages), firstOffset, lastOffset)

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
