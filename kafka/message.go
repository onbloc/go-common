package kafka

import (
	"context"
	"time"
)

type Message struct {
	Topic string
	Key   string
	// Value is the raw message payload. A nil Value publishes a Kafka
	// tombstone (a null-value record used to mark a key for deletion on a
	// compacted topic); a non-nil, possibly empty, Value publishes normally.
	Value []byte
}

type Metadata struct {
	Topic     string
	Key       string
	Partition int32
	Offset    int64
}

type BatchConfig struct {
	MaxMessages   int
	FlushInterval time.Duration
}

type BatchMessage struct {
	Value    []byte
	Metadata Metadata
}

type Publisher interface {
	PublishMessage(message Message) error
	PublishMessages(messages []Message) error
	Close() error
}

type Subscriber interface {
	StartListening(ctx context.Context, handler MessageHandler) error
	StartBatchListening(ctx context.Context, config BatchConfig, handler BatchMessageHandler) error
	Close() error
}

type (
	MessageHandler      func(message []byte, metadata Metadata, ack func() error) error
	BatchMessageHandler func(messages []BatchMessage, ack func() error) error
)

func (c BatchConfig) normalized() BatchConfig {
	if c.MaxMessages <= 0 {
		c.MaxMessages = 1
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}

	return c
}
