package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	Topic string
	Key   string
	// Value is the raw message payload. A nil Value publishes a Kafka
	// tombstone (a null-value record used to mark a key for deletion on a
	// compacted topic); a non-nil, possibly empty, Value publishes normally.
	Value []byte
	// Headers are optional record headers. Header keys must not be empty.
	Headers []Header
}

// Metadata describes where a message was written or read.
type Metadata struct {
	Topic     string
	Key       string
	Partition int32
	Offset    int64
	// Headers and Timestamp are set on received messages. Timestamp is the
	// record time (producer create time or broker log-append time, depending
	// on the topic's configuration).
	Headers   []Header
	Timestamp time.Time
}

type BatchConfig struct {
	MaxMessages   int
	FlushInterval time.Duration
}

type BatchMessage struct {
	Value    []byte
	Metadata Metadata
}

// Header is a Kafka record header: out-of-band key/value metadata carried
// with a message (for example a trace ID or a schema version). Keys may
// repeat; order is preserved.
type Header struct {
	Key   string
	Value []byte
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

// The handler types receive a commit function. Calling commit stores the
// consumer group's offset for every message of the batch (one message for
// StartListening), marking them as processed. The consumer never commits on
// its own, so a message whose batch is not committed is delivered again after a
// restart or rebalance.
//
// Call commit last, once processing has succeeded: it cannot be undone, so
// returning an error after commit does not bring the messages back. Committing
// a batch also commits every earlier offset of that partition. commit
// currently always returns nil, because Sarama does not report commit failures.
type (
	// MessageHandler handles one message.
	MessageHandler func(message []byte, metadata Metadata, commit func() error) error
	// BatchMessageHandler handles a batch of messages from one topic partition.
	BatchMessageHandler func(messages []BatchMessage, commit func() error) error
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

// toProducerMessage validates message and converts it to a Sarama message.
func toProducerMessage(message Message) (*sarama.ProducerMessage, error) {
	if message.Topic == "" {
		return nil, errors.New("kafka topic is required")
	}

	msg := &sarama.ProducerMessage{
		Topic: message.Topic,
		Value: sarama.ByteEncoder(message.Value),
	}

	if message.Key != "" {
		msg.Key = sarama.StringEncoder(message.Key)
	}

	for _, header := range message.Headers {
		if header.Key == "" {
			return nil, errors.New("kafka header key is required")
		}

		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(header.Key), Value: header.Value})
	}

	return msg, nil
}

// fromRecordHeaders converts Sarama record headers, preserving order and
// duplicate keys. It returns nil when there are none.
func fromRecordHeaders(records []*sarama.RecordHeader) []Header {
	if len(records) == 0 {
		return nil
	}

	headers := make([]Header, 0, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}

		headers = append(headers, Header{Key: string(record.Key), Value: record.Value})
	}

	return headers
}
