package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	Topic string
	// Key is the record key, raw bytes. A nil Key sends no key (a null key),
	// and the message goes to a partition without ordering guarantees. An
	// empty non-nil Key is a real key like any other: it is hashed, so all
	// messages with an empty key land in the same partition.
	Key []byte
	// Value is the raw message payload. A nil Value publishes a Kafka
	// tombstone (a null-value record used to mark a key for deletion on a
	// compacted topic); a non-nil, possibly empty, Value publishes normally.
	Value []byte
	// Headers are optional record headers. Header keys must not be empty.
	Headers []Header
}

// Metadata describes where a message was written or read.
type Metadata struct {
	Topic string
	// Key is the record key. It is nil when the record has no key, which is
	// not the same as an empty key.
	Key       []byte
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

// The handler types receive the consumer group session's context and a commit
// function.
//
// ctx is the session's context. It ends when a rebalance or a shutdown ends the
// session, and also when the context given to StartListening or
// StartBatchListening ends. Pass it to the work the handler starts (database
// transactions, calls to other services) so that work stops when this consumer
// loses its partitions: a rebalance waits for running handlers to return, and a
// handler that ignores ctx delays it, and may keep working on messages that now
// belong to another consumer. The context given to Start* is not a substitute,
// because it stays live through a rebalance.
//
// Calling commit stores the consumer group's offset for every message of the
// batch (one message for StartListening), marking them as processed. The
// consumer never commits on its own, so a message whose batch is not committed
// is delivered again after a restart or rebalance.
//
// Call commit last, once processing has succeeded: it cannot be undone, so
// returning an error after commit does not bring the messages back. Committing
// a batch also commits every earlier offset of that partition.
//
// Once the session has ended, commit marks and commits nothing and returns the
// context's error, so a handler cannot mistake a lost session for a stored
// offset; the batch is delivered again. Otherwise it returns nil, because
// Sarama does not report broker commit failures.
//
// Prefer calling commit from the handler, before it returns. A commit may be
// kept and called later, for example when processing finishes on another
// goroutine, and it still commits its own batch, as long as the session is
// still live.
//
// A handler error that is only the session's own cancellation (ctx.Err(), or an
// error wrapping it) is a normal stop, not a failure: it is not logged or
// reported, and the consumer rejoins at once.
type (
	// MessageHandler handles one message.
	MessageHandler func(ctx context.Context, message []byte, metadata Metadata, commit func() error) error
	// BatchMessageHandler handles a batch of messages from one topic partition.
	BatchMessageHandler func(ctx context.Context, messages []BatchMessage, commit func() error) error
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

	// A nil key is sent as a null key, an empty non-nil key as an empty one:
	// Kafka tells them apart, and so do partitioning and log compaction.
	if message.Key != nil {
		msg.Key = sarama.ByteEncoder(message.Key)
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
