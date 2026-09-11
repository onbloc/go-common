package kafka

import (
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

// fakeSyncProducer is a minimal sarama.SyncProducer test double. Embedding
// the interface promotes the transaction-related methods our code never
// calls, so only SendMessage/SendMessages/Close need real implementations.
type fakeSyncProducer struct {
	sarama.SyncProducer

	messages []*sarama.ProducerMessage
	sendErr  error
	closeErr error
	closed   bool
}

func (f *fakeSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if f.sendErr != nil {
		return 0, 0, f.sendErr
	}

	f.messages = append(f.messages, msg)

	return 0, int64(len(f.messages) - 1), nil
}

func (f *fakeSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	if f.sendErr != nil {
		return f.sendErr
	}

	f.messages = append(f.messages, msgs...)

	return nil
}

func (f *fakeSyncProducer) Close() error {
	f.closed = true

	return f.closeErr
}

func newTestProducer(fake *fakeSyncProducer) *Producer {
	return &Producer{producer: fake, logger: defaultLogger()}
}

func TestPublishMessageRejectsEmptyTopic(t *testing.T) {
	fake := &fakeSyncProducer{}
	p := newTestProducer(fake)

	if err := p.PublishMessage(Message{Value: []byte("value")}); err == nil {
		t.Fatal("expected error for empty topic")
	}
	if len(fake.messages) != 0 {
		t.Fatalf("messages sent = %d, want 0", len(fake.messages))
	}
}

func TestPublishMessagePublishesNilValueAsTombstone(t *testing.T) {
	fake := &fakeSyncProducer{}
	p := newTestProducer(fake)

	if err := p.PublishMessage(Message{Topic: "topic", Key: "key"}); err != nil {
		t.Fatalf("PublishMessage() error = %v", err)
	}

	if len(fake.messages) != 1 {
		t.Fatalf("messages sent = %d, want 1", len(fake.messages))
	}

	value, err := fake.messages[0].Value.Encode()
	if err != nil {
		t.Fatalf("Value.Encode() error = %v", err)
	}
	if value != nil {
		t.Fatalf("Value = %q, want nil (tombstone)", value)
	}
}

func TestPublishMessagesRejectsEmptyTopic(t *testing.T) {
	fake := &fakeSyncProducer{}
	p := newTestProducer(fake)

	err := p.PublishMessages([]Message{{Topic: "topic", Value: []byte("a")}, {Value: []byte("b")}})
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
	if len(fake.messages) != 0 {
		t.Fatalf("messages sent = %d, want 0", len(fake.messages))
	}
}

func TestPublishMessagesPublishesNilAndEmptyValues(t *testing.T) {
	fake := &fakeSyncProducer{}
	p := newTestProducer(fake)

	err := p.PublishMessages([]Message{
		{Topic: "topic", Value: nil},
		{Topic: "topic", Value: []byte{}},
	})
	if err != nil {
		t.Fatalf("PublishMessages() error = %v", err)
	}

	if len(fake.messages) != 2 {
		t.Fatalf("messages sent = %d, want 2", len(fake.messages))
	}

	tombstone, err := fake.messages[0].Value.Encode()
	if err != nil {
		t.Fatalf("Value.Encode() error = %v", err)
	}
	if tombstone != nil {
		t.Fatalf("first Value = %q, want nil (tombstone)", tombstone)
	}

	empty, err := fake.messages[1].Value.Encode()
	if err != nil {
		t.Fatalf("Value.Encode() error = %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("second Value = %v, want non-nil empty slice", empty)
	}
}

func TestProducerCloseClosesUnderlyingProducer(t *testing.T) {
	fake := &fakeSyncProducer{}
	p := newTestProducer(fake)

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !fake.closed {
		t.Fatal("underlying producer was not closed")
	}
}

func TestProducerCloseWrapsUnderlyingError(t *testing.T) {
	wantErr := errors.New("boom")
	p := newTestProducer(&fakeSyncProducer{closeErr: wantErr})

	err := p.Close()
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("Close() error = %v, want wrapped %v", err, wantErr)
	}
}
