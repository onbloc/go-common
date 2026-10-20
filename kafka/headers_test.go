package kafka

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestPublishMessageSendsHeadersInOrderIncludingDuplicates(t *testing.T) {
	fake := &fakeSyncProducer{}
	p := newTestProducer(fake)

	err := p.PublishMessage(Message{
		Topic: "topic",
		Value: []byte("v"),
		Headers: []Header{
			{Key: "trace-id", Value: []byte("abc")},
			{Key: "tag", Value: []byte("a")},
			{Key: "tag", Value: []byte("b")},
		},
	})
	if err != nil {
		t.Fatalf("PublishMessage() error = %v", err)
	}

	got := fake.messages[0].Headers
	want := []struct{ key, value string }{{"trace-id", "abc"}, {"tag", "a"}, {"tag", "b"}}
	if len(got) != len(want) {
		t.Fatalf("headers = %v, want %d entries", got, len(want))
	}
	for i := range want {
		if string(got[i].Key) != want[i].key || string(got[i].Value) != want[i].value {
			t.Fatalf("header[%d] = %s=%s, want %s=%s", i, got[i].Key, got[i].Value, want[i].key, want[i].value)
		}
	}
}

func TestPublishRejectsEmptyHeaderKey(t *testing.T) {
	fake := &fakeSyncProducer{}
	p := newTestProducer(fake)
	bad := Message{Topic: "topic", Value: []byte("v"), Headers: []Header{{Key: "", Value: []byte("x")}}}

	if err := p.PublishMessage(bad); err == nil {
		t.Fatal("PublishMessage() expected error for an empty header key")
	}

	// A bad message anywhere in a batch must send none of the batch.
	if err := p.PublishMessages([]Message{{Topic: "topic", Value: []byte("ok")}, bad}); err == nil {
		t.Fatal("PublishMessages() expected error for an empty header key")
	}
	if len(fake.messages) != 0 {
		t.Fatalf("messages sent = %d, want 0", len(fake.messages))
	}
}

func TestConsumeClaimDeliversHeadersAndTimestamp(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	rec := newBatchRecorder()
	h := newTestHandler(BatchConfig{MaxMessages: 1, FlushInterval: time.Hour}, rec.handler)

	done := runClaim(h, session, claim)

	sent := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	claim.messages <- &sarama.ConsumerMessage{
		Topic:     "topic",
		Offset:    1,
		Value:     []byte("v"),
		Timestamp: sent,
		Headers: []*sarama.RecordHeader{
			{Key: []byte("tag"), Value: []byte("a")},
			{Key: []byte("tag"), Value: []byte("b")},
		},
	}
	rec.waitForCall(t)
	close(claim.messages)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}

	meta := rec.batches[0][0].Metadata
	if !meta.Timestamp.Equal(sent) {
		t.Fatalf("Timestamp = %v, want %v", meta.Timestamp, sent)
	}
	if len(meta.Headers) != 2 || meta.Headers[0].Key != "tag" || string(meta.Headers[0].Value) != "a" || string(meta.Headers[1].Value) != "b" {
		t.Fatalf("Headers = %+v, want tag=a then tag=b", meta.Headers)
	}
}

func TestConsumeClaimLeavesHeadersNilWhenMessageHasNone(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	rec := newBatchRecorder()
	h := newTestHandler(BatchConfig{MaxMessages: 1, FlushInterval: time.Hour}, rec.handler)

	done := runClaim(h, session, claim)

	claim.send(0)
	rec.waitForCall(t)
	close(claim.messages)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}

	if headers := rec.batches[0][0].Metadata.Headers; headers != nil {
		t.Fatalf("Headers = %+v, want nil", headers)
	}
}
