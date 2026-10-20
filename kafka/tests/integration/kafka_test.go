package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"

	commonkafka "github.com/onbloc/go-common/kafka"
)

var sharedKafkaBroker string

const (
	defaultTimeout   = 30 * time.Second
	largeTimeout     = 60 * time.Second
	messageCount     = 20
	largeMessageSize = 4 * 1024 * 1024 // 4MB
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(
		ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to start kafka container: %v", err))
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		if terminateErr := kafkaContainer.Terminate(ctx); terminateErr != nil {
			fmt.Printf("failed to terminate kafka container: %v\n", terminateErr)
		}
		panic(fmt.Sprintf("failed to get kafka brokers: %v", err))
	}

	sharedKafkaBroker = brokers[0]

	code := m.Run()

	if err := kafkaContainer.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate kafka container: %v\n", err)
	}

	os.Exit(code)
}

func TestKafkaIntegration_PublishAndConsume(t *testing.T) {
	env := newTestEnv(t, "test-topic-basic", "test-consumer-group")

	testMessage := "Hello, Kafka!"
	env.publishMessage(t, testMessage)

	received := env.consumeOne(t, defaultTimeout)
	if received != testMessage {
		t.Errorf("message mismatch: expected %q, got %q", testMessage, received)
	}
}

func TestKafkaIntegration_HeadersAndTimestampRoundTrip(t *testing.T) {
	env := newTestEnv(t, "test-topic-headers", "test-consumer-group-headers")

	before := time.Now().Add(-time.Minute)
	err := env.producer.PublishMessage(commonkafka.Message{
		Topic: env.topic,
		Key:   []byte("key-1"),
		Value: []byte("payload"),
		Headers: []commonkafka.Header{
			{Key: "trace-id", Value: []byte("abc")},
			{Key: "tag", Value: []byte("a")},
			{Key: "tag", Value: []byte("b")},
		},
	})
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	received := make(chan commonkafka.Metadata, 1)
	go func() {
		err := env.consumer.StartListening(ctx, func(_ context.Context, _ []byte, meta commonkafka.Metadata, commit func() error) error {
			received <- meta

			return commit()
		})
		if err != nil && ctx.Err() == nil {
			t.Logf("listener error: %v", err)
		}
	}()

	select {
	case meta := <-received:
		if string(meta.Key) != "key-1" {
			t.Errorf("key = %q, want key-1", meta.Key)
		}
		if len(meta.Headers) != 3 ||
			meta.Headers[0].Key != "trace-id" || string(meta.Headers[0].Value) != "abc" ||
			meta.Headers[1].Key != "tag" || string(meta.Headers[1].Value) != "a" ||
			meta.Headers[2].Key != "tag" || string(meta.Headers[2].Value) != "b" {
			t.Errorf("headers = %+v, want trace-id=abc, tag=a, tag=b in order", meta.Headers)
		}
		if meta.Timestamp.Before(before) || meta.Timestamp.After(time.Now().Add(time.Minute)) {
			t.Errorf("timestamp = %v, want a recent time", meta.Timestamp)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the message")
	}
}

// A null key, an empty key and a binary key must arrive as sent: Kafka tells
// the first two apart and the key is raw bytes.
func TestKafkaIntegration_KeyRoundTripKeepsNullEmptyAndBinary(t *testing.T) {
	env := newTestEnv(t, "test-topic-keys", "test-consumer-group-keys")

	binary := []byte{0xff, 0x00, 0x80, 'k'}
	err := env.producer.PublishMessages([]commonkafka.Message{
		{Topic: env.topic, Key: nil, Value: []byte("null")},
		{Topic: env.topic, Key: []byte{}, Value: []byte("empty")},
		{Topic: env.topic, Key: binary, Value: []byte("binary")},
	})
	if err != nil {
		t.Fatalf("failed to publish messages: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	var mu sync.Mutex
	keys := map[string][]byte{}
	done := make(chan struct{})
	go func() {
		err := env.consumer.StartListening(ctx, func(_ context.Context, message []byte, meta commonkafka.Metadata, commit func() error) error {
			mu.Lock()
			keys[string(message)] = meta.Key
			complete := len(keys) == 3
			mu.Unlock()

			if complete {
				close(done)
			}

			return commit()
		})
		if err != nil && ctx.Err() == nil {
			t.Logf("listener error: %v", err)
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the messages")
	}

	mu.Lock()
	defer mu.Unlock()

	if got := keys["null"]; got != nil {
		t.Errorf("null key arrived as %#v, want nil", got)
	}
	if got := keys["empty"]; got == nil || len(got) != 0 {
		t.Errorf("empty key arrived as %#v, want an empty non-nil key", got)
	}
	if got := keys["binary"]; !bytes.Equal(got, binary) {
		t.Errorf("binary key arrived as %#v, want %#v", got, binary)
	}
}

// Sarama panics on a send to a closed producer and crashes the process on a
// second Close. The Producer must turn both into plain behavior.
func TestKafkaIntegration_CloseIsSafeToRepeatAndPublishAfterCloseFails(t *testing.T) {
	env := newTestEnv(t, "test-topic-close", "test-consumer-group-close")
	env.publishMessage(t, "before close")

	if err := env.producer.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := env.producer.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}

	err := env.producer.PublishMessage(commonkafka.Message{Topic: env.topic, Value: []byte("after close")})
	if !errors.Is(err, commonkafka.ErrProducerClosed) {
		t.Fatalf("PublishMessage() after Close error = %v, want ErrProducerClosed", err)
	}

	// The test cleanup closes the producer a third time; that must not crash
	// the test process either.
}

func TestKafkaIntegration_PublishMultipleMessages(t *testing.T) {
	env := newTestEnv(t, "test-topic-batch", "test-consumer-group-batch")

	messages := []string{"message-1", "message-2", "message-3", "message-4", "message-5"}
	env.publishMessages(t, messages)

	received := env.consumeN(t, len(messages), defaultTimeout)

	if len(received) != len(messages) {
		t.Errorf("message count mismatch: expected %d, got %d", len(messages), len(received))
	}
	for i, msg := range messages {
		if received[i] != msg {
			t.Errorf("message %d mismatch: expected %q, got %q", i, msg, received[i])
		}
	}
}

func TestKafkaIntegration_LargeMessage(t *testing.T) {
	env := newLargeMessageTestEnv(t, "test-topic-large", "test-consumer-group-large")

	largeMessage := strings.Repeat("A", largeMessageSize)
	t.Logf("Publishing large message of size: %d bytes (%.2f MB)", len(largeMessage), float64(len(largeMessage))/(1024*1024))

	env.publishMessage(t, largeMessage)

	received := env.consumeOne(t, largeTimeout)
	if len(received) != len(largeMessage) {
		t.Errorf("large message size mismatch: expected %d, got %d", len(largeMessage), len(received))
	}
	if received != largeMessage {
		t.Error("large message content mismatch")
	}
	t.Logf("Successfully received large message of size: %d bytes", len(received))
}

func TestKafkaIntegration_PublishMessageTooLarge(t *testing.T) {
	topic := "test-topic-publish-too-large"
	kafkaConf := &commonkafka.Config{
		Brokers:  []string{sharedKafkaBroker},
		ClientID: "test-client-publish-too-large",
		Producer: &commonkafka.ProducerSettings{
			Acks:            "all",
			RetryBackoffMs:  100,
			Compression:     "lz4",
			MaxMessageBytes: 1024,
		},
	}

	producer := newProducer(t, kafkaConf)

	largeMessage := strings.Repeat("A", 2*1024)
	t.Logf("Attempting to publish message of size: %d bytes (limit: %d bytes)", len(largeMessage), 1024)

	err := producer.PublishMessage(commonkafka.Message{Topic: topic, Value: []byte(largeMessage)})
	if err == nil {
		t.Fatal("expected error when publishing message larger than MaxMessageBytes, but got nil")
	}
	t.Logf("Got expected publish error: %v", err)
}

func TestKafkaIntegration_MessageOrder(t *testing.T) {
	env := newTestEnv(t, "test-topic-order", "test-consumer-group-order")

	for i := 0; i < messageCount; i++ {
		env.publishMessage(t, fmt.Sprintf("order-msg-%03d", i))
	}

	received := env.consumeN(t, messageCount, defaultTimeout)

	if len(received) != messageCount {
		t.Fatalf("message count mismatch: expected %d, got %d", messageCount, len(received))
	}

	for i := 0; i < messageCount; i++ {
		expected := fmt.Sprintf("order-msg-%03d", i)
		if received[i] != expected {
			t.Errorf("message order violation at index %d: expected %q, got %q", i, expected, received[i])
		}
	}
	t.Log("Message order verified successfully")
}

// A handler error ends the group session and the uncommitted message is
// delivered again; this is the behavior the README documents.
func TestKafkaIntegration_HandlerErrorRedeliversMessage(t *testing.T) {
	env := newTestEnv(t, "test-topic-redeliver", "test-consumer-group-redeliver")
	env.publishMessage(t, "again")

	ctx, cancel := context.WithTimeout(context.Background(), largeTimeout)
	defer cancel()

	var calls atomic.Int32
	delivered := make(chan struct{})
	go func() {
		err := env.consumer.StartListening(ctx, func(_ context.Context, message []byte, _ commonkafka.Metadata, commit func() error) error {
			if calls.Add(1) == 1 {
				return errors.New("first attempt fails")
			}
			close(delivered)

			return commit()
		})
		if err != nil && ctx.Err() == nil {
			t.Logf("listener error: %v", err)
		}
	}()

	select {
	case <-delivered:
	case <-ctx.Done():
		t.Fatal("the message was not delivered again after the handler error")
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("handler calls = %d, want 2", got)
	}
}

// An error returned by the message handler reaches the error handler, naming
// the partition it came from. This exercises Sarama's own error stream, which
// is only enabled when an error handler is registered.
func TestKafkaIntegration_ErrorHandlerReceivesHandlerError(t *testing.T) {
	topic, group := "test-topic-errhandler", "test-consumer-group-errhandler"
	env := newTestEnv(t, topic, group)
	env.publishMessage(t, "fails")

	consumerConfig, err := commonkafka.NewConsumerConfigBy(&commonkafka.Config{
		Brokers:  []string{sharedKafkaBroker},
		ClientID: "test-client-" + topic,
		Consumer: &commonkafka.ConsumerSettings{
			Topics:           []string{topic},
			Group:            group,
			SessionTimeoutMs: 10000,
			HeartbeatMs:      3000,
			AutoOffsetReset:  "earliest",
		},
	})
	if err != nil {
		t.Fatalf("failed to create consumer config: %v", err)
	}

	handlerErr := errors.New("cannot process")
	reported := make(chan *commonkafka.ConsumerError, 8)
	consumer, err := commonkafka.NewConsumer(consumerConfig, commonkafka.WithConsumerErrorHandler(func(e *commonkafka.ConsumerError) {
		select {
		case reported <- e:
		default:
		}
	}))
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	go func() {
		err := consumer.StartListening(ctx, func(context.Context, []byte, commonkafka.Metadata, func() error) error { return handlerErr })
		if err != nil && ctx.Err() == nil {
			t.Logf("listener error: %v", err)
		}
	}()

	select {
	case e := <-reported:
		if !errors.Is(e, handlerErr) {
			t.Errorf("reported error = %v, want it to wrap %v", e, handlerErr)
		}
		if e.Topic != topic || e.Partition < 0 {
			t.Errorf("reported partition = %s[%d], want %s[>=0]", e.Topic, e.Partition, topic)
		}
	case <-ctx.Done():
		t.Fatal("the handler error was never reported to the error handler")
	}
}

func TestKafkaIntegration_CommitStoresOffset(t *testing.T) {
	topic := "test-topic-commit"
	consumerGroup := "test-consumer-group-commit"
	env := newTestEnv(t, topic, consumerGroup)

	testMessage := "commit-test-message"
	env.publishMessage(t, testMessage)

	received := env.consumeOne(t, defaultTimeout)
	if received != testMessage {
		t.Errorf("message mismatch: expected %q, got %q", testMessage, received)
	}
	t.Log("Message received and committed successfully")

	env.consumer.Close()

	env2 := newTestEnv(t, topic, consumerGroup)

	newMessage := "new-message-after-commit"
	env2.publishMessage(t, newMessage)

	secondReceived := env2.consumeOne(t, defaultTimeout)
	if secondReceived != newMessage {
		t.Errorf("expected new message %q, got %q (old message may have been re-delivered)", newMessage, secondReceived)
	}
	t.Log("Second consumer correctly received only the new message")
}

func TestKafkaIntegration_StartListeningStopsOnContextCancelAndResumesWithNewConsumer(t *testing.T) {
	topic := "test-topic-graceful-stop"
	consumerGroup := "test-consumer-group-graceful-stop"
	env := newTestEnv(t, topic, consumerGroup)

	env.publishMessages(t, []string{"first", "second"})

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	listenerCtx, stopListener := context.WithCancel(ctx)
	processedMessages := make(chan string, 2)
	listenerErr := make(chan error, 1)

	go func() {
		listenerErr <- env.consumer.StartListening(listenerCtx, func(_ context.Context, message []byte, _ commonkafka.Metadata, commit func() error) error {
			processedMessages <- string(message)
			if err := commit(); err != nil {
				return err
			}

			stopListener()

			return nil
		})
	}()

	require.Equal(t, "first", receiveString(t, processedMessages, defaultTimeout))
	require.ErrorIs(t, receiveError(t, listenerErr, defaultTimeout), context.Canceled)
	requireNoMessage(t, processedMessages)

	require.NoError(t, env.consumer.Close())

	env2 := newTestEnv(t, topic, consumerGroup)
	require.Equal(t, "second", env2.consumeOne(t, defaultTimeout))
}

// The handler's context is the group session's, so it ends with the listener
// and a commit after that is refused: the message was never stored and the next
// consumer of the group gets it again.
func TestKafkaIntegration_HandlerContextEndsWithSessionAndLateCommitIsRefused(t *testing.T) {
	topic := "test-topic-session-ctx"
	consumerGroup := "test-consumer-group-session-ctx"
	env := newTestEnv(t, topic, consumerGroup)

	env.publishMessage(t, "pending")

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	listenerCtx, stopListener := context.WithCancel(ctx)
	started := make(chan struct{}, 1)
	commitErr := make(chan error, 1)
	listenerErr := make(chan error, 1)

	go func() {
		listenerErr <- env.consumer.StartListening(listenerCtx, func(handlerCtx context.Context, _ []byte, _ commonkafka.Metadata, commit func() error) error {
			select {
			case started <- struct{}{}:
			default:
			}

			<-handlerCtx.Done()

			select {
			case commitErr <- commit():
			default:
			}

			return handlerCtx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(defaultTimeout):
		t.Fatal("handler was not called in time")
	}

	stopListener()

	require.ErrorIs(t, receiveError(t, commitErr, defaultTimeout), context.Canceled)
	require.ErrorIs(t, receiveError(t, listenerErr, defaultTimeout), context.Canceled)
	require.NoError(t, env.consumer.Close())

	env2 := newTestEnv(t, topic, consumerGroup)
	require.Equal(t, "pending", env2.consumeOne(t, defaultTimeout))
}

// testEnv encapsulates producer and consumer for cleaner test setup.
type testEnv struct {
	topic    string
	producer *commonkafka.Producer
	consumer *commonkafka.Consumer
}

func newTestEnv(t *testing.T, topic, consumerGroup string) *testEnv {
	t.Helper()

	kafkaConf := &commonkafka.Config{
		Brokers:  []string{sharedKafkaBroker},
		ClientID: "test-client-" + topic,
		Producer: &commonkafka.ProducerSettings{
			Acks:           "all",
			RetryBackoffMs: 100,
			Compression:    "lz4",
		},
		Consumer: &commonkafka.ConsumerSettings{
			Topics:           []string{topic},
			Group:            consumerGroup,
			SessionTimeoutMs: 10000,
			HeartbeatMs:      3000,
			AutoOffsetReset:  "earliest",
		},
	}

	return newTestEnvWithConfig(t, topic, kafkaConf)
}

func newLargeMessageTestEnv(t *testing.T, topic, consumerGroup string) *testEnv {
	t.Helper()

	// Idempotence forces at most one in-flight request per broker connection,
	// which would silently override the MaxOpenRequests: 5 below. Disable it
	// explicitly so this test actually exercises pipelined large-message sends.
	disableIdempotence := false

	kafkaConf := &commonkafka.Config{
		Brokers:  []string{sharedKafkaBroker},
		ClientID: "test-client-" + topic,
		Producer: &commonkafka.ProducerSettings{
			Acks:              "all",
			RetryBackoffMs:    100,
			Compression:       "lz4",
			MaxMessageBytes:   5 * 1024 * 1024,
			MaxOpenRequests:   5,
			EnableIdempotence: &disableIdempotence,
		},
		Consumer: &commonkafka.ConsumerSettings{
			Topics:           []string{topic},
			Group:            consumerGroup,
			SessionTimeoutMs: 30000,
			HeartbeatMs:      3000,
			AutoOffsetReset:  "earliest",
			MaxFetchBytes:    5 * 1024 * 1024,
		},
	}

	return newTestEnvWithConfig(t, topic, kafkaConf)
}

func newTestEnvWithConfig(t *testing.T, topic string, kafkaConf *commonkafka.Config) *testEnv {
	t.Helper()

	producer := newProducer(t, kafkaConf)
	consumer := newConsumer(t, kafkaConf)

	return &testEnv{
		topic:    topic,
		producer: producer,
		consumer: consumer,
	}
}

func newProducer(t *testing.T, kafkaConf *commonkafka.Config) *commonkafka.Producer {
	t.Helper()

	producerConfig, err := commonkafka.NewProducerConfigBy(kafkaConf)
	if err != nil {
		t.Fatalf("failed to create producer config: %v", err)
	}

	producer, err := commonkafka.NewProducer(producerConfig)
	if err != nil {
		t.Fatalf("failed to create kafka producer: %v", err)
	}
	t.Cleanup(func() { producer.Close() })

	return producer
}

func newConsumer(t *testing.T, kafkaConf *commonkafka.Config) *commonkafka.Consumer {
	t.Helper()

	consumerConfig, err := commonkafka.NewConsumerConfigBy(kafkaConf)
	if err != nil {
		t.Fatalf("failed to create consumer config: %v", err)
	}

	consumer, err := commonkafka.NewConsumer(consumerConfig)
	if err != nil {
		t.Fatalf("failed to create kafka consumer: %v", err)
	}
	t.Cleanup(func() { consumer.Close() })

	return consumer
}

func (e *testEnv) publishMessage(t *testing.T, value string) {
	t.Helper()

	if err := e.producer.PublishMessage(commonkafka.Message{Topic: e.topic, Value: []byte(value)}); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}
}

func (e *testEnv) publishMessages(t *testing.T, values []string) {
	t.Helper()

	messages := make([]commonkafka.Message, len(values))
	for i, v := range values {
		messages[i] = commonkafka.Message{Topic: e.topic, Value: []byte(v)}
	}
	if err := e.producer.PublishMessages(messages); err != nil {
		t.Fatalf("failed to publish messages: %v", err)
	}
}

func (e *testEnv) consumeOne(t *testing.T, timeout time.Duration) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)

	var received string
	done := make(chan struct{})

	go func() {
		if err := e.consumer.StartListening(ctx, func(_ context.Context, message []byte, _ commonkafka.Metadata, commit func() error) error {
			received = string(message)
			close(done)
			return commit()
		}); err != nil && ctx.Err() == nil {
			t.Logf("listener error: %v", err)
		}
	}()

	select {
	case <-done:
		return received
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
		return ""
	}
}

func (e *testEnv) consumeN(t *testing.T, count int, timeout time.Duration) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)

	received := make([]string, 0, count)
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		if err := e.consumer.StartListening(ctx, func(_ context.Context, message []byte, _ commonkafka.Metadata, commit func() error) error {
			mu.Lock()
			received = append(received, string(message))
			n := len(received)
			mu.Unlock()

			if err := commit(); err != nil {
				return err
			}

			if n >= count {
				close(done)
			}
			return nil
		}); err != nil && ctx.Err() == nil {
			t.Logf("listener error: %v", err)
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for messages")
	}

	mu.Lock()
	defer mu.Unlock()
	return received
}

func receiveString(t *testing.T, ch <-chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatal("timeout waiting for value")
		return ""
	}
}

func receiveError(t *testing.T, ch <-chan error, timeout time.Duration) error {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatal("timeout waiting for error")
		return nil
	}
}

func requireNoMessage(t *testing.T, ch <-chan string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected message: %q", v)
	case <-time.After(200 * time.Millisecond):
	}
}

// A handler that always fails must not cycle the group faster than the
// consumer's backoff: each redelivery waits at least about a second.
func TestKafkaIntegration_HandlerErrorBacksOffBeforeRejoining(t *testing.T) {
	env := newTestEnv(t, "test-topic-backoff", "test-consumer-group-backoff")
	env.publishMessage(t, "poison")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var mu sync.Mutex
	var stamps []time.Time
	err := env.consumer.StartListening(ctx, func(context.Context, []byte, commonkafka.Metadata, func() error) error {
		mu.Lock()
		stamps = append(stamps, time.Now())
		mu.Unlock()

		return errors.New("poison")
	})
	if err != nil && ctx.Err() == nil {
		t.Fatalf("listener error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(stamps) < 3 {
		t.Fatalf("handler calls = %d in 8s, want at least 3 redeliveries to compare", len(stamps))
	}
	for i := 1; i < len(stamps); i++ {
		if gap := stamps[i].Sub(stamps[i-1]); gap < 900*time.Millisecond {
			t.Errorf("redelivery %d came after %v, want at least ~1s of backoff", i, gap)
		}
	}
}
