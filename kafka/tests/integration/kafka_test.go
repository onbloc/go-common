package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
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
			RequiredAcks:    "all",
			MaxRetries:      3,
			RetryBackoffMs:  100,
			Compression:     "lz4",
			MaxMessageBytes: 1024,
		},
	}

	producer := newProducer(t, kafkaConf)

	largeMessage := strings.Repeat("A", 2*1024)
	t.Logf("Attempting to publish message of size: %d bytes (limit: %d bytes)", len(largeMessage), 1024)

	err := producer.PublishMessage(commonkafka.Message{Topic: topic, Value: largeMessage})
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

func TestKafkaIntegration_AckCommitsOffset(t *testing.T) {
	topic := "test-topic-ack"
	consumerGroup := "test-consumer-group-ack"
	env := newTestEnv(t, topic, consumerGroup)

	testMessage := "ack-test-message"
	env.publishMessage(t, testMessage)

	received := env.consumeOne(t, defaultTimeout)
	if received != testMessage {
		t.Errorf("message mismatch: expected %q, got %q", testMessage, received)
	}
	t.Log("Message received and acked successfully")

	env.consumer.Close()

	env2 := newTestEnv(t, topic, consumerGroup)

	newMessage := "new-message-after-ack"
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
		listenerErr <- env.consumer.StartListening(listenerCtx, func(message string, _ commonkafka.Metadata, ack func() error) error {
			processedMessages <- message
			if err := ack(); err != nil {
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
			RequiredAcks:   "all",
			MaxRetries:     3,
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

	kafkaConf := &commonkafka.Config{
		Brokers:  []string{sharedKafkaBroker},
		ClientID: "test-client-" + topic,
		Producer: &commonkafka.ProducerSettings{
			RequiredAcks:    "all",
			MaxRetries:      3,
			RetryBackoffMs:  100,
			Compression:     "lz4",
			MaxMessageBytes: 5 * 1024 * 1024,
			MaxOpenRequests: 5,
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

	if err := e.producer.PublishMessage(commonkafka.Message{Topic: e.topic, Value: value}); err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}
}

func (e *testEnv) publishMessages(t *testing.T, values []string) {
	t.Helper()

	messages := make([]commonkafka.Message, len(values))
	for i, v := range values {
		messages[i] = commonkafka.Message{Topic: e.topic, Value: v}
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
		if err := e.consumer.StartListening(ctx, func(message string, _ commonkafka.Metadata, ack func() error) error {
			received = message
			close(done)
			return ack()
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
		if err := e.consumer.StartListening(ctx, func(message string, _ commonkafka.Metadata, ack func() error) error {
			mu.Lock()
			received = append(received, message)
			n := len(received)
			mu.Unlock()

			if err := ack(); err != nil {
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
