package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// fakeGroup is a sarama.ConsumerGroup test double with a controllable error
// stream and Consume result.
type fakeGroup struct {
	sarama.ConsumerGroup

	errs       chan error
	consumeErr error
	calls      atomic.Int32
}

func (g *fakeGroup) Errors() <-chan error { return g.errs }

func (g *fakeGroup) Consume(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	g.calls.Add(1)
	if g.consumeErr != nil {
		return g.consumeErr
	}

	<-ctx.Done()

	return nil
}

func (g *fakeGroup) Close() error { return nil }

type errorCollector struct {
	mu   sync.Mutex
	errs []*ConsumerError
	seen chan struct{}
}

func newErrorCollector() *errorCollector {
	return &errorCollector{seen: make(chan struct{}, 16)}
}

func (c *errorCollector) handle(err *ConsumerError) {
	c.mu.Lock()
	c.errs = append(c.errs, err)
	c.mu.Unlock()
	c.seen <- struct{}{}
}

func (c *errorCollector) wait(t *testing.T) {
	t.Helper()

	select {
	case <-c.seen:
	case <-time.After(testWait):
		t.Fatal("error handler was not called in time")
	}
}

func newTestConsumer(group sarama.ConsumerGroup, handler func(*ConsumerError)) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		consumerGroup: group,
		topics:        []string{"topic"},
		logger:        defaultLogger(),
		ctx:           ctx,
		cancel:        cancel,
		errorHandler:  handler,
		retryBackoff:  10 * time.Millisecond,
	}
}

func TestForwardGroupErrorsKeepsPartitionAndUnwraps(t *testing.T) {
	boom := errors.New("fetch failed")
	group := &fakeGroup{errs: make(chan error, 4)}
	collector := newErrorCollector()
	c := newTestConsumer(group, collector.handle)

	done := make(chan struct{})
	go func() { c.forwardGroupErrors(); close(done) }()

	group.errs <- &sarama.ConsumerError{Topic: "orders", Partition: 2, Err: boom}
	group.errs <- errors.New("group failure")
	collector.wait(t)
	collector.wait(t)

	// The stream closes when the group closes; forwarding must end with it.
	close(group.errs)
	select {
	case <-done:
	case <-time.After(testWait):
		t.Fatal("forwardGroupErrors did not stop when the error stream closed")
	}

	partitionErr, groupErr := collector.errs[0], collector.errs[1]
	if partitionErr.Topic != "orders" || partitionErr.Partition != 2 || !errors.Is(partitionErr, boom) {
		t.Fatalf("partition error = %+v, want orders[2] wrapping %v", partitionErr, boom)
	}
	if groupErr.Topic != "" || groupErr.Partition != noPartition {
		t.Fatalf("group error = %+v, want no topic and partition %d", groupErr, noPartition)
	}
}

func TestStartBatchListeningReportsConsumeErrorsAndKeepsRetrying(t *testing.T) {
	boom := errors.New("broker unavailable")
	group := &fakeGroup{errs: make(chan error), consumeErr: boom}
	collector := newErrorCollector()
	c := newTestConsumer(group, collector.handle)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.StartBatchListening(ctx, BatchConfig{MaxMessages: 1}, func(context.Context, []BatchMessage, func() error) error { return nil })
	}()

	// Two reports prove the consumer retried after the first failure.
	collector.wait(t)
	collector.wait(t)
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("StartBatchListening() error = %v, want context.Canceled", err)
	}
	if got := group.calls.Load(); got < 2 {
		t.Fatalf("Consume calls = %d, want at least 2", got)
	}
	if first := collector.errs[0]; !errors.Is(first, boom) || first.Partition != noPartition {
		t.Fatalf("reported error = %+v, want a group-level error wrapping %v", first, boom)
	}
}

func TestGroupConfigEnablesReturnErrorsOnlyWithHandler(t *testing.T) {
	original := sarama.NewConfig()

	without := newTestConsumer(nil, nil)
	if got := without.groupConfig(original); got != original || got.Consumer.Return.Errors {
		t.Fatal("without a handler the config must be passed through unchanged")
	}

	with := newTestConsumer(nil, func(*ConsumerError) {})
	got := with.groupConfig(original)
	if !got.Consumer.Return.Errors {
		t.Fatal("with a handler, Sarama must be told to return errors")
	}
	if original.Consumer.Return.Errors {
		t.Fatal("the caller's config was modified; it must be left untouched")
	}
}
