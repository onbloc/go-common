package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

const testWait = 2 * time.Second

// fakeSession is a minimal sarama.ConsumerGroupSession. Embedding the
// interface promotes the methods ConsumeClaim never calls.
type fakeSession struct {
	sarama.ConsumerGroupSession

	ctx context.Context

	mu      sync.Mutex
	marked  []int64
	commits int
}

func newFakeSession(ctx context.Context) *fakeSession {
	return &fakeSession{ctx: ctx}
}

func (s *fakeSession) Context() context.Context { return s.ctx }

func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.marked = append(s.marked, msg.Offset)
}

func (s *fakeSession) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commits++
}

func (s *fakeSession) snapshot() (marked []int64, commits int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]int64(nil), s.marked...), s.commits
}

// fakeClaim feeds messages from a channel the test controls.
type fakeClaim struct {
	sarama.ConsumerGroupClaim

	messages chan *sarama.ConsumerMessage
}

func newFakeClaim() *fakeClaim {
	return &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 16)}
}

func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func (c *fakeClaim) send(offsets ...int64) {
	for _, offset := range offsets {
		c.messages <- &sarama.ConsumerMessage{
			Topic:     "topic",
			Partition: 0,
			Offset:    offset,
			Key:       []byte("key"),
			Value:     []byte("value"),
		}
	}
}

type batchRecorder struct {
	mu      sync.Mutex
	batches [][]BatchMessage
	calls   chan struct{}
}

func newBatchRecorder() *batchRecorder {
	return &batchRecorder{calls: make(chan struct{}, 16)}
}

// handler records each batch and commits it, like a well-behaved application.
// It signals only after the commit has run, so a test that waits for the call
// can assert on the committed offsets without racing the commit.
func (r *batchRecorder) handler(messages []BatchMessage, commit func() error) error {
	r.mu.Lock()
	r.batches = append(r.batches, append([]BatchMessage(nil), messages...))
	r.mu.Unlock()

	err := commit()
	r.calls <- struct{}{}

	return err
}

func (r *batchRecorder) waitForCall(t *testing.T) {
	t.Helper()

	select {
	case <-r.calls:
	case <-time.After(testWait):
		t.Fatal("handler was not called in time")
	}
}

func (r *batchRecorder) sizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()

	sizes := make([]int, len(r.batches))
	for i, batch := range r.batches {
		sizes[i] = len(batch)
	}

	return sizes
}

func runClaim(h *batchConsumerHandler, session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) <-chan error {
	done := make(chan error, 1)
	go func() { done <- h.ConsumeClaim(session, claim) }()

	return done
}

func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(testWait):
		t.Fatal("ConsumeClaim did not return in time")

		return nil
	}
}

func newTestHandler(config BatchConfig, fn BatchMessageHandler) *batchConsumerHandler {
	return &batchConsumerHandler{logger: defaultLogger(), batchConfig: config, metadataHandler: fn}
}

func assertMarked(t *testing.T, session *fakeSession, wantOffsets []int64, wantCommits int) {
	t.Helper()

	marked, commits := session.snapshot()
	if len(marked) != len(wantOffsets) {
		t.Fatalf("marked offsets = %v, want %v", marked, wantOffsets)
	}
	for i := range wantOffsets {
		if marked[i] != wantOffsets[i] {
			t.Fatalf("marked offsets = %v, want %v", marked, wantOffsets)
		}
	}
	if commits != wantCommits {
		t.Fatalf("commits = %d, want %d", commits, wantCommits)
	}
}

func TestConsumeClaimFlushesWhenBatchIsFull(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	rec := newBatchRecorder()
	// A long interval proves the flush came from reaching MaxMessages.
	h := newTestHandler(BatchConfig{MaxMessages: 3, FlushInterval: time.Hour}, rec.handler)

	done := runClaim(h, session, claim)

	claim.send(0, 1, 2)
	rec.waitForCall(t)

	if got := rec.sizes(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("batch sizes = %v, want [3]", got)
	}
	assertMarked(t, session, []int64{0, 1, 2}, 1)

	close(claim.messages)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}
}

func TestConsumeClaimFlushesPartialBatchWhenIntervalElapses(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	rec := newBatchRecorder()
	h := newTestHandler(BatchConfig{MaxMessages: 100, FlushInterval: 20 * time.Millisecond}, rec.handler)

	done := runClaim(h, session, claim)

	claim.send(0, 1)
	rec.waitForCall(t)

	if got := rec.sizes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("batch sizes = %v, want [2]", got)
	}
	assertMarked(t, session, []int64{0, 1}, 1)

	close(claim.messages)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}
}

func TestConsumeClaimFlushesRemainingBatchWhenClaimCloses(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	rec := newBatchRecorder()
	h := newTestHandler(BatchConfig{MaxMessages: 100, FlushInterval: time.Hour}, rec.handler)

	done := runClaim(h, session, claim)

	claim.send(0, 1)
	close(claim.messages)

	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}
	if got := rec.sizes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("batch sizes = %v, want [2]", got)
	}
	assertMarked(t, session, []int64{0, 1}, 1)
}

func TestConsumeClaimReturnsNilWhenSessionEndsAndDropsPendingBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	session := newFakeSession(ctx)
	claim := newFakeClaim()
	rec := newBatchRecorder()
	h := newTestHandler(BatchConfig{MaxMessages: 100, FlushInterval: time.Hour}, rec.handler)

	done := runClaim(h, session, claim)

	claim.send(0)
	// Let the message enter the pending batch before the session ends.
	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v, want nil: a session ending is not an error", err)
	}
	if got := rec.sizes(); len(got) != 0 {
		t.Fatalf("batch sizes = %v, want none: the pending batch must be left for redelivery", got)
	}
	assertMarked(t, session, nil, 0)
}

func TestConsumeClaimReturnsHandlerErrorWithoutCommitting(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	handlerErr := errors.New("boom")
	h := newTestHandler(BatchConfig{MaxMessages: 2, FlushInterval: time.Hour},
		func([]BatchMessage, func() error) error { return handlerErr })

	done := runClaim(h, session, claim)

	claim.send(0, 1)

	if err := waitDone(t, done); !errors.Is(err, handlerErr) {
		t.Fatalf("ConsumeClaim() error = %v, want %v", err, handlerErr)
	}
	assertMarked(t, session, nil, 0)
}

func TestConsumeClaimDoesNotCommitUnlessHandlerCommits(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	called := make(chan struct{}, 1)
	h := newTestHandler(BatchConfig{MaxMessages: 1, FlushInterval: time.Hour},
		func([]BatchMessage, func() error) error {
			called <- struct{}{}

			return nil
		})

	done := runClaim(h, session, claim)

	claim.send(0)
	select {
	case <-called:
	case <-time.After(testWait):
		t.Fatal("handler was not called in time")
	}

	close(claim.messages)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}
	assertMarked(t, session, nil, 0)
}

func TestConsumeClaimPassesMessageMetadataToHandler(t *testing.T) {
	session := newFakeSession(t.Context())
	claim := newFakeClaim()
	rec := newBatchRecorder()
	h := newTestHandler(BatchConfig{MaxMessages: 1, FlushInterval: time.Hour}, rec.handler)

	done := runClaim(h, session, claim)

	claim.send(7)
	rec.waitForCall(t)
	close(claim.messages)
	if err := waitDone(t, done); err != nil {
		t.Fatalf("ConsumeClaim() error = %v", err)
	}

	got := rec.batches[0][0]
	if string(got.Value) != "value" || got.Metadata.Topic != "topic" || string(got.Metadata.Key) != "key" || got.Metadata.Offset != 7 {
		t.Fatalf("batch message = %+v", got)
	}
}

// claimGroup runs one claim with a single message through the handler on every
// Consume call and returns nil, as Sarama's Consume does after a session ends.
type claimGroup struct {
	sarama.ConsumerGroup

	mu        sync.Mutex
	calls     []time.Time
	claimErrs []error
}

func (g *claimGroup) Consume(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
	g.mu.Lock()
	g.calls = append(g.calls, time.Now())
	g.mu.Unlock()

	claim := newFakeClaim()
	claim.send(0)
	close(claim.messages)

	// Sarama reports a claim's error and ends the session; Consume returns nil.
	err := handler.ConsumeClaim(newFakeSession(ctx), claim)

	g.mu.Lock()
	g.claimErrs = append(g.claimErrs, err)
	g.mu.Unlock()

	return nil
}

func (g *claimGroup) Close() error { return nil }

func (g *claimGroup) claimErrors() []error {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]error(nil), g.claimErrs...)
}

func (g *claimGroup) callTimes() []time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]time.Time(nil), g.calls...)
}

// listen runs StartBatchListening on c until n Consume calls were made, then
// stops it and returns when each call happened.
func listen(t *testing.T, c *Consumer, group *claimGroup, handler BatchMessageHandler, n int) []time.Time {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := c.StartBatchListening(ctx, BatchConfig{MaxMessages: 1, FlushInterval: time.Hour}, handler); !errors.Is(err, context.Canceled) {
			t.Errorf("StartBatchListening() error = %v, want context.Canceled", err)
		}
	}()

	deadline := time.After(10 * time.Second)
	for len(group.callTimes()) < n {
		select {
		case <-deadline:
			t.Fatalf("only %d Consume calls in time, want %d", len(group.callTimes()), n)
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	<-done

	return group.callTimes()
}

// A handler error ends the session without Consume returning an error. The
// consumer must wait before rejoining, or one message that always fails would
// rebalance the group in a tight loop.
func TestConsumerBacksOffAfterAHandlerError(t *testing.T) {
	const backoff = 100 * time.Millisecond

	group := &claimGroup{}
	c := newTestConsumer(group, nil)
	c.retryBackoff = backoff

	calls := listen(t, c, group, func([]BatchMessage, func() error) error { return errors.New("poison") }, 3)

	// The premise: a handler error comes back from ConsumeClaim, ending the
	// session. The last Consume call may have started after the listener was
	// stopped, and a claim whose session is already over returns nil, so only
	// the calls that were followed by another one are checked.
	for i, err := range group.claimErrors()[:2] {
		if err == nil {
			t.Fatalf("ConsumeClaim %d returned nil, want the handler error", i+1)
		}
	}

	for i := 1; i < 3; i++ {
		if gap := calls[i].Sub(calls[i-1]); gap < backoff*8/10 {
			t.Fatalf("gap before Consume call %d = %v, want at least ~%v after a handler error", i+1, gap, backoff)
		}
	}
}

// A session that ends normally (rebalance, shutdown) must rejoin at once; the
// backoff is only for failures.
func TestConsumerRejoinsAtOnceAfterANormalSession(t *testing.T) {
	group := &claimGroup{}
	c := newTestConsumer(group, nil)
	c.retryBackoff = time.Second

	start := time.Now()
	listen(t, c, group, func(_ []BatchMessage, commit func() error) error { return commit() }, 5)

	for i, err := range group.claimErrors() {
		if err != nil {
			t.Fatalf("ConsumeClaim %d returned %v, want nil after a commit", i+1, err)
		}
	}

	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("5 normal sessions took %v, want an immediate rejoin with no backoff", elapsed)
	}
}
