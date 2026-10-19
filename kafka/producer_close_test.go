package kafka

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// closeAwareProducer behaves like Sarama's sync producer where it matters here:
// sending to it after it was closed, or closing it while a send is in progress,
// panics ("send on closed channel"), and so does a second Close.
type closeAwareProducer struct {
	sarama.SyncProducer

	closed atomic.Bool
	closes atomic.Int32
	sent   atomic.Int32
}

func (f *closeAwareProducer) SendMessage(*sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if f.closed.Load() {
		panic("send on closed channel")
	}

	time.Sleep(200 * time.Microsecond) // a send takes a broker round trip

	if f.closed.Load() {
		panic("producer closed during a send")
	}

	f.sent.Add(1)

	return 0, 0, nil
}

func (f *closeAwareProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		if _, _, err := f.SendMessage(msg); err != nil {
			return err
		}
	}

	return nil
}

func (f *closeAwareProducer) Close() error {
	if f.closes.Add(1) > 1 {
		panic("send on closed channel")
	}

	f.closed.Store(true)

	return nil
}

func newCloseAwareProducer() (*Producer, *closeAwareProducer) {
	fake := &closeAwareProducer{}

	return &Producer{producer: fake, logger: defaultLogger()}, fake
}

func TestProducerCloseIsIdempotent(t *testing.T) {
	p, fake := newCloseAwareProducer()

	for i := range 3 {
		if err := p.Close(); err != nil {
			t.Fatalf("Close() call %d error = %v, want nil", i+1, err)
		}
	}
	if got := fake.closes.Load(); got != 1 {
		t.Fatalf("underlying producer closed %d times, want once", got)
	}
}

func TestProducerCloseFromSeveralGoroutinesClosesOnce(t *testing.T) {
	p, fake := newCloseAwareProducer()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Close()
		}()
	}
	wg.Wait()

	if got := fake.closes.Load(); got != 1 {
		t.Fatalf("underlying producer closed %d times, want once", got)
	}
}

func TestPublishAfterCloseReturnsErrProducerClosed(t *testing.T) {
	p, fake := newCloseAwareProducer()
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	message := Message{Topic: "topic", Value: []byte("v")}

	if err := p.PublishMessage(message); !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("PublishMessage() error = %v, want ErrProducerClosed", err)
	}
	if _, err := p.PublishMessageWithMetadata(message); !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("PublishMessageWithMetadata() error = %v, want ErrProducerClosed", err)
	}
	if err := p.PublishMessages([]Message{message}); !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("PublishMessages() error = %v, want ErrProducerClosed", err)
	}
	if got := fake.sent.Load(); got != 0 {
		t.Fatalf("%d messages reached the closed producer, want none", got)
	}
}

// Publishers racing Close must neither hit the closed producer nor be cut off
// mid-send: each call either completes or reports ErrProducerClosed.
func TestCloseDuringConcurrentPublishesNeverReachesAClosedProducer(t *testing.T) {
	p, _ := newCloseAwareProducer()

	var (
		wg       sync.WaitGroup
		panics   atomic.Int32
		badErrs  atomic.Int32
		stopped  atomic.Int32
		finished atomic.Int32
	)

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					panics.Add(1)
				}
			}()

			for {
				err := p.PublishMessage(Message{Topic: "topic", Value: []byte("v")})
				switch {
				case err == nil:
					finished.Add(1)
				case errors.Is(err, ErrProducerClosed):
					stopped.Add(1)

					return
				default:
					badErrs.Add(1)

					return
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	wg.Wait()

	if panics.Load() != 0 {
		t.Fatalf("%d publishers panicked on a closed producer", panics.Load())
	}
	if badErrs.Load() != 0 {
		t.Fatalf("%d publishers got an error other than ErrProducerClosed", badErrs.Load())
	}
	if finished.Load() == 0 || stopped.Load() != 8 {
		t.Fatalf("completed=%d stopped=%d, want publishes before Close and all 8 publishers stopped by ErrProducerClosed", finished.Load(), stopped.Load())
	}
}
