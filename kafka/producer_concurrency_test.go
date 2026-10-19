package kafka

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// barrierProducer holds every SendMessage until n callers are inside it at the
// same time. It can only complete if the Producer lets publishes overlap.
type barrierProducer struct {
	sarama.SyncProducer

	arrived chan struct{}
	release chan struct{}
	n       int
}

func newBarrierProducer(n int) *barrierProducer {
	b := &barrierProducer{arrived: make(chan struct{}, n), release: make(chan struct{}), n: n}

	go func() {
		for range n {
			<-b.arrived
		}
		close(b.release)
	}()

	return b
}

func (b *barrierProducer) SendMessage(*sarama.ProducerMessage) (partition int32, offset int64, err error) {
	b.arrived <- struct{}{}
	<-b.release

	return 0, 0, nil
}

func TestProducerPublishesConcurrently(t *testing.T) {
	const publishers = 8

	p := &Producer{producer: newBarrierProducer(publishers), logger: defaultLogger()}

	var wg sync.WaitGroup
	errs := make(chan error, publishers)
	for range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- p.PublishMessage(Message{Topic: "topic", Value: []byte("v")})
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(testWait):
		t.Fatal("publishes were serialized: they never overlapped")
	}

	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("PublishMessage() error = %v", err)
		}
	}
}

type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Infof(format string, args ...any)  { l.add("INFO "+format, args...) }
func (l *recordingLogger) Errorf(format string, args ...any) { l.add("ERROR "+format, args...) }

type recordingDebugLogger struct{ recordingLogger }

func (l *recordingDebugLogger) Debugf(format string, args ...any) { l.add("DEBUG "+format, args...) }

func TestPublishLogsOnlyAtDebugLevel(t *testing.T) {
	publish := func(l Logger) {
		p := &Producer{producer: &fakeSyncProducer{}, logger: l}
		if err := p.PublishMessage(Message{Topic: "a", Value: []byte("v")}); err != nil {
			t.Fatalf("PublishMessage() error = %v", err)
		}
		if err := p.PublishMessages([]Message{{Topic: "a"}, {Topic: "b"}, {Topic: "b"}}); err != nil {
			t.Fatalf("PublishMessages() error = %v", err)
		}
	}

	plain := &recordingLogger{}
	publish(plain)
	if len(plain.lines) != 0 {
		t.Fatalf("a logger without Debugf received %v, want no output", plain.lines)
	}

	debug := &recordingDebugLogger{}
	publish(debug)
	if len(debug.lines) != 2 {
		t.Fatalf("debug logger lines = %v, want 2", debug.lines)
	}
	for _, line := range debug.lines {
		if !strings.HasPrefix(line, "DEBUG ") {
			t.Fatalf("line %q not logged at debug level", line)
		}
	}
	if want := "topics=2 count=3"; !strings.Contains(debug.lines[1], want) {
		t.Fatalf("batch line = %q, want it to contain %q", debug.lines[1], want)
	}
}
