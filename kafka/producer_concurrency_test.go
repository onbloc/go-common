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

// recordingLogger records every line with its level.
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
func (l *recordingLogger) Debugf(format string, args ...any) { l.add("DEBUG "+format, args...) }

func TestPublishLogsOnlyAtDebugLevel(t *testing.T) {
	logger := &recordingLogger{}
	p := &Producer{producer: &fakeSyncProducer{}, logger: logger}

	if err := p.PublishMessage(Message{Topic: "a", Value: []byte("v")}); err != nil {
		t.Fatalf("PublishMessage() error = %v", err)
	}
	if err := p.PublishMessages([]Message{{Topic: "a"}, {Topic: "b"}, {Topic: "b"}}); err != nil {
		t.Fatalf("PublishMessages() error = %v", err)
	}

	if len(logger.lines) != 2 {
		t.Fatalf("lines = %v, want one per publish call", logger.lines)
	}
	for _, line := range logger.lines {
		if !strings.HasPrefix(line, "DEBUG ") {
			t.Fatalf("line %q not logged at debug level", line)
		}
	}
	if want := "count=3"; !strings.Contains(logger.lines[1], want) {
		t.Fatalf("batch line = %q, want it to contain %q", logger.lines[1], want)
	}
}
