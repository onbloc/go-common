package kafka

import (
	"testing"
	"time"
)

func TestBatchConfigNormalizedDefaultsEmptyValues(t *testing.T) {
	config := BatchConfig{}.normalized()

	if config.MaxMessages != 1 {
		t.Fatalf("MaxMessages = %d, want 1", config.MaxMessages)
	}
	if config.FlushInterval != time.Second {
		t.Fatalf("FlushInterval = %s, want %s", config.FlushInterval, time.Second)
	}
}

func TestBatchConfigNormalizedKeepsConfiguredValues(t *testing.T) {
	config := BatchConfig{
		MaxMessages:   100,
		FlushInterval: 250 * time.Millisecond,
	}.normalized()

	if config.MaxMessages != 100 {
		t.Fatalf("MaxMessages = %d, want 100", config.MaxMessages)
	}
	if config.FlushInterval != 250*time.Millisecond {
		t.Fatalf("FlushInterval = %s, want %s", config.FlushInterval, 250*time.Millisecond)
	}
}
