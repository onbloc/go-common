package kafka

import (
	"testing"
	"time"
)

func TestNewConsumerConfigByRequiresTopics(t *testing.T) {
	_, err := NewConsumerConfigBy(&Config{
		Brokers: []string{"localhost:9092"},
		Consumer: &ConsumerSettings{
			Group: "projection",
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewConsumerConfigByNormalizesTopics(t *testing.T) {
	consumerConfig, err := NewConsumerConfigBy(&Config{
		Brokers: []string{"localhost:9092"},
		Consumer: &ConsumerSettings{
			Topics: []string{"topic-a, topic-b", "topic-c"},
			Group:  "projection",
		},
	})
	if err != nil {
		t.Fatalf("NewConsumerConfigBy() error = %v", err)
	}

	want := []string{"topic-a", "topic-b", "topic-c"}
	if len(consumerConfig.Topics) != len(want) {
		t.Fatalf("topics length = %d, want %d", len(consumerConfig.Topics), len(want))
	}

	for i := range want {
		if consumerConfig.Topics[i] != want[i] {
			t.Fatalf("topic[%d] = %q, want %q", i, consumerConfig.Topics[i], want[i])
		}
	}
}

func TestNewConsumerConfigByDefaultsConcurrency(t *testing.T) {
	consumerConfig, err := NewConsumerConfigBy(&Config{
		Brokers: []string{"localhost:9092"},
		Consumer: &ConsumerSettings{
			Topics: []string{"topic-a"},
			Group:  "projection",
		},
	})
	if err != nil {
		t.Fatalf("NewConsumerConfigBy() error = %v", err)
	}

	if consumerConfig.Concurrency != 1 {
		t.Fatalf("concurrency = %d, want 1", consumerConfig.Concurrency)
	}
}

func TestNewConsumerConfigByDefaultsSessionTiming(t *testing.T) {
	// Configs built outside struct-tag defaulting arrive with zero timing
	// fields. The zero session timeout must fall back to a value Sarama
	// accepts (>= 2ms).
	consumerConfig, err := NewConsumerConfigBy(&Config{
		Brokers: []string{"localhost:9092"},
		Consumer: &ConsumerSettings{
			Topics: []string{"topic-a"},
			Group:  "projection",
		},
	})
	if err != nil {
		t.Fatalf("NewConsumerConfigBy() error = %v", err)
	}

	if err := consumerConfig.ConsumerConfig.Validate(); err != nil {
		t.Fatalf("sarama config validation failed: %v", err)
	}

	wantSession := time.Duration(defaultSessionTimeoutMs) * time.Millisecond
	if got := consumerConfig.ConsumerConfig.Consumer.Group.Session.Timeout; got != wantSession {
		t.Fatalf("session timeout = %s, want %s", got, wantSession)
	}

	wantHeartbeat := time.Duration(defaultHeartbeatMs) * time.Millisecond
	if got := consumerConfig.ConsumerConfig.Consumer.Group.Heartbeat.Interval; got != wantHeartbeat {
		t.Fatalf("heartbeat interval = %s, want %s", got, wantHeartbeat)
	}
}

func TestNewConsumerConfigByUsesConfiguredConcurrency(t *testing.T) {
	consumerConfig, err := NewConsumerConfigBy(&Config{
		Brokers: []string{"localhost:9092"},
		Consumer: &ConsumerSettings{
			Topics:      []string{"topic-a"},
			Group:       "projection",
			Concurrency: 4,
		},
	})
	if err != nil {
		t.Fatalf("NewConsumerConfigBy() error = %v", err)
	}

	if consumerConfig.Concurrency != 4 {
		t.Fatalf("concurrency = %d, want 4", consumerConfig.Concurrency)
	}
}

func TestNewConsumerConfigByRequiresConfig(t *testing.T) {
	if _, err := NewConsumerConfigBy(nil); err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestNewProducerConfigByRequiresProducerSettings(t *testing.T) {
	if _, err := NewProducerConfigBy(&Config{Brokers: []string{"localhost:9092"}}); err == nil {
		t.Fatal("expected error for missing producer settings")
	}
}

func TestConsumerSettingsBatchConfigDefaults(t *testing.T) {
	config := (&ConsumerSettings{}).BatchConfig()

	if config.MaxMessages != defaultBatchMaxMessages {
		t.Fatalf("MaxMessages = %d, want %d", config.MaxMessages, defaultBatchMaxMessages)
	}
	if config.FlushInterval != time.Duration(defaultBatchFlushIntervalMs)*time.Millisecond {
		t.Fatalf("FlushInterval = %s, want %dms", config.FlushInterval, defaultBatchFlushIntervalMs)
	}
}

func TestConsumerSettingsBatchConfigConfigured(t *testing.T) {
	config := (&ConsumerSettings{BatchMaxMessages: 50, BatchFlushIntervalMs: 500}).BatchConfig()

	if config.MaxMessages != 50 {
		t.Fatalf("MaxMessages = %d, want 50", config.MaxMessages)
	}
	if config.FlushInterval != 500*time.Millisecond {
		t.Fatalf("FlushInterval = %s, want 500ms", config.FlushInterval)
	}
}
