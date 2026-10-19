package kafka

import (
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

func TestAcksAcceptsKafkaValues(t *testing.T) {
	tests := []struct {
		spelling Acks
		want     sarama.RequiredAcks
	}{
		{"0", sarama.NoResponse},
		{"1", sarama.WaitForLocal},
		{"all", sarama.WaitForAll},
		{"-1", sarama.WaitForAll}, // Kafka treats -1 as all
		{"", sarama.WaitForAll},   // unset means the default
	}

	for _, tt := range tests {
		t.Run(string(tt.spelling), func(t *testing.T) {
			conf := validConfig()
			conf.Producer.Acks = tt.spelling
			// Idempotence forces acks=all, which would hide the value under test.
			disabled := false
			conf.Producer.EnableIdempotence = &disabled

			if err := conf.Validate(); err != nil {
				t.Fatalf("Validate() error = %v, want %q accepted", err, tt.spelling)
			}

			producerConfig, err := NewProducerConfigBy(conf)
			if err != nil {
				t.Fatalf("NewProducerConfigBy() error = %v", err)
			}
			if got := producerConfig.ProducerConfig.Producer.RequiredAcks; got != tt.want {
				t.Fatalf("Sarama RequiredAcks = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAcksRejectsOtherValues(t *testing.T) {
	// These look plausible but are not Kafka acks values; "none" and "local"
	// were Sarama's names and are no longer accepted.
	for _, spelling := range []Acks{"2", "quorum", "ALL", "-2", "none", "local"} {
		conf := validConfig()
		conf.Producer.Acks = spelling

		err := conf.Validate()
		if err == nil || !strings.Contains(err.Error(), "producer.acks") {
			t.Fatalf("Validate() error = %v, want %q rejected", err, spelling)
		}
	}
}

func TestAcksErrorListsKafkaValuesFirst(t *testing.T) {
	conf := validConfig()
	conf.Producer.Acks = "quorum"

	err := conf.Validate()
	if err == nil || !strings.Contains(err.Error(), "want one of: 0, 1, all, -1") {
		t.Fatalf("Validate() error = %v, want the accepted values listed Kafka's first", err)
	}
}

func TestIdempotentProducerStillForcesAcksAll(t *testing.T) {
	conf := validConfig()
	conf.Producer.Acks = AcksNone // asked for no acks, but idempotence is on

	producerConfig, err := NewProducerConfigBy(conf)
	if err != nil {
		t.Fatalf("NewProducerConfigBy() error = %v", err)
	}
	if got := producerConfig.ProducerConfig.Producer.RequiredAcks; got != sarama.WaitForAll {
		t.Fatalf("Sarama RequiredAcks = %d, want WaitForAll for an idempotent producer", got)
	}
}
