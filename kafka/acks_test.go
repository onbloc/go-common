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

// An idempotent producer (the default) needs acks=all and one in-flight request.
// A config that asks for something else is rejected, not silently overridden:
// the value the user wrote must be the value that is used.
func TestIdempotenceConflictsAreRejectedNotOverridden(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ProducerSettings)
		want   string
	}{
		{"acks 0", func(p *ProducerSettings) { p.Acks = AcksNone }, "producer.acks"},
		{"acks 1", func(p *ProducerSettings) { p.Acks = AcksLeader }, "producer.acks"},
		{"more in-flight requests", func(p *ProducerSettings) { p.MaxOpenRequests = 5 }, "producer.max_open_requests"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := validConfig()
			tt.mutate(conf.Producer)

			err := conf.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), "enable_idempotence") {
				t.Fatalf("Validate() error = %v, want it to name %s and the idempotence switch", err, tt.want)
			}
			if _, err = NewProducerConfigBy(conf); err == nil {
				t.Fatal("NewProducerConfigBy() expected the same conflict to be rejected")
			}

			// Turning idempotence off is the way to use the setting, and it is
			// then applied as written.
			off := false
			conf.Producer.EnableIdempotence = &off
			producerConfig, err := NewProducerConfigBy(conf)
			if err != nil {
				t.Fatalf("NewProducerConfigBy() with idempotence off error = %v", err)
			}
			sc := producerConfig.ProducerConfig
			if tt.want == "producer.acks" && sc.Producer.RequiredAcks == sarama.WaitForAll {
				t.Fatal("acks was overridden to all although idempotence is off")
			}
			if tt.want == "producer.max_open_requests" && sc.Net.MaxOpenRequests != 5 {
				t.Fatalf("MaxOpenRequests = %d, want 5", sc.Net.MaxOpenRequests)
			}
		})
	}
}

func TestIdempotentProducerAcceptsAcksAllInEitherSpelling(t *testing.T) {
	for _, acks := range []Acks{"", AcksAll, "-1"} {
		conf := validConfig()
		conf.Producer.Acks = acks

		if err := conf.Validate(); err != nil {
			t.Fatalf("Validate() with acks %q error = %v, want it accepted while idempotent", acks, err)
		}
	}
}
