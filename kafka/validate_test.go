package kafka

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

const unsupportedCompression = "brotli"

func validConfig() *Config {
	conf := DefaultConfig()
	conf.Brokers = []string{"localhost:9092"}
	conf.Consumer.Group = "group"
	conf.Consumer.Topics = []string{"topic"}

	return conf
}

// The `default` tags are what a config loader applies; DefaultConfig is what
// code gets. They must describe the same defaults.
func TestDefaultTagsMatchDefaultConfig(t *testing.T) {
	var walk func(path string, tagged, def reflect.Value)
	walk = func(path string, tagged, def reflect.Value) {
		typ := tagged.Type()
		for i := range typ.NumField() {
			field := typ.Field(i)
			name := path + "." + field.Name
			want := def.Field(i)

			if field.Type.Kind() == reflect.Pointer && field.Type.Elem().Kind() == reflect.Struct {
				if want.IsNil() {
					t.Fatalf("%s: DefaultConfig leaves section nil", name)
				}
				walk(name, reflect.New(field.Type.Elem()).Elem(), want.Elem())

				continue
			}

			tag, ok := field.Tag.Lookup("default")
			if !ok {
				continue
			}

			got := want
			if got.Kind() == reflect.Pointer {
				if got.IsNil() {
					t.Fatalf("%s: has default tag %q but DefaultConfig leaves it nil", name, tag)
				}
				got = got.Elem()
			}

			var rendered string
			//nolint:exhaustive // only the kinds used by the default tags are rendered; others fail below
			switch got.Kind() {
			case reflect.String:
				rendered = got.String()
			case reflect.Int:
				rendered = strconv.FormatInt(got.Int(), 10)
			case reflect.Bool:
				rendered = strconv.FormatBool(got.Bool())
			default:
				t.Fatalf("%s: unsupported kind %s", name, got.Kind())
			}

			if rendered != tag {
				t.Errorf("%s: default tag = %q, DefaultConfig = %q", name, tag, rendered)
			}
		}
	}

	walk("Config", reflect.ValueOf(Config{}), reflect.ValueOf(*DefaultConfig()))
}

func TestDefaultConfigIsValidOnceBrokersAreSet(t *testing.T) {
	if err := DefaultConfig().Validate(); err == nil {
		t.Fatal("expected the missing brokers to be reported")
	}

	if err := validConfig().Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsUnrecognizedAndOutOfRangeValues(t *testing.T) {
	zero := 0
	negative := -1

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no brokers", func(c *Config) { c.Brokers = []string{" "} }, "brokers are required"},
		{"unknown acks", func(c *Config) { c.Producer.Acks = "quorum" }, "producer.acks"},
		{"unknown compression", func(c *Config) { c.Producer.Compression = unsupportedCompression }, "producer.compression"},
		{"negative retries", func(c *Config) { c.Producer.MaxRetries = &negative }, "producer.max_retries"},
		{"zero retries with idempotence", func(c *Config) { c.Producer.MaxRetries = &zero }, "enable_idempotence"},
		{"negative backoff", func(c *Config) { c.Producer.RetryBackoffMs = -1 }, "producer.retry_backoff_ms"},
		{"unknown offset reset", func(c *Config) { c.Consumer.AutoOffsetReset = "oldest" }, "consumer.auto_offset_reset"},
		{"heartbeat not below session", func(c *Config) { c.Consumer.HeartbeatMs = c.Consumer.SessionTimeoutMs }, "heartbeat_ms"},
		{"unknown protocol", func(c *Config) { c.Security.Protocol = "TLS" }, "security.protocol"},
		{"sasl without mechanism", func(c *Config) {
			c.Security.Protocol = protocolSASLSSL
			c.Security.SASLUsername = "u"
		}, "sasl_mechanism is required"},
		{"sasl without username", func(c *Config) {
			c.Security.Protocol = protocolSASLSSL
			c.Security.SASLMechanism = "PLAIN"
		}, "sasl_username is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := validConfig()
			tt.mutate(conf)

			err := conf.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	conf := validConfig()
	conf.Producer.Compression = unsupportedCompression
	conf.Consumer.Concurrency = -1

	err := conf.Validate()
	if err == nil || !strings.Contains(err.Error(), "producer.compression") || !strings.Contains(err.Error(), "consumer.concurrency") {
		t.Fatalf("Validate() error = %v, want both problems reported", err)
	}
}

// A loader that applies `default` tags fills both sections even when the
// service uses one role, so a present consumer section with no group or topics
// must not fail the config of a producer-only service.
func TestValidateAcceptsLoaderShapedProducerOnlyConfig(t *testing.T) {
	conf := DefaultConfig()
	conf.Brokers = []string{"localhost:9092"}

	if err := conf.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want a producer-only config accepted", err)
	}
	if err := conf.ValidateProducer(); err != nil {
		t.Fatalf("ValidateProducer() error = %v", err)
	}

	err := conf.ValidateConsumer()
	if err == nil || !strings.Contains(err.Error(), "consumer.group") || !strings.Contains(err.Error(), "consumer.topics") {
		t.Fatalf("ValidateConsumer() error = %v, want the missing group and topics reported", err)
	}
}

func TestValidateConsumerRequiresGroupAndTopics(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"missing group": func(c *Config) { c.Consumer.Group = "" },
		"blank topics":  func(c *Config) { c.Consumer.Topics = []string{" , "} },
	} {
		t.Run(name, func(t *testing.T) {
			conf := validConfig()
			mutate(conf)

			if err := conf.ValidateConsumer(); err == nil {
				t.Fatal("ValidateConsumer() expected an error")
			}
		})
	}
}

func TestValidateRolesRequireTheirSection(t *testing.T) {
	noProducer := validConfig()
	noProducer.Producer = nil
	if err := noProducer.ValidateProducer(); err == nil {
		t.Fatal("ValidateProducer() expected an error without a producer section")
	}
	if err := noProducer.ValidateConsumer(); err != nil {
		t.Fatalf("ValidateConsumer() error = %v, want it independent of the producer", err)
	}

	noConsumer := validConfig()
	noConsumer.Consumer = nil
	if err := noConsumer.ValidateConsumer(); err == nil {
		t.Fatal("ValidateConsumer() expected an error without a consumer section")
	}
	if err := noConsumer.ValidateProducer(); err != nil {
		t.Fatalf("ValidateProducer() error = %v, want it independent of the consumer", err)
	}
}

func TestNewProducerConfigByHonorsExplicitZeroRetriesWithoutIdempotence(t *testing.T) {
	zero := 0
	disabled := false
	producerConfig, err := NewProducerConfigBy(&Config{
		Brokers:  []string{"localhost:9092"},
		Producer: &ProducerSettings{MaxRetries: &zero, EnableIdempotence: &disabled},
	})
	if err != nil {
		t.Fatalf("NewProducerConfigBy() error = %v", err)
	}

	if got := producerConfig.ProducerConfig.Producer.Retry.Max; got != 0 {
		t.Fatalf("Retry.Max = %d, want 0 (explicit zero must not be replaced by the default)", got)
	}
}

func TestNewProducerConfigByRejectsInvalidConfig(t *testing.T) {
	conf := validConfig()
	conf.Producer.Compression = unsupportedCompression

	if _, err := NewProducerConfigBy(conf); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestNewProducerConfigByIgnoresInvalidConsumerSection(t *testing.T) {
	conf := validConfig()
	conf.Consumer.Group = ""

	if _, err := NewProducerConfigBy(conf); err != nil {
		t.Fatalf("producer must not depend on consumer settings: %v", err)
	}
}

func TestAcksMapToSarama(t *testing.T) {
	tests := map[string]sarama.RequiredAcks{
		"0":   sarama.NoResponse,
		"1":   sarama.WaitForLocal,
		"all": sarama.WaitForAll,
		"-1":  sarama.WaitForAll, // Kafka treats -1 as all
		"":    sarama.WaitForAll, // unset means the default
	}

	for acks, want := range tests {
		t.Run("acks="+acks, func(t *testing.T) {
			conf := validConfig()
			conf.Producer.Acks = acks
			// An idempotent producer requires acks=all, which would hide the value under test.
			off := false
			conf.Producer.EnableIdempotence = &off

			producerConfig, err := NewProducerConfigBy(conf)
			if err != nil {
				t.Fatalf("NewProducerConfigBy() error = %v, want %q accepted", err, acks)
			}
			if got := producerConfig.ProducerConfig.Producer.RequiredAcks; got != want {
				t.Fatalf("Sarama RequiredAcks = %d, want %d", got, want)
			}
		})
	}
}

func TestAcksRejectsOtherValues(t *testing.T) {
	for _, acks := range []string{"2", "quorum", "ALL", "-2", "none", "local"} {
		conf := validConfig()
		conf.Producer.Acks = acks

		err := conf.Validate()
		if err == nil || !strings.Contains(err.Error(), "producer.acks") || !strings.Contains(err.Error(), "want one of: 0, 1, all, -1") {
			t.Fatalf("Validate() with acks %q error = %v, want it rejected with the accepted values listed", acks, err)
		}
	}
}

// An idempotent producer (the default) needs acks=all and one in-flight request.
// A config that asks for something else is rejected, not silently overridden.
func TestIdempotenceConflictsAreRejectedNotOverridden(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ProducerSettings)
		want   string
	}{
		{"acks 0", func(p *ProducerSettings) { p.Acks = "0" }, "producer.acks"},
		{"acks 1", func(p *ProducerSettings) { p.Acks = "1" }, "producer.acks"},
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

			// With idempotence off the setting is applied as written.
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
	for _, acks := range []string{"", "all", "-1"} {
		conf := validConfig()
		conf.Producer.Acks = acks

		if err := conf.Validate(); err != nil {
			t.Fatalf("Validate() with acks %q error = %v, want it accepted while idempotent", acks, err)
		}
	}
}
