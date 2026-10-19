package kafka

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

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

func TestDefaultConfigIsValidOnceConnectionFieldsAreSet(t *testing.T) {
	if err := DefaultConfig().Validate(); err == nil {
		t.Fatal("expected brokers/group/topics to be reported as missing")
	}

	if err := validConfig().Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateAllowsProducerOnlyAndConsumerOnly(t *testing.T) {
	producerOnly := validConfig()
	producerOnly.Consumer = nil
	if err := producerOnly.Validate(); err != nil {
		t.Fatalf("producer-only Validate() error = %v", err)
	}

	consumerOnly := validConfig()
	consumerOnly.Producer = nil
	if err := consumerOnly.Validate(); err != nil {
		t.Fatalf("consumer-only Validate() error = %v", err)
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
		{"unknown acks", func(c *Config) { c.Producer.RequiredAcks = "quorum" }, "producer.required_acks"},
		{"unknown compression", func(c *Config) { c.Producer.Compression = "brotli" }, "producer.compression"},
		{"negative retries", func(c *Config) { c.Producer.MaxRetries = &negative }, "producer.max_retries"},
		{"zero retries with idempotence", func(c *Config) { c.Producer.MaxRetries = &zero }, "enable_idempotence"},
		{"negative backoff", func(c *Config) { c.Producer.RetryBackoffMs = -1 }, "producer.retry_backoff_ms"},
		{"unknown offset reset", func(c *Config) { c.Consumer.AutoOffsetReset = "oldest" }, "consumer.auto_offset_reset"},
		{"missing group", func(c *Config) { c.Consumer.Group = "" }, "consumer.group"},
		{"blank topics", func(c *Config) { c.Consumer.Topics = []string{" , "} }, "consumer.topics"},
		{"heartbeat not below session", func(c *Config) { c.Consumer.HeartbeatMs = c.Consumer.SessionTimeoutMs }, "heartbeat_ms"},
		{"unknown protocol", func(c *Config) { c.Security.Protocol = "TLS" }, "security.protocol"},
		{"sasl without mechanism", func(c *Config) {
			c.Security.Protocol = "SASL_SSL"
			c.Security.SASLUsername = "u"
		}, "sasl_mechanism is required"},
		{"sasl without username", func(c *Config) {
			c.Security.Protocol = "SASL_SSL"
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
	conf.Producer.Compression = "brotli"
	conf.Consumer.Group = ""

	err := conf.Validate()
	if err == nil || !strings.Contains(err.Error(), "producer.compression") || !strings.Contains(err.Error(), "consumer.group") {
		t.Fatalf("Validate() error = %v, want both problems reported", err)
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
	conf.Producer.Compression = "brotli"

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
