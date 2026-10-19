package kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/IBM/sarama"
)

// Validate reports every problem with the values in c, joined into one error:
// brokers, security, and the settings of each producer and consumer section
// that is present. Unset (zero) tuning fields are valid and resolve to their
// documented defaults; Validate only rejects values that are out of range, not
// recognized, or that contradict each other.
//
// Validate does not require a role to be configured. A config decoded by a
// loader that applies `default` tags has both sections filled with defaults,
// even in a producer-only service, so a section that is present proves
// nothing. What a role cannot run without (a consumer's group and topics) is
// checked by ValidateProducer and ValidateConsumer, which NewPublisher and
// NewSubscriber also apply.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("kafka config is required")
	}

	errs := c.validateCommon()
	if c.Producer != nil {
		errs = append(errs, c.Producer.validate()...)
	}
	if c.Consumer != nil {
		errs = append(errs, c.Consumer.validate()...)
	}

	return errors.Join(errs...)
}

// ValidateProducer checks that c can build a Publisher: Validate's checks of
// the brokers, security and producer values, and that a producer section exists.
func (c *Config) ValidateProducer() error {
	if c == nil {
		return errors.New("kafka config is required")
	}
	if c.Producer == nil {
		return errors.New("kafka producer config is required")
	}

	errs := c.validateCommon()
	errs = append(errs, c.Producer.validate()...)

	return errors.Join(errs...)
}

// ValidateConsumer checks that c can build a Subscriber: Validate's checks of
// the brokers, security and consumer values, and that a consumer section exists
// with a group and at least one topic.
func (c *Config) ValidateConsumer() error {
	if c == nil {
		return errors.New("kafka config is required")
	}
	if c.Consumer == nil {
		return errors.New("kafka consumer config is required")
	}

	errs := c.validateCommon()
	errs = append(errs, c.Consumer.validateRequired()...)
	errs = append(errs, c.Consumer.validate()...)

	return errors.Join(errs...)
}

func (c *Config) validateCommon() []error {
	var errs []error

	hasBroker := false
	for _, broker := range c.Brokers {
		if strings.TrimSpace(broker) != "" {
			hasBroker = true

			break
		}
	}
	if !hasBroker {
		errs = append(errs, errors.New("brokers are required"))
	}

	return append(errs, c.Security.validate()...)
}

func (p *ProducerSettings) validate() []error {
	var errs []error

	errs = appendUnlessOneOf(errs, "producer.acks", string(p.Acks), acksNames()...)
	errs = appendUnlessOneOf(errs, "producer.compression", p.Compression, "none", "gzip", "snappy", "lz4", "zstd")
	errs = appendIfNegative(errs, "producer.retry_backoff_ms", p.RetryBackoffMs)
	errs = appendIfNegative(errs, "producer.max_open_requests", p.MaxOpenRequests)
	errs = appendIfNegative(errs, "producer.max_message_bytes", p.MaxMessageBytes)

	if p.MaxRetries != nil {
		errs = appendIfNegative(errs, "producer.max_retries", *p.MaxRetries)
	}
	if idempotenceEnabled(p.EnableIdempotence) {
		if effectiveMaxRetries(p.MaxRetries) <= 0 {
			errs = append(errs, errors.New("producer.max_retries must be greater than 0 while producer.enable_idempotence is true"))
		}

		// Sarama cannot run an idempotent producer with weaker acks or more than
		// one in-flight request. Reject the combination instead of overriding
		// what the config asked for; the loader's default tags make an implicit
		// idempotence indistinguishable from an explicit one.
		if parseKafkaAcks(p.Acks) != sarama.WaitForAll {
			errs = append(errs, fmt.Errorf("producer.acks %q needs producer.enable_idempotence set to false: an idempotent producer requires acks all", p.Acks))
		}
		if maxOpen := defaultIfZero(p.MaxOpenRequests, defaultProducerMaxOpenRequests); maxOpen > 1 {
			errs = append(errs, fmt.Errorf("producer.max_open_requests %d needs producer.enable_idempotence set to false: this client allows one in-flight request for an idempotent producer", maxOpen))
		}
	}

	return errs
}

// validateRequired checks what a subscriber cannot run without.
func (c *ConsumerSettings) validateRequired() []error {
	var errs []error

	if c.Group == "" {
		errs = append(errs, errors.New("consumer.group is required"))
	}
	if len(normalizeKafkaTopics(c.Topics)) == 0 {
		errs = append(errs, errors.New("consumer.topics are required"))
	}

	return errs
}

// validate checks the values of the consumer settings.
func (c *ConsumerSettings) validate() []error {
	var errs []error

	errs = appendUnlessOneOf(errs, "consumer.auto_offset_reset", c.AutoOffsetReset, "earliest", "latest")
	errs = appendIfNegative(errs, "consumer.concurrency", c.Concurrency)
	errs = appendIfNegative(errs, "consumer.session_timeout_ms", c.SessionTimeoutMs)
	errs = appendIfNegative(errs, "consumer.heartbeat_ms", c.HeartbeatMs)
	errs = appendIfNegative(errs, "consumer.max_fetch_bytes", c.MaxFetchBytes)
	errs = appendIfNegative(errs, "consumer.batch_max_messages", c.BatchMaxMessages)
	errs = appendIfNegative(errs, "consumer.batch_flush_interval_ms", c.BatchFlushIntervalMs)

	sessionTimeoutMs := defaultIfZero(c.SessionTimeoutMs, defaultSessionTimeoutMs)
	heartbeatMs := defaultIfZero(c.HeartbeatMs, defaultHeartbeatMs)
	if heartbeatMs >= sessionTimeoutMs {
		errs = append(errs, fmt.Errorf("consumer.heartbeat_ms (%d) must be less than consumer.session_timeout_ms (%d)", heartbeatMs, sessionTimeoutMs))
	}

	return errs
}

func (s *SecurityConfig) validate() []error {
	if s == nil {
		return nil
	}

	var errs []error

	errs = appendUnlessOneOf(errs, "security.protocol", s.Protocol, protocolPlaintext, protocolSSL, protocolSASLPlaintext, protocolSASLSSL)

	if s.Protocol == protocolSASLPlaintext || s.Protocol == protocolSASLSSL {
		errs = appendUnlessOneOf(errs, "security.sasl_mechanism", s.SASLMechanism, "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
		if s.SASLMechanism == "" {
			errs = append(errs, fmt.Errorf("security.sasl_mechanism is required for protocol %s", s.Protocol))
		}
		if s.SASLUsername == "" {
			errs = append(errs, fmt.Errorf("security.sasl_username is required for protocol %s", s.Protocol))
		}
	}

	return errs
}

// appendUnlessOneOf appends an error when value is set but not in allowed.
// An empty value is valid: it means "use the default".
func appendUnlessOneOf(errs []error, field, value string, allowed ...string) []error {
	if value == "" {
		return errs
	}
	for _, candidate := range allowed {
		if value == candidate {
			return errs
		}
	}

	return append(errs, fmt.Errorf("%s %q is not supported (want one of: %s)", field, value, strings.Join(allowed, ", ")))
}

func appendIfNegative(errs []error, field string, value int) []error {
	if value < 0 {
		return append(errs, fmt.Errorf("%s must not be negative, got %d", field, value))
	}

	return errs
}

// effectiveMaxRetries resolves MaxRetries: nil means the documented default,
// while an explicit value (including 0) is kept.
func effectiveMaxRetries(maxRetries *int) int {
	if maxRetries == nil {
		return defaultProducerMaxRetries
	}

	return *maxRetries
}
