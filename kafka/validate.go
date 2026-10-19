package kafka

import (
	"errors"
	"fmt"
	"strings"
)

// Validate reports every problem found in c, joined into one error. It checks
// the brokers and security settings, and each of the producer and consumer
// sections that is present. A nil section is valid here: a producer-only
// service need not configure a consumer.
//
// Unset (zero) tuning fields are valid and resolve to their documented
// defaults; Validate only rejects values that are out of range or not
// recognized, which would otherwise be silently replaced by a default.
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

// validateProducer checks only what NewPublisher depends on.
func (c *Config) validateProducer() error {
	errs := c.validateCommon()
	errs = append(errs, c.Producer.validate()...)

	return errors.Join(errs...)
}

// validateConsumer checks only what NewSubscriber depends on.
func (c *Config) validateConsumer() error {
	errs := c.validateCommon()
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

	errs = appendUnlessOneOf(errs, "producer.required_acks", p.RequiredAcks, "none", "local", "all")
	errs = appendUnlessOneOf(errs, "producer.compression", p.Compression, "none", "gzip", "snappy", "lz4", "zstd")
	errs = appendIfNegative(errs, "producer.retry_backoff_ms", p.RetryBackoffMs)
	errs = appendIfNegative(errs, "producer.max_open_requests", p.MaxOpenRequests)
	errs = appendIfNegative(errs, "producer.max_message_bytes", p.MaxMessageBytes)

	if p.MaxRetries != nil {
		errs = appendIfNegative(errs, "producer.max_retries", *p.MaxRetries)
	}
	if idempotenceEnabled(p.EnableIdempotence) && effectiveMaxRetries(p.MaxRetries) <= 0 {
		errs = append(errs, errors.New("producer.max_retries must be greater than 0 while producer.enable_idempotence is true"))
	}

	return errs
}

func (c *ConsumerSettings) validate() []error {
	var errs []error

	if c.Group == "" {
		errs = append(errs, errors.New("consumer.group is required"))
	}
	if len(normalizeKafkaTopics(c.Topics)) == 0 {
		errs = append(errs, errors.New("consumer.topics are required"))
	}

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
