package kafka

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// Fallback consumer group timing, mirroring the struct `default` tags on
// ConsumerSettings and Sarama's own defaults. Applied when the configured
// values are zero (e.g. slice-element consumers that struct-tag defaulting
// cannot reach).
const (
	defaultSessionTimeoutMs = 10000
	defaultHeartbeatMs      = 3000

	defaultBatchMaxMessages     = 200
	defaultBatchFlushIntervalMs = 1000
)

// Fallback producer tuning, mirroring the struct `default` tags on
// ProducerSettings. Applied when ProducerSettings is built directly (e.g. in
// code, without a config loader applying struct-tag defaults), where the zero
// value would otherwise silently disable retries and idempotence.
const (
	defaultProducerMaxRetries      = 3
	defaultProducerRetryBackoffMs  = 100
	defaultProducerMaxOpenRequests = 1
	defaultProducerMaxMessageBytes = 5242880
)

// Config is the application-owned Kafka configuration. Decode it with a
// mapstructure-aware config loader (such as github.com/onbloc/go-common/config)
// or construct it directly.
type Config struct {
	Producer *ProducerSettings `mapstructure:"producer"`
	Consumer *ConsumerSettings `mapstructure:"consumer"`
	Security *SecurityConfig   `mapstructure:"security"`
	ClientID string            `mapstructure:"client_id" default:""`
	Brokers  []string          `mapstructure:"brokers"`
}

type ProducerSettings struct {
	RequiredAcks    string `mapstructure:"required_acks" default:"all"`
	Compression     string `mapstructure:"compression" default:"lz4"`
	MaxRetries      int    `mapstructure:"max_retries" default:"3"`
	RetryBackoffMs  int    `mapstructure:"retry_backoff_ms" default:"100"`
	MaxOpenRequests int    `mapstructure:"max_open_requests" default:"1"`
	MaxMessageBytes int    `mapstructure:"max_message_bytes" default:"5242880"`
	// EnableIdempotence defaults to true when nil. Use a pointer so an
	// explicit false (disable idempotence) is distinguishable from an unset
	// field, which a plain bool cannot represent.
	EnableIdempotence *bool `mapstructure:"enable_idempotence" default:"true"`
}

type ConsumerSettings struct {
	Topics           []string `mapstructure:"topics"`
	Group            string   `mapstructure:"group" default:""`
	Concurrency      int      `mapstructure:"concurrency" default:"1"`
	AutoOffsetReset  string   `mapstructure:"auto_offset_reset" default:"earliest"`
	SessionTimeoutMs int      `mapstructure:"session_timeout_ms" default:"10000"`
	HeartbeatMs      int      `mapstructure:"heartbeat_ms" default:"3000"`
	MaxFetchBytes    int      `mapstructure:"max_fetch_bytes" default:"5242880"`
	// BatchMaxMessages and BatchFlushIntervalMs shape how many messages
	// StartBatchListening groups per offset commit; see BatchConfig.
	BatchMaxMessages     int `mapstructure:"batch_max_messages" default:"200"`
	BatchFlushIntervalMs int `mapstructure:"batch_flush_interval_ms" default:"1000"`
}

// BatchConfig returns the consumer's batching knobs as a kafka.BatchConfig,
// falling back to defaults when unset (a zero value would otherwise mean "one
// message per commit").
func (c *ConsumerSettings) BatchConfig() BatchConfig {
	maxMessages := c.BatchMaxMessages
	if maxMessages <= 0 {
		maxMessages = defaultBatchMaxMessages
	}

	flushIntervalMs := c.BatchFlushIntervalMs
	if flushIntervalMs <= 0 {
		flushIntervalMs = defaultBatchFlushIntervalMs
	}

	return BatchConfig{
		MaxMessages:   maxMessages,
		FlushInterval: time.Duration(flushIntervalMs) * time.Millisecond,
	}
}

type SecurityConfig struct {
	Protocol          string `mapstructure:"protocol" default:"PLAINTEXT"`
	SASLMechanism     string `mapstructure:"sasl_mechanism" default:""`
	SASLUsername      string `mapstructure:"sasl_username" default:""`
	SASLPassword      string `mapstructure:"sasl_password" default:""`
	TLSCAFile         string `mapstructure:"tls_ca_file" default:""`
	TLSClientCertFile string `mapstructure:"tls_client_cert_file" default:""`
	TLSClientKeyFile  string `mapstructure:"tls_client_key_file" default:""`
	TLSInsecure       bool   `mapstructure:"tls_insecure" default:"false"`
}

// ProducerConfig is the Sarama-ready configuration passed to NewProducer.
type ProducerConfig struct {
	ProducerConfig *sarama.Config
	Brokers        []string
}

// ConsumerConfig is the Sarama-ready configuration passed to NewConsumer.
type ConsumerConfig struct {
	ConsumerConfig *sarama.Config
	Topics         []string
	ConsumerGroup  string
	Brokers        []string
	Concurrency    int
}

func NewProducerConfigBy(conf *Config) (*ProducerConfig, error) {
	if conf == nil {
		return nil, fmt.Errorf("kafka config is required for Kafka producer")
	}

	if conf.Producer == nil {
		return nil, fmt.Errorf("producer config is required for Kafka producer")
	}

	producerConfig, err := buildProducerConfig(conf)
	if err != nil {
		return nil, err
	}

	return &ProducerConfig{
		Brokers:        conf.Brokers,
		ProducerConfig: producerConfig,
	}, nil
}

func NewConsumerConfigBy(conf *Config) (*ConsumerConfig, error) {
	if conf == nil {
		return nil, fmt.Errorf("kafka config is required for Kafka consumer")
	}

	if conf.Consumer == nil {
		return nil, fmt.Errorf("consumer config is required for Kafka consumer")
	}

	if conf.Consumer.Group == "" {
		return nil, fmt.Errorf("consumer group is required for Kafka consumer")
	}

	topics := normalizeKafkaTopics(conf.Consumer.Topics)
	if len(topics) == 0 {
		return nil, fmt.Errorf("topics are required for Kafka consumer")
	}

	concurrency := conf.Consumer.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	consumerConfig, err := buildConsumerConfig(conf)
	if err != nil {
		return nil, err
	}

	return &ConsumerConfig{
		Brokers:        conf.Brokers,
		Topics:         topics,
		ConsumerGroup:  conf.Consumer.Group,
		ConsumerConfig: consumerConfig,
		Concurrency:    concurrency,
	}, nil
}

func normalizeKafkaTopics(topics []string) []string {
	normalized := make([]string, 0, len(topics))
	for _, topic := range topics {
		for part := range strings.SplitSeq(topic, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				normalized = append(normalized, part)
			}
		}
	}

	return normalized
}

func buildProducerConfig(conf *Config) (*sarama.Config, error) {
	sc := sarama.NewConfig()
	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true

	applyProducerSettings(sc, conf.Producer)
	applyKafkaCommon(sc, conf.ClientID)
	if err := applyKafkaSecurity(sc, conf.Security); err != nil {
		return nil, err
	}

	return sc, nil
}

func buildConsumerConfig(conf *Config) (*sarama.Config, error) {
	sc := sarama.NewConfig()

	applyConsumerSettings(sc, conf.Consumer)
	applyKafkaCommon(sc, conf.ClientID)
	if err := applyKafkaSecurity(sc, conf.Security); err != nil {
		return nil, err
	}

	return sc, nil
}

func applyProducerSettings(sc *sarama.Config, p *ProducerSettings) {
	sc.Producer.RequiredAcks = parseKafkaRequiredAcks(p.RequiredAcks)
	sc.Producer.Compression = parseKafkaCompression(p.Compression)
	sc.Producer.Retry.Max = defaultIfZero(p.MaxRetries, defaultProducerMaxRetries)
	sc.Producer.Retry.Backoff = time.Duration(defaultIfZero(p.RetryBackoffMs, defaultProducerRetryBackoffMs)) * time.Millisecond
	sc.Producer.Idempotent = idempotenceEnabled(p.EnableIdempotence)
	sc.Producer.MaxMessageBytes = defaultIfZero(p.MaxMessageBytes, defaultProducerMaxMessageBytes)

	maxOpenRequests := defaultIfZero(p.MaxOpenRequests, defaultProducerMaxOpenRequests)
	if sc.Producer.Idempotent {
		// Sarama requires idempotent producers to use WaitForAll acks and at
		// most one in-flight request per broker connection. Keep this clamp
		// explicit so Kafka producer ordering/duplicate guarantees are not
		// accidentally disabled by a wider max_open_requests setting.
		sc.Producer.RequiredAcks = sarama.WaitForAll
		if maxOpenRequests > 1 {
			maxOpenRequests = 1
		}
	}
	sc.Net.MaxOpenRequests = maxOpenRequests
}

// defaultIfZero substitutes def for v when v is unset (zero or negative), the
// same "unset means apply the documented default" convention applyConsumerSettings
// uses for its own timing fields.
func defaultIfZero(v, def int) int {
	if v <= 0 {
		return def
	}

	return v
}

// idempotenceEnabled treats a nil EnableIdempotence as the documented default
// (true); only an explicit false disables idempotence.
func idempotenceEnabled(enabled *bool) bool {
	return enabled == nil || *enabled
}

func applyConsumerSettings(sc *sarama.Config, c *ConsumerSettings) {
	// Default timing when omitted. Configs that do not come from struct-tag
	// defaulting (built in code, or decoded outside the struct-tag walk)
	// arrive with zero tuning fields, and Sarama rejects a zero session
	// timeout. These fall back to the same values as the struct `default`
	// tags, which also match Sarama's own defaults.
	sessionTimeoutMs := c.SessionTimeoutMs
	if sessionTimeoutMs <= 0 {
		sessionTimeoutMs = defaultSessionTimeoutMs
	}
	heartbeatMs := c.HeartbeatMs
	if heartbeatMs <= 0 {
		heartbeatMs = defaultHeartbeatMs
	}

	sc.Consumer.Group.Session.Timeout = time.Duration(sessionTimeoutMs) * time.Millisecond
	sc.Consumer.Group.Heartbeat.Interval = time.Duration(heartbeatMs) * time.Millisecond
	sc.Consumer.Offsets.Initial = parseKafkaAutoOffsetReset(c.AutoOffsetReset)
	sc.Consumer.Offsets.AutoCommit.Enable = false

	if c.MaxFetchBytes > 0 {
		sc.Consumer.Fetch.Default = clampToInt32(c.MaxFetchBytes)
	}
}

// clampToInt32 saturates v to math.MaxInt32 instead of wrapping, since a
// misconfigured byte-size setting silently wrapping to a negative int32 would
// be far worse than clamping to the largest representable value.
func clampToInt32(v int) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}

	return int32(v) // #nosec G115 -- bounded by the check above
}

func applyKafkaCommon(sc *sarama.Config, clientID string) {
	if clientID != "" {
		sc.ClientID = clientID
	}

	sc.Net.ReadTimeout = 30 * time.Second
	sc.Net.WriteTimeout = 30 * time.Second
}
