# kafka

`kafka` wraps [Sarama](https://github.com/IBM/sarama) with a Producer/Consumer API for publishing and consuming Kafka messages, including batched consumption and SASL/TLS security.

## Installation

While this repository is private, configure Go and GitHub authentication before downloading the module:

```bash
go env -w GOPRIVATE=github.com/onbloc/*
go get github.com/onbloc/go-common/kafka@v0.1.0
```

## Usage

Applications own their configuration struct and decode it with a config loader such as [`github.com/onbloc/go-common/config`](../config):

```go
package bootstrap

import (
    commonconfig "github.com/onbloc/go-common/config"
    "github.com/onbloc/go-common/kafka"
)

type Config struct {
    Kafka *kafka.Config `mapstructure:"kafka"`
}

func LoadConfig() (*Config, error) {
    target := &Config{}
    if err := commonconfig.Load(target, commonconfig.Options{}); err != nil {
        return nil, err
    }

    return target, nil
}
```

```yaml
# config/config.yaml
kafka:
  brokers: ["localhost:9092"]
  producer:
    required_acks: all
    compression: lz4
  consumer:
    topics: ["orders"]
    group: order-processor
```

### Configuration in code

`Config` is a plain struct, so the loader above is a recommendation, not a requirement. Any mapstructure-aware loader works, or build it directly. `DefaultConfig()` returns every documented default (the same values the `default` struct tags give a loader); set brokers, topics, and group, then override whatever else you need:

```go
conf := kafka.DefaultConfig()
conf.Brokers = []string{"localhost:9092"}
conf.Consumer.Topics = []string{"orders"}
conf.Consumer.Group = "order-processor"

if err := conf.Validate(); err != nil {
    return err
}
```

`Validate` reports every problem at once: missing brokers, group or topics, unrecognized `required_acks` / `compression` / `auto_offset_reset` / `protocol` / `sasl_mechanism` values, negative numbers, a heartbeat not shorter than the session timeout, and `max_retries: 0` while idempotence is on. A nil `producer` or `consumer` section is valid, so a producer-only service need not configure a consumer. `NewPublisher` and `NewSubscriber` validate only the sections they use.

Unset numeric fields resolve to their defaults. `max_retries` and `enable_idempotence` are pointers so an explicit `0` / `false` is honored rather than treated as unset.

### Publishing

```go
publisher, err := kafka.NewPublisher(conf.Kafka, log)
if err != nil {
    return err
}

err = publisher.PublishMessage(kafka.Message{Topic: "orders", Key: "order-1", Value: []byte("{...}")})
```

`Message.Value` is `[]byte`, so any payload encoding (JSON, protobuf, raw binary) works. A nil `Value` publishes a Kafka tombstone (a null-value record, typically used to mark a key for deletion on a compacted topic).

Call `publisher.Close()` on shutdown to close the underlying producer.

### Consuming

```go
subscriber, err := kafka.NewSubscriber(conf.Kafka, log)
if err != nil {
    return err
}

err = subscriber.StartListening(ctx, func(message []byte, meta kafka.Metadata, commit func() error) error {
    // handle message
    return commit()
})
```

`StartBatchListening` groups messages up to `BatchConfig.MaxMessages` or until `FlushInterval` elapses, whichever comes first, and commits the offset once per batch:

```go
err = subscriber.StartBatchListening(ctx, conf.Kafka.Consumer.BatchConfig(), func(messages []kafka.BatchMessage, commit func() error) error {
    // handle messages
    return commit()
})
```

When `Config.Consumer.Concurrency` is greater than 1, `NewSubscriber` returns a `*kafka.ConcurrentConsumer` that runs that many consumer instances in the same group to process partitions in parallel. Call `subscriber.Close()` on shutdown to stop consuming and close the consumer group(s).

### Logging

`log` above is any value with `Infof(format string, args ...any)` and `Errorf(format string, args ...any)` methods (`kafka.Logger`); most application loggers satisfy this without an adapter. Pass `nil` to discard log output.

### Security

`Config.Security` configures `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`, with `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` SASL mechanisms and optional mutual TLS.

```yaml
kafka:
  security:
    protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: app
    sasl_password: secret
```
