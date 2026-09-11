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

### Publishing

```go
publisher, err := kafka.NewPublisher(conf.Kafka, log)
if err != nil {
    return err
}

err = publisher.PublishMessage(kafka.Message{Topic: "orders", Key: "order-1", Value: "{...}"})
```

Call `kafka.NewProducer` directly instead of `NewPublisher` when the caller needs `Close()`; it returns the concrete `*kafka.Producer`.

### Consuming

```go
subscriber, err := kafka.NewSubscriber(conf.Kafka, log)
if err != nil {
    return err
}

err = subscriber.StartListening(ctx, func(message string, meta kafka.Metadata, ack func() error) error {
    // handle message
    return ack()
})
```

`StartBatchListening` groups messages up to `BatchConfig.MaxMessages` or until `FlushInterval` elapses, whichever comes first, and commits the offset once per batch:

```go
err = subscriber.StartBatchListening(ctx, conf.Kafka.Consumer.BatchConfig(), func(messages []kafka.BatchMessage, ack func() error) error {
    // handle messages
    return ack()
})
```

When `Config.Consumer.Concurrency` is greater than 1, `NewSubscriber` returns a `*kafka.ConcurrentConsumer` that runs that many consumer instances in the same group to process partitions in parallel.

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
