# kafka

`kafka` wraps [Sarama](https://github.com/IBM/sarama) with a Producer/Consumer API for publishing and consuming Kafka messages, including batched consumption and SASL/TLS security.

## Installation

Requires Go 1.24 or later. Once the repository is public and `kafka/v0.1.0` is published, install the module with:

```bash
go get github.com/onbloc/go-common/kafka@v0.1.0
```

Public downloads do not require GitHub authentication or `GOPRIVATE`.

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
    acks: all
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

`Validate` reports every problem with the values at once: missing brokers, unrecognized `acks` / `compression` / `auto_offset_reset` / `protocol` / `sasl_mechanism` values, negative numbers, a heartbeat not shorter than the session timeout, and `max_retries: 0` while idempotence is on.

`Validate` does not require a role to be configured, so it is safe to call at bootstrap in a producer-only or consumer-only service. This matters with a loader that applies `default` tags (such as `go-common/config`): it fills both the `producer` and `consumer` sections with defaults even when your service uses only one. What a role cannot run without is checked by `ValidateProducer` (a producer section) and `ValidateConsumer` (a consumer section with a group and at least one topic); `NewPublisher` and `NewSubscriber` apply them too, so a missing consumer group is reported when the subscriber is built, or earlier if you call `ValidateConsumer` yourself.

Unset numeric fields resolve to their defaults. `max_retries` and `enable_idempotence` are pointers so an explicit `0` / `false` is honored rather than treated as unset.

### Producer acknowledgements

`producer.acks` is Kafka's `acks` setting: how many broker acknowledgements a publish waits for before it succeeds.

| Value | Waits for |
| --- | --- |
| `0` | nothing; a message can be lost without the producer noticing |
| `1` | the partition leader to write it |
| `all` (default), or `-1` | every in-sync replica |

Any other value is rejected by `Validate`. An idempotent producer (`enable_idempotence`, on by default) always uses `all`, whatever is set here; turn idempotence off to use `0` or `1`. In YAML or an environment variable an unquoted `1` works, and in code the typed constants `kafka.AcksNone`, `kafka.AcksLeader` and `kafka.AcksAll` are available.

### Publishing

```go
publisher, err := kafka.NewPublisher(conf.Kafka, log)
if err != nil {
    return err
}

err = publisher.PublishMessage(kafka.Message{Topic: "orders", Key: []byte("order-1"), Value: []byte("{...}")})
```

`Message.Value` is `[]byte`, so any payload encoding (JSON, protobuf, raw binary) works. A nil `Value` publishes a Kafka tombstone (a null-value record, typically used to mark a key for deletion on a compacted topic). `Message.Key` is `[]byte` as well; leave it nil to send no key (see [Ordering and message keys](#ordering-and-message-keys)).

`Message.Headers` carries optional record headers (for example a trace ID or schema version). Keys must not be empty; they may repeat and keep their order. Consumers receive them, along with the record `Timestamp`, in `Metadata`. The timestamp is the producer's create time or the broker's log-append time, depending on the topic's `message.timestamp.type`.

`PublishMessage` and `PublishMessages` are safe to call concurrently. `PublishMessages` accepts messages for any mix of topics and validates all of them before sending any; if some fail to send, the returned error (a `sarama.ProducerErrors`) lists the failed messages while others may already be written, so retrying the whole call can duplicate them.

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

`StartBatchListening` groups messages up to `BatchConfig.MaxMessages` or until `FlushInterval` elapses, whichever comes first, and `commit` stores the offset once per batch:

```go
err = subscriber.StartBatchListening(ctx, conf.Kafka.Consumer.BatchConfig(), func(messages []kafka.BatchMessage, commit func() error) error {
    // handle messages
    return commit()
})
```

When `Config.Consumer.Concurrency` is greater than 1, `NewSubscriber` returns a `*kafka.ConcurrentConsumer` that runs that many consumer instances in the same group. Call `subscriber.Close()` on shutdown to stop consuming and close the consumer group(s).

### Delivery semantics

Delivery is at-least-once. The consumer never commits offsets on its own (auto-commit is off): the handler's `commit` function does.

- `commit()` stores the consumer group's offset for the messages of the batch (one message with `StartListening`), so they are not delivered again. It also covers every earlier offset of that partition and cannot be undone: call it last, after processing has succeeded.
- A handler that returns without calling `commit()` leaves its messages uncommitted. They are delivered again after a restart or rebalance, unless a later `commit()` on the same partition covers them.
- `commit()` always returns `nil`: Sarama does not report commit failures, so a failed commit shows up as a redelivery rather than an error.
- When a rebalance or shutdown ends the session, a batch that was still filling is dropped without a commit and delivered again to whichever consumer takes the partition next. This is not reported as an error.
- A crash between processing and `commit()` delivers the batch again, so handlers must be safe to run twice on the same messages.
- `StartListening` and `StartBatchListening` return the context's error (`context.Canceled`) when stopped through `ctx` or `Close()`; treat that as a normal stop. They return only then: errors from the consumer group are logged and retried every second, not returned (see [Observing consumer errors](#observing-consumer-errors)).

### Handler errors

When a handler returns an error, the module logs it through `Logger.Errorf` and returns it from the partition's claim. Sarama ends the whole group session as soon as any claim returns, so every partition of this consumer stops, the consumer joins the group again, and the uncommitted batch is delivered again. A message that can never succeed therefore blocks its partition and is retried in a loop.

There is no built-in retry, skip, or dead-letter policy; decide in the handler:

```go
err = subscriber.StartListening(ctx, func(message []byte, meta kafka.Metadata, commit func() error) error {
    if err := process(ctx, message); err != nil {
        if isPermanent(err) {
            log.Errorf("dropping %s[%d]@%d: %v", meta.Topic, meta.Partition, meta.Offset, err)
            // publish to a dead-letter topic here if the message must be kept
            return commit() // skip it
        }

        return err // transient: deliver again
    }

    return commit()
})
```

With `StartBatchListening`, `commit` commits the whole batch, so skipping drops every message in it; retry or split such a batch yourself.

### Observing consumer errors

`StartListening` and `StartBatchListening` do not return consumer errors: an error from the consumer group is logged through `Logger.Errorf` and consuming is retried every second, and Sarama's own consume errors (for example a failed fetch) only reach Sarama's package logger, which discards them by default. Register an error handler to observe all of them:

```go
subscriber, err := kafka.NewSubscriber(conf.Kafka, log, kafka.WithConsumerErrorHandler(func(e *kafka.ConsumerError) {
    metrics.ConsumeErrors.WithLabelValues(e.Topic).Inc()
    log.Errorf("consume error: %v", e)
}))
```

- It receives errors from the consumer group, errors Sarama reports for a partition, and errors returned by your own message handler. A `ConsumerError` names the topic partition it came from (for a group-level error `Topic` is empty and `Partition` is `-1`) and wraps the underlying error, so `errors.Is` and `errors.As` work on it.
- It is for observation only. It does not change how the consumer retries, commits or ends a session. To stop consuming, cancel the context passed to `Start*` or call `Close()`.
- It is called from the consumer's goroutines, concurrently with your message handler and, with several partitions or `Concurrency` above 1, with itself. It must be safe for concurrent use and return quickly: a slow handler delays consuming, and errors Sarama has no room to queue are dropped.
- Registering a handler is what makes Sarama report its errors on the group's error stream; the config you pass in is not modified. Without a handler nothing changes.

### Ordering and message keys

Kafka orders messages only within a partition. The default partitioner hashes `Message.Key`, so messages with the same key go to the same partition and are consumed in publish order. A message with a nil `Key` (no key) has no such guarantee: it goes to a random partition and has no ordering relative to other messages. To process messages strictly in order, publish them under one key; one consumer in the group then handles them at a time.

The key is raw bytes, and Kafka tells a missing key from an empty one: a nil `Key` is sent as a null key, while an empty non-nil `Key` (`[]byte{}`) is a real key like any other, so it is hashed and all messages with an empty key share one partition. The difference matters for partitioning and for compacted topics. `Metadata.Key` keeps it too: it is nil when the record has no key.

### Concurrency

Within a consumer group each partition is read by one member, so parallelism is limited by the number of partitions: a `Concurrency` above the partition count only adds idle members, and it does not speed up a single partition. Sarama runs one goroutine per assigned topic partition, so a consumer that owns several partitions calls your handler from several goroutines at once; the handler must be safe for concurrent use. A batch passed to a `BatchMessageHandler` always comes from one topic partition.

### Message size

`producer.max_message_bytes` defaults to 5 MiB, but Kafka brokers accept about 1 MB per message by default (`message.max.bytes`). Raise the broker limit (and the topic's `max.message.bytes`) to match, or the broker rejects the publish with a message-too-large error. Raise `consumer.max_fetch_bytes` as well if you consume large messages.

### Logging

`log` above is any value with `Infof`, `Errorf` and `Debugf` methods of the form `(format string, args ...any)` (`kafka.Logger`); most application loggers satisfy this without an adapter. Pass `nil` to discard log output.

Per-message and per-batch activity (published, received, processed) is logged at debug level only, so it stays quiet at normal log levels. Failures are logged through `Errorf`.

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
