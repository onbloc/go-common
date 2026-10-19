// Package kafka provides a Producer/Consumer wrapper around Sarama for
// publishing and consuming Kafka messages.
//
// Configuration is a plain *Config struct; this package does not depend on any
// config loader. Decode it with any mapstructure-aware loader (such as
// github.com/onbloc/go-common/config, which applies the `default` tags), or
// build it in code starting from DefaultConfig:
//
//	conf := kafka.DefaultConfig()
//	conf.Brokers = []string{"localhost:9092"}
//	if err := conf.Validate(); err != nil {
//		return err
//	}
//	publisher, err := kafka.NewPublisher(conf, nil)
//
// Consumer.StartListening delivers one message at a time; StartBatchListening
// groups messages up to BatchConfig.MaxMessages or until FlushInterval
// elapses, whichever comes first. Delivery is at-least-once: the consumer
// never commits offsets itself, the handler calls its commit function once
// processing has succeeded. A handler error ends the consumer group session
// and the uncommitted batch is delivered again; retry, skip and dead-letter
// policy is left to the handler. Sarama runs one goroutine per assigned
// partition, so handlers may be called concurrently.
// NewConcurrentConsumer runs several consumer instances in the same group;
// parallelism is limited by the number of partitions. Start* return only when
// the context ends or the consumer is closed: errors from the consumer group
// are logged and retried every second, not returned. Use
// WithConsumerErrorHandler to observe them.
//
// Kafka orders messages only within a partition: messages with the same
// Message.Key are consumed in publish order, messages with a nil key are not
// ordered relative to each other. Keys are raw bytes; a nil key and an empty
// key are different in Kafka and stay different here.
//
// A Logger may be supplied through WithProducerLogger/WithConsumerLogger; any
// type with Errorf and Debugf methods satisfies it. Per-message
// activity is logged at debug level. Without a logger, log output is
// discarded.
package kafka
