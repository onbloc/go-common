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
// elapses, whichever comes first, and commits the offset once per batch.
// NewConcurrentConsumer runs several consumer instances in the same group to
// process partitions in parallel.
//
// A Logger may be supplied through WithProducerLogger/WithConsumerLogger; any
// type with Infof/Errorf methods satisfies it. Without one, log output is
// discarded.
package kafka
