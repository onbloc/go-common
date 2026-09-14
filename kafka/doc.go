// Package kafka provides a Producer/Consumer wrapper around Sarama for
// publishing and consuming Kafka messages.
//
// Applications own their configuration struct and decode it with a config
// loader such as github.com/onbloc/go-common/config; this package only needs
// the resulting *Config:
//
//	conf := &kafka.Config{
//		Brokers:  []string{"localhost:9092"},
//		Producer: &kafka.ProducerSettings{RequiredAcks: "all", Compression: "lz4"},
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
