package kafka

import "github.com/IBM/sarama"

// parseKafkaAcks maps a Acks spelling to Sarama's value. An
// empty or unknown value is the default, acks=all; Config.Validate rejects the
// unknown ones before they get here.
func parseKafkaAcks(value Acks) sarama.RequiredAcks {
	for _, entry := range acksSpellings {
		if entry.spelling == value {
			return entry.value
		}
	}

	return sarama.WaitForAll
}

func parseKafkaCompression(value string) sarama.CompressionCodec {
	switch value {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	default:
		return sarama.CompressionLZ4
	}
}

func parseKafkaAutoOffsetReset(value string) int64 {
	switch value {
	case "latest":
		return sarama.OffsetNewest
	case "earliest":
		return sarama.OffsetOldest
	default:
		return sarama.OffsetOldest
	}
}
