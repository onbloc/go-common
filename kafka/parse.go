package kafka

import "github.com/IBM/sarama"

func parseKafkaRequiredAcks(value string) sarama.RequiredAcks {
	switch value {
	case "none":
		return sarama.NoResponse
	case "local":
		return sarama.WaitForLocal
	case "all":
		return sarama.WaitForAll
	default:
		return sarama.WaitForAll
	}
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
