package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

func applyKafkaSecurity(sc *sarama.Config, sec *SecurityConfig) error {
	if sec == nil {
		return nil
	}

	if sec.Protocol == "SASL_SSL" || sec.Protocol == "SASL_PLAINTEXT" {
		applyKafkaSASL(sc, sec)
	}

	if sec.Protocol == "SSL" || sec.Protocol == "SASL_SSL" {
		if err := applyKafkaTLS(sc, sec); err != nil {
			return err
		}
	}

	return nil
}

func applyKafkaSASL(sc *sarama.Config, sec *SecurityConfig) {
	sc.Net.SASL.Enable = true
	sc.Net.SASL.User = sec.SASLUsername
	sc.Net.SASL.Password = sec.SASLPassword

	switch sec.SASLMechanism {
	case "SCRAM-SHA-256":
		sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return newKafkaScramClientSHA256()
		}
	case "SCRAM-SHA-512":
		sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return newKafkaScramClientSHA512()
		}
	case "PLAIN":
		sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	default:
		sc.Net.SASL.Mechanism = sarama.SASLMechanism(sec.SASLMechanism)
	}
}

func applyKafkaTLS(sc *sarama.Config, sec *SecurityConfig) error {
	sc.Net.TLS.Enable = true
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	if sec.TLSInsecure {
		tlsConfig.InsecureSkipVerify = true
	}

	if sec.TLSCAFile != "" {
		certPool, err := loadKafkaRootCAs(sec.TLSCAFile)
		if err != nil {
			return err
		}
		tlsConfig.RootCAs = certPool
	}

	if sec.TLSClientCertFile != "" || sec.TLSClientKeyFile != "" {
		if sec.TLSClientCertFile == "" || sec.TLSClientKeyFile == "" {
			return fmt.Errorf("both tls_client_cert_file and tls_client_key_file are required for Kafka mutual TLS")
		}

		cert, err := tls.LoadX509KeyPair(sec.TLSClientCertFile, sec.TLSClientKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load Kafka TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	sc.Net.TLS.Config = tlsConfig

	return nil
}

func loadKafkaRootCAs(caFile string) (*x509.CertPool, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read Kafka TLS CA file: %w", err)
	}

	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("failed to parse Kafka TLS CA file: %s", caFile)
	}

	return certPool, nil
}
