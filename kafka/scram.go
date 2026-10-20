package kafka

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/xdg-go/scram"
)

type kafkaScramClient struct {
	*scram.Client
	*scram.ClientConversation
	hashGen scram.HashGeneratorFcn
}

func newKafkaScramClientSHA256() *kafkaScramClient {
	return &kafkaScramClient{hashGen: sha256.New}
}

func newKafkaScramClientSHA512() *kafkaScramClient {
	return &kafkaScramClient{hashGen: sha512.New}
}

func (c *kafkaScramClient) Begin(userName, password, authzID string) error {
	client, err := c.hashGen.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}

	c.Client = client
	c.ClientConversation = client.NewConversation()

	return nil
}

func (c *kafkaScramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *kafkaScramClient) Done() bool {
	return c.ClientConversation.Done()
}
