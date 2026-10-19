package kafka

import (
	"bytes"
	"testing"
)

// Kafka tells a null key from an empty one, and so do partitioning and
// compaction, so the producer must keep the difference.
func TestPublishMessageDistinguishesNilEmptyAndBinaryKeys(t *testing.T) {
	tests := []struct {
		name    string
		key     []byte
		wantNil bool
		want    []byte
	}{
		{"nil key is a null key", nil, true, nil},
		{"empty key is an empty key", []byte{}, false, []byte{}},
		{"binary key is preserved byte for byte", []byte{0xff, 0x00, 0x80}, false, []byte{0xff, 0x00, 0x80}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeSyncProducer{}
			p := newTestProducer(fake)

			if err := p.PublishMessage(Message{Topic: "topic", Key: tt.key, Value: []byte("v")}); err != nil {
				t.Fatalf("PublishMessage() error = %v", err)
			}

			sent := fake.messages[0].Key
			if tt.wantNil {
				if sent != nil {
					t.Fatalf("sent key = %v, want no key at all", sent)
				}

				return
			}

			if sent == nil {
				t.Fatal("sent key is nil, want a key")
			}
			encoded, err := sent.Encode()
			if err != nil {
				t.Fatalf("Key.Encode() error = %v", err)
			}
			if encoded == nil || !bytes.Equal(encoded, tt.want) {
				t.Fatalf("encoded key = %#v, want %#v (non-nil)", encoded, tt.want)
			}
		})
	}
}

func TestPublishMessageWithMetadataReturnsTheKey(t *testing.T) {
	p := newTestProducer(&fakeSyncProducer{})

	meta, err := p.PublishMessageWithMetadata(Message{Topic: "topic", Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("PublishMessageWithMetadata() error = %v", err)
	}
	if string(meta.Key) != "k" {
		t.Fatalf("Metadata.Key = %q, want k", meta.Key)
	}
}
