package kafka

import "testing"

// The low-level constructors are public: a nil config must be reported as an
// error, not crash the caller.
func TestConstructorsReturnErrorForNilConfig(t *testing.T) {
	tests := map[string]func() error{
		"NewProducer":           func() error { _, err := NewProducer(nil); return err },
		"NewConsumer":           func() error { _, err := NewConsumer(nil); return err },
		"NewConcurrentConsumer": func() error { _, err := NewConcurrentConsumer(nil); return err },
	}

	for name, construct := range tests {
		t.Run(name, func(t *testing.T) {
			if err := construct(); err == nil {
				t.Fatal("expected an error for a nil config")
			}
		})
	}
}
