package kafka

import "testing"

// A caller may guard with `if p != nil`, which a nil *Producer wrapped in the
// Publisher interface would pass. The factories must return a real nil on error.
func TestFactoriesReturnNilInterfacesOnError(t *testing.T) {
	conf := DefaultConfig()
	conf.Brokers = []string{"127.0.0.1:1"} // nothing listens here: building the client fails
	conf.Consumer.Group = "group"
	conf.Consumer.Topics = []string{"topic"}

	publisher, err := NewPublisher(conf, nil)
	if err == nil {
		t.Fatal("NewPublisher() expected an error for unreachable brokers")
	}
	if publisher != nil {
		t.Fatalf("NewPublisher() returned %T on error, want a nil Publisher", publisher)
	}

	subscriber, err := NewSubscriber(conf, nil)
	if err == nil {
		t.Fatal("NewSubscriber() expected an error for unreachable brokers")
	}
	if subscriber != nil {
		t.Fatalf("NewSubscriber() returned %T on error, want a nil Subscriber", subscriber)
	}

	conf.Consumer.Concurrency = 3
	subscriber, err = NewSubscriber(conf, nil)
	if err == nil {
		t.Fatal("NewSubscriber() with concurrency expected an error for unreachable brokers")
	}
	if subscriber != nil {
		t.Fatalf("NewSubscriber() with concurrency returned %T on error, want a nil Subscriber", subscriber)
	}
}
