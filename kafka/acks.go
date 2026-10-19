package kafka

import "github.com/IBM/sarama"

// Acks is how many broker acknowledgements a producer waits for before
// a publish succeeds: Kafka's producer "acks" setting. Use the Kafka values
// "0", "1" and "all" (or "-1", which Kafka treats as "all"); the names "none"
// and "local" are accepted as aliases of "0" and "1". An empty value means the
// default, "all".
//
// A plain string works anywhere a Acks is expected, including YAML
// and environment variables, where an unquoted 1 is read as "1".
type Acks string

const (
	// AcksNone ("0") does not wait for any acknowledgement. Messages can be
	// lost without the producer noticing.
	AcksNone Acks = "0"
	// AcksLeader ("1") waits for the partition leader to write the message. It
	// is lost if the leader fails before the followers copy it.
	AcksLeader Acks = "1"
	// AcksAll ("all") waits for every in-sync replica. It is the default, and
	// idempotent producers always use it.
	AcksAll Acks = "all"
)

// acksSpellings lists every accepted spelling in documentation order. It is the
// single source for parsing and validation.
var acksSpellings = []struct {
	spelling Acks
	value    sarama.RequiredAcks
}{
	{AcksNone, sarama.NoResponse},
	{AcksLeader, sarama.WaitForLocal},
	{AcksAll, sarama.WaitForAll},
	{"-1", sarama.WaitForAll},
	{"none", sarama.NoResponse},
	{"local", sarama.WaitForLocal},
}

// acksNames returns the accepted spellings for error messages.
func acksNames() []string {
	names := make([]string, len(acksSpellings))
	for i, entry := range acksSpellings {
		names[i] = string(entry.spelling)
	}

	return names
}
