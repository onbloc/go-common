// Package retry calls a function again after a failure, with exponential
// backoff, until it succeeds, runs out of attempts, or the context ends.
//
// Configuration is a plain struct; this package does not depend on any config
// loader. Decode it with any mapstructure-aware loader (such as
// github.com/onbloc/go-common/config, which applies the `default` tags), or
// build it in code starting from DefaultConfig:
//
//	cfg := retry.DefaultConfig()
//	cfg.MaxAttempts = 5
//
//	conn, err := retry.Do(ctx, func() (*Conn, error) {
//		return dial(addr)
//	}, cfg, retry.WithOnRetry(func(attempt int, err error) {
//		log.Printf("dial attempt %d failed: %v", attempt, err)
//	}))
//	if errors.Is(err, retry.ErrMaxAttempts) {
//		// every attempt failed; errors.Is also matches the last dial error
//	}
//
// The first wait is InitialIntervalMs, each following wait is the previous one
// times Multiplier, capped at MaxIntervalMs. Waiting stops as soon as ctx ends.
// All errors are retried; Do cannot tell a permanent failure from a transient
// one, so fn should not be used for operations that are unsafe to repeat.
package retry
