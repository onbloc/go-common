package retry

import (
	"context"
	"time"
)

// Option configures a single Do call.
type Option func(*settings)

// settings holds the per-call state that Options modify.
type settings struct {
	onRetry func(attempt int, err error)
	sleep   func(ctx context.Context, d time.Duration) error
}

func defaultSettings() *settings {
	return &settings{sleep: sleep}
}

// WithOnRetry registers fn to be called after each failed attempt that will
// be followed by another, before the wait. attempt is the 1-based number of
// the attempt that failed. It is not called after the last attempt. A nil fn
// is ignored.
func WithOnRetry(fn func(attempt int, err error)) Option {
	return func(s *settings) {
		if fn != nil {
			s.onRetry = fn
		}
	}
}

// sleep waits for d or until ctx ends, whichever comes first.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
