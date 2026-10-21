package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrMaxAttempts is returned, wrapped around the last error from fn, when every
// attempt failed.
var ErrMaxAttempts = errors.New("retry: max attempts reached")

// Do calls fn until it succeeds or cfg.MaxAttempts calls have failed, waiting
// between attempts as described by Config.
//
// On success it returns fn's result. When all attempts fail it returns the zero
// value and an error that matches both ErrMaxAttempts and the last error from
// fn under errors.Is and errors.As. When ctx ends before or between attempts
// it stops at once and returns the zero value and an error that matches
// ctx.Err() and, if fn had failed, its last error. A running fn is not
// interrupted; pass ctx into it if it should be.
func Do[T any](ctx context.Context, fn func() (T, error), cfg *Config, opts ...Option) (T, error) {
	var zero T

	if fn == nil {
		return zero, errors.New("retry: fn is required")
	}

	if err := cfg.Validate(); err != nil {
		return zero, err
	}

	s := defaultSettings()
	for _, opt := range opts {
		opt(s)
	}

	interval := time.Duration(cfg.InitialIntervalMs) * time.Millisecond

	var lastErr error

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, canceled(err, lastErr, attempt-1)
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		if attempt >= cfg.MaxAttempts {
			return zero, fmt.Errorf("%w (%d): %w", ErrMaxAttempts, attempt, err)
		}

		if s.onRetry != nil {
			s.onRetry(attempt, err)
		}

		if err := s.sleep(ctx, interval); err != nil {
			return zero, canceled(err, lastErr, attempt)
		}

		interval = cfg.nextInterval(interval)
	}
}

// canceled wraps the context error, and the last attempt error if there was one.
func canceled(ctxErr, lastErr error, attempts int) error {
	if lastErr == nil {
		return fmt.Errorf("retry: %w", ctxErr)
	}

	return fmt.Errorf("retry: %w after %d attempts: %w", ctxErr, attempts, lastErr)
}
