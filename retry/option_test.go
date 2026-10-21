package retry_test

import (
	"context"
	"errors"
	"testing"

	"github.com/onbloc/go-common/retry"
)

// A caller outside the package must be able to name the option type, e.g. to
// forward options through its own wrapper around Do.
func doWithDefaults[T any](fn func() (T, error), opts ...retry.Option) (T, error) {
	cfg := retry.DefaultConfig()
	cfg.MaxAttempts = 2
	cfg.InitialIntervalMs = 0

	return retry.Do(context.Background(), fn, cfg, opts...)
}

func TestOptionsCanBeForwardedByCallers(t *testing.T) {
	t.Parallel()

	var attempts []int

	_, err := doWithDefaults(func() (int, error) { return 0, errors.New("boom") },
		retry.WithOnRetry(func(attempt int, _ error) { attempts = append(attempts, attempt) }))
	if !errors.Is(err, retry.ErrMaxAttempts) {
		t.Fatalf("err = %v, want ErrMaxAttempts", err)
	}

	if len(attempts) != 1 || attempts[0] != 1 {
		t.Fatalf("onRetry attempts = %v, want [1]", attempts)
	}
}
