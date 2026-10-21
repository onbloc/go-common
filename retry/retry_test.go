package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// recordSleeps replaces the wait with a recorder so delays are asserted
// without real sleeping.
func recordSleeps(waits *[]time.Duration) Option {
	return func(s *settings) {
		s.sleep = func(ctx context.Context, d time.Duration) error {
			*waits = append(*waits, d)

			return ctx.Err()
		}
	}
}

func failTimes(n int, calls *int) func() (string, error) {
	return func() (string, error) {
		*calls++
		if *calls <= n {
			return "", errBoom
		}

		return "ok", nil
	}
}

func TestDoSucceedsFirstAttempt(t *testing.T) {
	t.Parallel()

	var (
		calls int
		waits []time.Duration
	)

	got, err := Do(context.Background(), failTimes(0, &calls), DefaultConfig(), recordSleeps(&waits))
	if err != nil || got != "ok" {
		t.Fatalf("Do() = %q, %v; want ok, nil", got, err)
	}

	if calls != 1 || len(waits) != 0 {
		t.Fatalf("calls = %d, waits = %v; want 1 call, no wait", calls, waits)
	}
}

func TestDoRetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	var (
		calls int
		waits []time.Duration
	)

	cfg := DefaultConfig()
	cfg.MaxAttempts = 5

	got, err := Do(context.Background(), failTimes(3, &calls), cfg, recordSleeps(&waits))
	if err != nil || got != "ok" {
		t.Fatalf("Do() = %q, %v; want ok, nil", got, err)
	}

	if calls != 4 {
		t.Fatalf("calls = %d, want 4", calls)
	}

	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
}

func TestDoExhaustsAttempts(t *testing.T) {
	t.Parallel()

	var (
		calls int
		waits []time.Duration
	)

	cfg := DefaultConfig()
	cfg.MaxAttempts = 4
	cfg.InitialIntervalMs = 100
	cfg.MaxIntervalMs = 250

	got, err := Do(context.Background(), failTimes(100, &calls), cfg, recordSleeps(&waits))
	if got != "" {
		t.Fatalf("result = %q, want zero value", got)
	}

	if !errors.Is(err, ErrMaxAttempts) || !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want ErrMaxAttempts and the last error", err)
	}

	if calls != 4 {
		t.Fatalf("calls = %d, want 4", calls)
	}

	// No wait after the last attempt; waits are capped at max_interval_ms.
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond}
	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
}

func TestDoSingleAttemptDoesNotRetry(t *testing.T) {
	t.Parallel()

	var (
		calls int
		waits []time.Duration
	)

	cfg := DefaultConfig()
	cfg.MaxAttempts = 1

	_, err := Do(context.Background(), failTimes(100, &calls), cfg, recordSleeps(&waits))
	if !errors.Is(err, ErrMaxAttempts) {
		t.Fatalf("err = %v, want ErrMaxAttempts", err)
	}

	if calls != 1 || len(waits) != 0 {
		t.Fatalf("calls = %d, waits = %v; want 1 call, no wait", calls, waits)
	}
}

func TestDoKeepsTypedErrorReachable(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MaxAttempts = 2

	var waits []time.Duration

	_, err := Do(context.Background(), func() (int, error) {
		return 0, &time.ParseError{Value: "x"}
	}, cfg, recordSleeps(&waits))

	var parseErr *time.ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("err = %v, want errors.As to reach the last error", err)
	}
}

func TestDoOnRetry(t *testing.T) {
	t.Parallel()

	var (
		calls int
		waits []time.Duration
		seen  []int
	)

	cfg := DefaultConfig()
	cfg.MaxAttempts = 3

	_, err := Do(context.Background(), failTimes(100, &calls), cfg, recordSleeps(&waits),
		WithOnRetry(func(attempt int, err error) {
			if !errors.Is(err, errBoom) {
				t.Errorf("onRetry err = %v, want errBoom", err)
			}

			seen = append(seen, attempt)
		}))
	if err == nil {
		t.Fatal("Do() = nil, want error")
	}

	// Called after attempts 1 and 2, not after the final failure.
	if fmt.Sprint(seen) != "[1 2]" {
		t.Fatalf("onRetry attempts = %v, want [1 2]", seen)
	}
}

func TestDoNilOptionsAreIgnored(t *testing.T) {
	t.Parallel()

	var (
		calls int
		waits []time.Duration
	)

	got, err := Do(context.Background(), failTimes(1, &calls), DefaultConfig(), recordSleeps(&waits), WithOnRetry(nil))
	if err != nil || got != "ok" {
		t.Fatalf("Do() = %q, %v; want ok, nil", got, err)
	}
}

func TestDoRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()

		calls := 0
		cfg := DefaultConfig()
		cfg.MaxAttempts = 0

		got, err := Do(context.Background(), func() (int, error) { calls++; return 1, nil }, cfg)
		if err == nil || got != 0 {
			t.Fatalf("Do() = %d, %v; want zero value and error", got, err)
		}

		if calls != 0 {
			t.Fatalf("fn called %d times with an invalid config, want 0", calls)
		}
	})

	t.Run("nil config", func(t *testing.T) {
		t.Parallel()

		calls := 0

		if _, err := Do(context.Background(), func() (int, error) { calls++; return 1, nil }, nil); err == nil {
			t.Fatal("Do() = nil, want error")
		}

		if calls != 0 {
			t.Fatalf("fn called %d times with a nil config, want 0", calls)
		}
	})

	t.Run("nil fn", func(t *testing.T) {
		t.Parallel()

		if _, err := Do[int](context.Background(), nil, DefaultConfig()); err == nil {
			t.Fatal("Do() = nil, want error")
		}
	})
}

func TestDoContextAlreadyDoneSkipsFn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false

	_, err := Do(ctx, func() (int, error) {
		called = true

		return 1, nil
	}, DefaultConfig())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	if called {
		t.Fatal("fn was called with an already-canceled context")
	}
}

func TestDoContextCanceledDuringWait(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	cfg := DefaultConfig()
	cfg.MaxAttempts = 10

	_, err := Do(ctx, func() (int, error) {
		calls++
		cancel() // canceled while the next wait is pending

		return 0, errBoom
	}, cfg)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want context.Canceled and the last error", err)
	}

	if errors.Is(err, ErrMaxAttempts) {
		t.Fatalf("err = %v, must not report exhausted attempts", err)
	}

	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

// The real wait must end when ctx does, not after the full interval.
func TestSleepReturnsOnContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()

	err := sleep(ctx, time.Minute)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sleep() = %v, want DeadlineExceeded", err)
	}

	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("sleep() took %v, want it to end with ctx", elapsed)
	}
}

func TestSleepWaitsFullDuration(t *testing.T) {
	t.Parallel()

	start := time.Now()

	if err := sleep(context.Background(), 30*time.Millisecond); err != nil {
		t.Fatalf("sleep() = %v, want nil", err)
	}

	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("sleep() returned after %v, want at least 30ms", elapsed)
	}
}
