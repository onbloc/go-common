package retry

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Documented defaults. The struct `default` tags below must repeat these as
// literals (tags cannot reference constants); TestDefaultTagsMatchDefaultConfig
// keeps the two in sync. They are applied by DefaultConfig only: unlike other
// modules, a zero value here is meaningful (no wait, no cap) and is never
// replaced by a default.
const (
	defaultMaxAttempts       = 3
	defaultInitialIntervalMs = 100
	defaultMultiplier        = 2.0
	defaultMaxIntervalMs     = 30000
)

// maxIntervalMs is the largest millisecond count that still fits a
// time.Duration (int64 nanoseconds); larger values would overflow.
const maxIntervalMs = math.MaxInt64 / int64(time.Millisecond)

// Config is the application-owned retry configuration. It is a plain struct
// with no dependency on a config loader.
//
// The wait before the first retry is InitialIntervalMs; each following wait is
// the previous one multiplied by Multiplier, capped at MaxIntervalMs.
type Config struct {
	// MaxAttempts is the total number of calls to fn, including the first. At
	// least 1; 1 means no retry.
	MaxAttempts int `mapstructure:"max_attempts" default:"3"`
	// InitialIntervalMs is the wait before the first retry. 0 retries without
	// waiting.
	InitialIntervalMs int `mapstructure:"initial_interval_ms" default:"100"`
	// Multiplier grows the wait after each retry. Finite and at least 1; 1
	// keeps the wait constant.
	Multiplier float64 `mapstructure:"multiplier" default:"2"`
	// MaxIntervalMs caps a single wait. 0 means no cap beyond what a
	// time.Duration can hold. When set it must not be below InitialIntervalMs.
	MaxIntervalMs int `mapstructure:"max_interval_ms" default:"30000"`
}

// DefaultConfig returns a Config with every documented default set. It is the
// in-code equivalent of what a loader produces from the `default` tags.
func DefaultConfig() *Config {
	return &Config{
		MaxAttempts:       defaultMaxAttempts,
		InitialIntervalMs: defaultInitialIntervalMs,
		Multiplier:        defaultMultiplier,
		MaxIntervalMs:     defaultMaxIntervalMs,
	}
}

// Validate reports every problem with the values in c, joined into one error.
// Do validates its config too.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("retry config is required")
	}

	var errs []error

	if c.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("retry.max_attempts must be at least 1, got %d", c.MaxAttempts))
	}

	errs = appendIntervalErr(errs, "retry.initial_interval_ms", c.InitialIntervalMs)
	errs = appendIntervalErr(errs, "retry.max_interval_ms", c.MaxIntervalMs)

	// NaN fails the >= comparison, so it is rejected here too.
	if !(c.Multiplier >= 1) || math.IsInf(c.Multiplier, 0) {
		errs = append(errs, fmt.Errorf("retry.multiplier must be a finite number of at least 1, got %v", c.Multiplier))
	}

	if c.MaxIntervalMs > 0 && c.MaxIntervalMs < c.InitialIntervalMs {
		errs = append(errs, fmt.Errorf("retry.max_interval_ms (%d) must not be below retry.initial_interval_ms (%d)",
			c.MaxIntervalMs, c.InitialIntervalMs))
	}

	return errors.Join(errs...)
}

func appendIntervalErr(errs []error, name string, ms int) []error {
	if ms < 0 || int64(ms) > maxIntervalMs {
		return append(errs, fmt.Errorf("%s must be between 0 and %d, got %d", name, maxIntervalMs, ms))
	}

	return errs
}

// nextInterval returns the wait that follows cur. The product is computed in
// float64 and saturated before converting back, so it can neither overflow
// time.Duration nor wrap negative.
func (c *Config) nextInterval(cur time.Duration) time.Duration {
	limit := time.Duration(math.MaxInt64)
	if c.MaxIntervalMs > 0 {
		limit = time.Duration(c.MaxIntervalMs) * time.Millisecond
	}

	next := float64(cur) * c.Multiplier
	if next >= float64(limit) {
		return limit
	}

	return time.Duration(next)
}
