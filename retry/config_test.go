package retry

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

// limitMs is maxIntervalMs as an int. It goes through a variable so the
// conversion is not a compile-time constant (which would not build on 32-bit).
var limitMs = func() int {
	limit := int64(maxIntervalMs)

	return int(limit)
}()

func TestDefaultTagsMatchDefaultConfig(t *testing.T) {
	t.Parallel()

	v := reflect.ValueOf(DefaultConfig()).Elem()

	for i := range v.NumField() {
		field := v.Type().Field(i)

		tag, ok := field.Tag.Lookup("default")
		if !ok {
			t.Errorf("Config.%s has no default tag", field.Name)

			continue
		}

		if got := fmt.Sprint(v.Field(i).Interface()); got != tag {
			t.Errorf("Config.%s: default tag %q, DefaultConfig value %q", field.Name, tag, got)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "defaults", mutate: func(*Config) {}},
		{name: "single attempt", mutate: func(c *Config) { c.MaxAttempts = 1 }},
		{name: "no wait and no cap", mutate: func(c *Config) { c.InitialIntervalMs, c.MaxIntervalMs = 0, 0 }},
		{name: "constant interval", mutate: func(c *Config) { c.Multiplier = 1 }},
		{name: "max equals initial", mutate: func(c *Config) { c.MaxIntervalMs = c.InitialIntervalMs }},
		{name: "largest interval", mutate: func(c *Config) { c.InitialIntervalMs, c.MaxIntervalMs = 0, limitMs }},
		{name: "zero attempts", mutate: func(c *Config) { c.MaxAttempts = 0 }, wantErr: "retry.max_attempts"},
		{name: "negative attempts", mutate: func(c *Config) { c.MaxAttempts = -1 }, wantErr: "retry.max_attempts"},
		{name: "negative initial", mutate: func(c *Config) { c.InitialIntervalMs = -1 }, wantErr: "retry.initial_interval_ms"},
		{name: "initial overflows duration", mutate: func(c *Config) { c.InitialIntervalMs = limitMs + 1 }, wantErr: "retry.initial_interval_ms"},
		{name: "negative max", mutate: func(c *Config) { c.MaxIntervalMs = -1 }, wantErr: "retry.max_interval_ms"},
		{name: "max overflows duration", mutate: func(c *Config) { c.MaxIntervalMs = limitMs + 1 }, wantErr: "retry.max_interval_ms"},
		{name: "max below initial", mutate: func(c *Config) { c.InitialIntervalMs, c.MaxIntervalMs = 500, 100 }, wantErr: "must not be below"},
		{name: "multiplier below one", mutate: func(c *Config) { c.Multiplier = 0.5 }, wantErr: "retry.multiplier"},
		{name: "multiplier zero", mutate: func(c *Config) { c.Multiplier = 0 }, wantErr: "retry.multiplier"},
		{name: "multiplier negative", mutate: func(c *Config) { c.Multiplier = -2 }, wantErr: "retry.multiplier"},
		{name: "multiplier NaN", mutate: func(c *Config) { c.Multiplier = math.NaN() }, wantErr: "retry.multiplier"},
		{name: "multiplier infinite", mutate: func(c *Config) { c.Multiplier = math.Inf(1) }, wantErr: "retry.multiplier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}

				return
			}

			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestConfigValidateReportsEveryProblem(t *testing.T) {
	t.Parallel()

	err := (&Config{MaxAttempts: 0, InitialIntervalMs: -1, Multiplier: 0, MaxIntervalMs: -1}).Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error")
	}

	for _, key := range []string{"max_attempts", "initial_interval_ms", "multiplier", "max_interval_ms"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("Validate() = %q, missing %q", err, key)
		}
	}
}

func TestNilConfigValidate(t *testing.T) {
	t.Parallel()

	var cfg *Config
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() on nil config = nil, want error")
	}
}

func TestNextInterval(t *testing.T) {
	t.Parallel()

	maxDuration := time.Duration(math.MaxInt64)

	tests := []struct {
		name string
		cfg  Config
		cur  time.Duration
		want time.Duration
	}{
		{name: "doubles", cfg: Config{Multiplier: 2}, cur: 100 * time.Millisecond, want: 200 * time.Millisecond},
		{name: "fractional", cfg: Config{Multiplier: 1.5}, cur: 100 * time.Millisecond, want: 150 * time.Millisecond},
		{name: "constant", cfg: Config{Multiplier: 1}, cur: time.Second, want: time.Second},
		{name: "zero stays zero", cfg: Config{Multiplier: 2}, cur: 0, want: 0},
		{name: "capped", cfg: Config{Multiplier: 2, MaxIntervalMs: 250}, cur: 200 * time.Millisecond, want: 250 * time.Millisecond},
		{name: "exactly at cap", cfg: Config{Multiplier: 2, MaxIntervalMs: 200}, cur: 100 * time.Millisecond, want: 200 * time.Millisecond},
		{name: "saturates without cap", cfg: Config{Multiplier: 2}, cur: maxDuration / 2, want: maxDuration},
		{name: "huge multiplier saturates", cfg: Config{Multiplier: 1e300}, cur: time.Hour, want: maxDuration},
		{name: "already saturated", cfg: Config{Multiplier: 10}, cur: maxDuration, want: maxDuration},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.cfg.nextInterval(tt.cur); got != tt.want {
				t.Fatalf("nextInterval(%v) = %v, want %v", tt.cur, got, tt.want)
			}
		})
	}
}
