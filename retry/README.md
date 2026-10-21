# retry

`retry` calls a function again after a failure, with exponential backoff, until it succeeds, runs out of attempts, or the `context.Context` ends. It uses only the standard library.

## Installation

Requires Go 1.24 or later. Once `retry/v0.1.0` is published, install the module with:

```bash
go get github.com/onbloc/go-common/retry@v0.1.0
```

## Usage

```go
cfg := retry.DefaultConfig()
cfg.MaxAttempts = 5

conn, err := retry.Do(ctx, func() (*Conn, error) {
    return dial(addr)
}, cfg,
    retry.WithOnRetry(func(attempt int, err error) {
        metrics.Inc("dial_retry")
    }),
)
if errors.Is(err, retry.ErrMaxAttempts) {
    // every attempt failed; errors.Is/As also reach the last error from dial
}
```

`Do[T]` returns fn's result on success. On failure it returns the zero value of `T`:

| Error | When |
| --- | --- |
| wraps `ErrMaxAttempts` and fn's last error | all `max_attempts` calls failed |
| wraps `ctx.Err()` and fn's last error (if any) | `ctx` ended before an attempt or during a wait; fn is not called again |
| validation error | the config is invalid, `cfg` is nil, or `fn` is nil; fn is never called |

A running fn is not interrupted when `ctx` ends; pass `ctx` into fn if it should be. Every error is retried: there is no way to mark one as permanent, so do not wrap operations that are unsafe to repeat.

## Configuration

`Config` is a plain struct tagged with `mapstructure` and `default`, so it can be decoded by [`config`](../config) (which applies the defaults) or any mapstructure-aware loader. Without a loader, start from `DefaultConfig()`.

| Field | Key | Default | Meaning |
| --- | --- | --- | --- |
| `MaxAttempts` | `max_attempts` | `3` | Total calls to fn, including the first. At least 1; `1` means no retry |
| `InitialIntervalMs` | `initial_interval_ms` | `100` | Wait before the first retry. `0` retries immediately |
| `Multiplier` | `multiplier` | `2` | Factor applied to the wait after each retry. Finite, at least 1; `1` keeps it constant |
| `MaxIntervalMs` | `max_interval_ms` | `30000` | Cap for a single wait. `0` means no cap |

Unlike the `kafka` and `rpc` modules, a zero value here is **not** replaced by a default: `0` is meaningful for `initial_interval_ms` and `max_interval_ms`, and an unset `max_attempts` or `multiplier` is rejected by `Validate` instead of silently becoming a default. A `Config` literal must therefore set every field, or start from `DefaultConfig()`.

`Validate` reports every problem at once, and `Do` calls it. Rules:

- `max_attempts` is at least 1.
- `initial_interval_ms` and `max_interval_ms` are between 0 and the largest millisecond count a `time.Duration` can hold (about 292 years); larger values would overflow.
- `multiplier` is finite and at least 1 (NaN, infinities, and values that would shrink or negate the wait are rejected).
- `max_interval_ms`, when set, is not below `initial_interval_ms`.

The wait sequence is `initial`, `initial * multiplier`, ... capped at `max_interval_ms`. Growth saturates instead of overflowing, so a large `max_attempts` with no cap cannot produce a negative wait. There is no wait after the last attempt, and no jitter.

## Callback

`WithOnRetry(fn)` calls `fn(attempt, err)` after each failed attempt that will be followed by another, before the wait; `attempt` is the 1-based number of the attempt that failed. It is not called after the final attempt. A nil callback is ignored.
