package kafka

// Logger is the minimal logging surface this package needs. Any application
// logger with these methods (e.g. zap's SugaredLogger, logrus) satisfies it
// without an adapter. Implement DebugLogger as well to receive debug output.
type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

func defaultLogger() Logger {
	return noopLogger{}
}

// DebugLogger is an optional extension of Logger. When the logger passed to
// the package also has Debugf, per-message and per-batch activity (publish,
// receive, processed) is logged through it; otherwise that output is dropped.
// It is kept out of Logger so loggers with only Infof and Errorf still work.
type DebugLogger interface {
	Logger
	Debugf(format string, args ...any)
}

func debugEnabled(l Logger) bool {
	_, ok := l.(DebugLogger)

	return ok
}

func debugf(l Logger, format string, args ...any) {
	if d, ok := l.(DebugLogger); ok {
		d.Debugf(format, args...)
	}
}
