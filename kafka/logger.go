package kafka

// Logger is the logging surface this package needs. Any application logger
// with these methods (e.g. zap's SugaredLogger, logrus) satisfies it without an
// adapter. Per-message and per-batch activity is logged through Debugf;
// failures are logged through Errorf.
type Logger interface {
	Errorf(format string, args ...any)
	Debugf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Errorf(string, ...any) {}
func (noopLogger) Debugf(string, ...any) {}

func defaultLogger() Logger {
	return noopLogger{}
}
