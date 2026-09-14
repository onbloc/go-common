package kafka

// Logger is the minimal logging surface this package needs. Any application
// logger with these methods (e.g. zap's SugaredLogger, logrus) satisfies it
// without an adapter.
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
