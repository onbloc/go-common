package rpc

import "google.golang.org/grpc"

// Options configures a *Server or *Client during construction.
type Options[T any] func(T)

func applyOptions[T any](t T, opts []Options[T]) {
	for _, opt := range opts {
		opt(t)
	}
}

// Logger is the logging surface this package needs. Any application logger
// with these methods (e.g. zap's SugaredLogger, logrus) satisfies it without an
// adapter.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

func defaultLogger() Logger {
	return noopLogger{}
}

// WithServerLogger sets the logger used for server lifecycle activity. A nil
// logger is ignored, leaving the default no-op logger in place.
func WithServerLogger(logger Logger) Options[*Server] {
	return func(s *Server) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithClientLogger sets the logger used for client activity. A nil logger is
// ignored, leaving the default no-op logger in place.
func WithClientLogger(logger Logger) Options[*Client] {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithGRPCServerOptions appends grpc.ServerOptions (interceptors, stats
// handlers, keepalive, ...). They are applied after the package's own options
// (TLS credentials), so they can override them. Calls accumulate.
func WithGRPCServerOptions(opts ...grpc.ServerOption) Options[*Server] {
	return func(s *Server) {
		s.grpcOpts = append(s.grpcOpts, opts...)
	}
}

// WithDialOptions appends grpc.DialOptions (interceptors, stats handlers,
// credentials, ...). They are applied after the package's own options (TLS
// credentials, load balancing policy, connect params), so they can override
// them. Calls accumulate.
func WithDialOptions(opts ...grpc.DialOption) Options[*Client] {
	return func(c *Client) {
		c.dialOpts = append(c.dialOpts, opts...)
	}
}
