package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	// ErrAlreadyStarted is returned by Start when it has been called before. A
	// Server serves once; build a new one to serve again.
	ErrAlreadyStarted = errors.New("rpc server already started")

	// ErrShutdownTimeout is returned by Start when in-flight calls did not
	// finish within ServerConfig.ShutdownTimeoutMs and were forced closed.
	ErrShutdownTimeout = errors.New("rpc server graceful shutdown timed out")
)

// GRPCServer is the server surface callers depend on; *Server implements it.
type GRPCServer interface {
	// Start serves until ctx ends or serving fails; see Server.Start.
	Start(ctx context.Context) error
	// Stop closes the server immediately.
	Stop()
	// GetServiceRegistrar returns the registrar for generated Register*Server
	// functions. Register every service before Start.
	GetServiceRegistrar() grpc.ServiceRegistrar
}

var _ GRPCServer = (*Server)(nil)

// Server is a gRPC server with context-driven graceful shutdown.
type Server struct {
	server   *grpc.Server
	conf     *ServerConfig
	logger   Logger
	grpcOpts []grpc.ServerOption

	started atomic.Bool
	stopped atomic.Bool
	ready   chan struct{} // closed once the listener is bound
	addr    net.Addr      // written before ready is closed
}

// NewServer validates conf and builds a Server. Options apply before the
// underlying gRPC server is built.
func NewServer(conf *ServerConfig, opts ...Options[*Server]) (*Server, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	s := &Server{
		conf:   conf,
		logger: defaultLogger(),
		ready:  make(chan struct{}),
	}

	applyOptions(s, opts)

	var serverOpts []grpc.ServerOption

	if conf.TLS.Enabled {
		creds, err := conf.TLS.serverCredentials()
		if err != nil {
			return nil, err
		}

		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	s.server = grpc.NewServer(append(serverOpts, s.grpcOpts...)...)

	if !conf.DisableReflection {
		reflection.Register(s.server)
	}

	return s, nil
}

// GetServiceRegistrar returns the registrar for generated Register*Server
// functions. Register every service before Start.
func (s *Server) GetServiceRegistrar() grpc.ServiceRegistrar {
	return s.server
}

// Ready is closed once Start has bound its listener, after which Addr is set.
// It is not closed when Start fails before that (for example, the port is in
// use): a caller that waits on Ready must also watch Start's result, or it
// blocks forever.
func (s *Server) Ready() <-chan struct{} {
	return s.ready
}

// Addr returns the bound listen address, or nil before Ready is closed. It
// tells the real port when the configured port is "0".
func (s *Server) Addr() net.Addr {
	select {
	case <-s.ready:
		return s.addr
	default:
		return nil
	}
}

// Start listens and serves until ctx ends, Stop is called, or serving fails.
//
// When ctx ends the server stops accepting new calls and waits for in-flight
// ones up to ServerConfig.ShutdownTimeout, then closes them and returns
// ErrShutdownTimeout. A shutdown triggered by ctx or Stop within the bound
// returns nil. A failure to listen or serve is returned wrapped.
func (s *Server) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	if s.stopped.Load() {
		return fmt.Errorf("rpc server failed to serve: %w", grpc.ErrServerStopped)
	}

	address := s.conf.Address()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("rpc server failed to listen on %s: %w", address, err)
	}

	s.addr = lis.Addr()
	close(s.ready)

	s.logger.Infof("gRPC server listening on %s", s.addr)

	serveErr := make(chan error, 1)

	go func() { serveErr <- s.server.Serve(lis) }()

	select {
	case err := <-serveErr:
		// Serve reports ErrServerStopped when Stop wins the race against it. Stop
		// was not called before Start (checked above), so this is a clean stop.
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.server.Stop()

			return fmt.Errorf("rpc server failed to serve: %w", err)
		}

		return nil
	case <-ctx.Done():
		return s.shutdown(serveErr)
	}
}

// Stop closes the server and its connections immediately. A running Start
// returns nil; a Start after Stop fails.
func (s *Server) Stop() {
	s.stopped.Store(true)
	s.server.Stop()
}

// shutdown drains in-flight calls, forcing them closed after the timeout.
func (s *Server) shutdown(serveErr <-chan error) error {
	s.logger.Infof("gRPC server shutting down")

	stopped := make(chan struct{})

	go func() {
		s.server.GracefulStop()
		close(stopped)
	}()

	timer := time.NewTimer(s.conf.ShutdownTimeout())
	defer timer.Stop()

	var result error

	select {
	case <-stopped:
	case <-timer.C:
		s.logger.Errorf("gRPC server graceful shutdown exceeded %s, forcing stop", s.conf.ShutdownTimeout())
		s.server.Stop()
		<-stopped

		result = ErrShutdownTimeout
	}

	<-serveErr

	return result
}
