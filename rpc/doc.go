// Package rpc provides a small gRPC server and client wrapper: configuration
// with validation, optional TLS, graceful shutdown driven by a context, and a
// per-client request ID counter.
//
// Configuration is a plain struct; this package does not depend on any config
// loader. Decode it with any mapstructure-aware loader (such as
// github.com/onbloc/go-common/config, which applies the `default` tags), or
// build it in code starting from DefaultServerConfig/DefaultClientConfig.
//
// Server:
//
//	conf := rpc.DefaultServerConfig()
//	conf.Port = "9090"
//	server, err := rpc.NewServer(conf)
//	if err != nil {
//		return err
//	}
//	pb.RegisterFooServer(server.GetServiceRegistrar(), impl)
//
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer stop()
//	err = server.Start(ctx) // blocks until ctx ends, then shuts down gracefully
//
// Start never terminates the process and does not handle OS signals: that is
// the caller's decision. When ctx ends, the server stops accepting calls and
// waits for in-flight ones up to ServerConfig.ShutdownTimeoutMs, then forces
// them closed and returns ErrShutdownTimeout.
//
// Client:
//
//	client, err := rpc.NewClient(&rpc.ClientConfig{InstanceID: "worker-1", Address: "localhost:9090"})
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	foo := pb.NewFooClient(client.GetConnection())
//
// NewClient does not connect: gRPC dials lazily, on the first call.
//
// TLS is off by default. Anything else gRPC supports (interceptors, stats
// handlers such as OpenTelemetry, keepalive, credentials) is injected with
// WithGRPCServerOptions and WithDialOptions; they are applied after this
// package's defaults, so they can override them. This package does not depend
// on any tracing or metrics library.
//
// A Logger may be supplied through WithServerLogger/WithClientLogger; any type
// with Debugf, Infof and Errorf methods satisfies it. Without a logger, log
// output is discarded.
package rpc
