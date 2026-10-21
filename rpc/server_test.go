package rpc

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func clientFor(t *testing.T, s *Server) *Client {
	t.Helper()

	return newClient(t, &ClientConfig{InstanceID: "test", Address: s.Addr().String()})
}

func TestServerServesCalls(t *testing.T) {
	rs := startServer(t, nil)

	if err := check(t, clientFor(t, rs.Server).GetConnection()); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

func TestServerAddrIsNilBeforeReady(t *testing.T) {
	server, err := NewServer(DefaultServerConfig())
	if err != nil {
		t.Fatal(err)
	}

	if addr := server.Addr(); addr != nil {
		t.Fatalf("Addr() = %v before Start, want nil", addr)
	}
}

func TestServerStopsGracefullyWhenContextEnds(t *testing.T) {
	rs := startServer(t, nil)
	conn := clientFor(t, rs.Server).GetConnection()

	if err := check(t, conn); err != nil {
		t.Fatalf("Check: %v", err)
	}

	rs.cancel()

	if err := rs.wait(t); err != nil {
		t.Fatalf("Start() = %v, want nil on graceful shutdown", err)
	}

	if err := check(t, conn); status.Code(err) != codes.Unavailable {
		t.Fatalf("Check after shutdown = %v, want Unavailable", err)
	}
}

// An in-flight stream outlives the shutdown timeout: Start must force it closed
// and say so, instead of waiting forever (GracefulStop alone never returns).
func TestServerForcesStopAfterShutdownTimeout(t *testing.T) {
	rs := startServer(t, &ServerConfig{ShutdownTimeoutMs: 100})
	conn := clientFor(t, rs.Server).GetConnection()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if _, err := stream.Recv(); err != nil { // stream is established and in flight
		t.Fatalf("first Recv: %v", err)
	}

	begin := time.Now()

	rs.cancel()

	if err := rs.wait(t); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("Start() = %v, want ErrShutdownTimeout", err)
	}

	if elapsed := time.Since(begin); elapsed < 100*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("shutdown took %s, want roughly the 100ms timeout", elapsed)
	}

	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv after forced stop = %v, want Unavailable", err)
	}
}

func TestServerGracefulShutdownWaitsForInFlightCalls(t *testing.T) {
	rs := startServer(t, &ServerConfig{ShutdownTimeoutMs: 5000})
	conn := clientFor(t, rs.Server).GetConnection()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}

	rs.cancel()

	select {
	case err := <-rs.done:
		t.Fatalf("Start returned %v while a call was still in flight", err)
	case <-time.After(200 * time.Millisecond):
	}

	cancel() // the in-flight call ends, so the drain can finish

	if err := rs.wait(t); err != nil {
		t.Fatalf("Start() = %v, want nil once in-flight calls finished", err)
	}
}

func TestServerStopEndsStart(t *testing.T) {
	rs := startServer(t, nil)

	rs.Stop()

	if err := rs.wait(t); err != nil {
		t.Fatalf("Start() = %v after Stop, want nil", err)
	}
}

func TestServerStartTwice(t *testing.T) {
	rs := startServer(t, nil)

	if err := rs.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start() = %v, want ErrAlreadyStarted", err)
	}
}

func TestServerStartAfterStop(t *testing.T) {
	server, err := NewServer(&ServerConfig{Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}

	server.Stop()

	if err := server.Start(context.Background()); !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatalf("Start() = %v after Stop, want grpc.ErrServerStopped", err)
	}
}

func TestServerListenFailure(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	port := busy.Addr().(*net.TCPAddr).Port

	server, err := NewServer(&ServerConfig{Host: "127.0.0.1", Port: strconv.Itoa(port)})
	if err != nil {
		t.Fatal(err)
	}

	err = server.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed to listen") {
		t.Fatalf("Start() = %v, want a listen failure", err)
	}
}

func TestServerReflection(t *testing.T) {
	const reflectionService = "grpc.reflection.v1.ServerReflection"

	tests := []struct {
		name    string
		disable bool
	}{
		{name: "enabled by default"},
		{name: "disabled", disable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := NewServer(&ServerConfig{DisableReflection: tt.disable})
			if err != nil {
				t.Fatal(err)
			}

			_, registered := server.server.GetServiceInfo()[reflectionService]
			if registered == tt.disable {
				t.Fatalf("reflection registered = %v, DisableReflection = %v", registered, tt.disable)
			}
		})
	}
}

func TestServerAppliesGRPCServerOptions(t *testing.T) {
	var calls atomic.Int32

	interceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		calls.Add(1)

		return handler(ctx, req)
	}

	rs := startServer(t, nil, WithGRPCServerOptions(grpc.UnaryInterceptor(interceptor)))

	if err := check(t, clientFor(t, rs.Server).GetConnection()); err != nil {
		t.Fatalf("Check: %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("interceptor ran %d times, want 1", got)
	}
}

func TestServerLogsLifecycle(t *testing.T) {
	log := &recordingLogger{}
	rs := startServer(t, nil, WithServerLogger(log))

	rs.cancel()

	if err := rs.wait(t); err != nil {
		t.Fatalf("Start() = %v", err)
	}

	if got := log.joined(); !strings.Contains(got, "listening on 127.0.0.1:") || !strings.Contains(got, "shutting down") {
		t.Fatalf("logged %q, want listening and shutting down lines", got)
	}
}

func TestNilLoggerKeepsDefault(t *testing.T) {
	rs := startServer(t, nil, WithServerLogger(nil))
	client := newClient(t, &ClientConfig{Address: rs.Addr().String()}, WithClientLogger(nil))

	if err := check(t, client.GetConnection()); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

func TestNewServerRejectsInvalidConfig(t *testing.T) {
	if _, err := NewServer(&ServerConfig{Port: "nope"}); err == nil {
		t.Fatal("NewServer accepted an invalid port")
	}

	if _, err := NewServer(nil); err == nil {
		t.Fatal("NewServer accepted a nil config")
	}
}
