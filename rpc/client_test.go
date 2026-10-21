package rpc

import (
	"context"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestClientDoesNotConnectOnCreate(t *testing.T) {
	client := newClient(t, &ClientConfig{InstanceID: "worker-1", Address: "127.0.0.1:1"})

	if got := client.GetInstanceID(); got != "worker-1" {
		t.Fatalf("GetInstanceID() = %q, want worker-1", got)
	}

	// Nothing listens there: the failure shows up on the first call, not before.
	if err := check(t, client.GetConnection()); status.Code(err) != codes.Unavailable {
		t.Fatalf("Check against a closed port = %v, want Unavailable", err)
	}
}

func TestClientGenerateRequestIDIsSequential(t *testing.T) {
	client := newClient(t, &ClientConfig{Address: "127.0.0.1:1"})

	for want := uint64(1); want <= 3; want++ {
		if got := client.GenerateRequestID(); got != want {
			t.Fatalf("GenerateRequestID() = %d, want %d", got, want)
		}
	}
}

func TestClientGenerateRequestIDIsUniqueUnderConcurrency(t *testing.T) {
	const (
		workers = 32
		perEach = 200
	)

	client := newClient(t, &ClientConfig{Address: "127.0.0.1:1"})

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ids = make([]uint64, 0, workers*perEach)
	)

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			local := make([]uint64, perEach)
			for i := range local {
				local[i] = client.GenerateRequestID()
			}

			mu.Lock()
			ids = append(ids, local...)
			mu.Unlock()
		}()
	}

	wg.Wait()

	slices.Sort(ids)

	for i, id := range ids {
		if want := uint64(i) + 1; id != want {
			t.Fatalf("id at %d = %d, want %d (duplicate or gap)", i, id, want)
		}
	}
}

func TestClientCloseTwiceReportsError(t *testing.T) {
	client := newClient(t, &ClientConfig{Address: "127.0.0.1:1"})

	if err := client.Close(); err != nil {
		t.Fatalf("first Close() = %v", err)
	}

	if err := client.Close(); err == nil {
		t.Fatal("second Close() = nil, want an error")
	}
}

func TestClientAppliesDialOptions(t *testing.T) {
	var calls atomic.Int32

	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		calls.Add(1)

		return invoker(ctx, method, req, reply, cc, opts...)
	}

	rs := startServer(t, nil)
	client := newClient(t, &ClientConfig{Address: rs.Addr().String()}, WithDialOptions(grpc.WithUnaryInterceptor(interceptor)))

	if err := check(t, client.GetConnection()); err != nil {
		t.Fatalf("Check: %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("interceptor ran %d times, want 1", got)
	}
}

func TestNewClientRejectsInvalidConfig(t *testing.T) {
	if _, err := NewClient(&ClientConfig{}); err == nil {
		t.Fatal("NewClient accepted an empty address")
	}

	if _, err := NewClient(nil); err == nil {
		t.Fatal("NewClient accepted a nil config")
	}
}

// A connection that is accepted and dropped must be retried with backoff. With a
// zero grpc Backoff the client redials in a tight loop.
func TestClientBacksOffBetweenReconnects(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	var accepted atomic.Int32

	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}

			accepted.Add(1)
			conn.Close()
		}
	}()

	client := newClient(t, &ClientConfig{Address: lis.Addr().String()})

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	// WaitForReady keeps the call pending while the channel retries connecting.
	_, err = healthpb.NewHealthClient(client.GetConnection()).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Check = %v, want DeadlineExceeded", err)
	}

	if got := accepted.Load(); got < 1 || got > 5 {
		t.Fatalf("server saw %d connection attempts in 700ms, want 1..5 with backoff", got)
	}
}
