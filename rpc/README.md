# rpc

`rpc` wraps [grpc-go](https://github.com/grpc/grpc-go) with a small server and client: validated configuration, optional TLS and mutual TLS, graceful shutdown driven by a `context.Context`, and a per-client request ID counter. It has no dependency on a logging, tracing or config library.

## Installation

Requires Go 1.24 or later. Once `rpc/v0.1.0` is published, install the module with:

```bash
go get github.com/onbloc/go-common/rpc@v0.1.0
```

## Server

```go
conf := rpc.DefaultServerConfig()
conf.Port = "9090"

server, err := rpc.NewServer(conf, rpc.WithServerLogger(log))
if err != nil {
    return err
}

pb.RegisterFooServer(server.GetServiceRegistrar(), impl) // register before Start

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := server.Start(ctx); err != nil { // blocks until ctx ends
    return err
}
```

`Start` never calls `os.Exit` and does not handle OS signals; both are the caller's decision. It returns:

| Result | When |
| --- | --- |
| `nil` | `ctx` ended and in-flight calls finished within the timeout, or `Stop` was called while serving |
| `ErrShutdownTimeout` | in-flight calls outlived `shutdown_timeout_ms` and were forced closed |
| `ErrAlreadyStarted` | `Start` was called before; a `Server` serves once |
| wrapped error | the port could not be bound, serving failed, or `Stop` was called before `Start` |

When `ctx` ends, the server stops accepting calls and waits for in-flight ones (`GracefulStop`) for at most `shutdown_timeout_ms`, then closes them (`Stop`). Streaming calls that never end are why the bound matters: `GracefulStop` alone waits for them forever.

`server.Ready()` is closed once the listener is bound, and `server.Addr()` then returns the real address, which is how to find the port when `port` is `"0"`.

The gRPC reflection service is registered by default so tools such as `grpcurl` can list services. Set `disable_reflection: true` to turn it off where exposing the service list is unwanted.

## Client

```go
client, err := rpc.NewClient(&rpc.ClientConfig{
    InstanceID: "worker-1",
    Address:    "localhost:9090",
})
if err != nil {
    return err
}
defer client.Close()

foo := pb.NewFooClient(client.GetConnection())
id := client.GenerateRequestID() // 1, 2, 3, ...; safe for concurrent use
```

`NewClient` does not connect. gRPC dials on the first call, so an unreachable address is reported by that call, not by `NewClient`. Failed connection attempts are retried with gRPC's default exponential backoff.

`Client.Close` returns an error when the connection was already closed.

## Configuration

`ServerConfig` and `ClientConfig` are plain structs. Decode them with any mapstructure-aware loader such as [`github.com/onbloc/go-common/config`](../config), which applies the `default` tags, or build them in code from `DefaultServerConfig()` / `DefaultClientConfig()`. Unset (zero) tuning fields resolve to the defaults below. Call `Validate` to check a config; `NewServer` and `NewClient` do it for you and report every problem at once.

```yaml
# config/config.yaml
rpc_server:
  port: "9090"
  shutdown_timeout_ms: 10000
  tls:
    enabled: true
    cert_file: /etc/tls/server.pem
    key_file: /etc/tls/server.key
    ca_file: /etc/tls/ca.pem # optional: require client certificates (mutual TLS)

rpc_client:
  instance_id: worker-1
  address: backend:9090
  tls:
    enabled: true
    ca_file: /etc/tls/ca.pem # optional: system roots when unset
```

Server keys (`ServerConfig`):

| Key | Default | Meaning |
| --- | --- | --- |
| `host` | empty | interface to listen on; empty is all interfaces |
| `port` | `8080` | TCP port; `0` picks a free one |
| `shutdown_timeout_ms` | `10000` | graceful shutdown bound |
| `disable_reflection` | `false` | turn off gRPC reflection |

Client keys (`ClientConfig`):

| Key | Default | Meaning |
| --- | --- | --- |
| `address` | none (required) | `host:port` or gRPC target URI |
| `instance_id` | empty | identifies this client to the services it calls |
| `load_balancing_policy` | `round_robin` | gRPC load balancing policy name |
| `min_connect_timeout_ms` | `5000` | minimum time per connection attempt |

### TLS

TLS is off by default. TLS 1.2 is the minimum version, and there is no config option to skip certificate verification.

- **Server:** `enabled` needs `cert_file` and `key_file`. Setting `ca_file` also requires every client to present a certificate signed by that CA.
- **Client:** `ca_file` verifies the server (system roots when unset). `cert_file` and `key_file`, set together, are the client certificate for mutual TLS. `server_name` overrides the name checked against the server certificate; it defaults to the host in `address`.

## Extending gRPC

Interceptors, stats handlers (OpenTelemetry's `otelgrpc`, for example), keepalive and anything else grpc-go supports are injected as options. They are applied after this module's defaults, so they can override them:

```go
server, err := rpc.NewServer(conf,
    rpc.WithGRPCServerOptions(
        grpc.StatsHandler(otelgrpc.NewServerHandler()),
        grpc.ChainUnaryInterceptor(logging),
    ),
)

client, err := rpc.NewClient(clientConf,
    rpc.WithDialOptions(grpc.WithStatsHandler(otelgrpc.NewClientHandler())),
)
```

## Logging

A `Logger` is any type with `Debugf`, `Infof` and `Errorf` methods, so most application loggers satisfy it without an adapter. Pass it with `WithServerLogger` / `WithClientLogger`; a nil logger is ignored. Without one, log output is discarded.

## Testing code that uses this module

`GRPCServer` and `GRPCClient` are the interfaces `*Server` and `*Client` implement. Depend on them in your own code to substitute a fake in tests.
