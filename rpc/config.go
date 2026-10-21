package rpc

import (
	"errors"
	"fmt"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/balancer"
)

// Documented defaults. The struct `default` tags below must repeat these as
// literals (tags cannot reference constants); TestDefaultTagsMatchDefaults
// keeps the two in sync. They are applied by the Default*Config functions, and
// as a fallback when a field is left unset in a config built without a loader.
const (
	defaultServerPort        = "8080"
	defaultShutdownTimeoutMs = 10000

	defaultLoadBalancingPolicy = "round_robin"
	defaultMinConnectTimeoutMs = 5000
)

// maxTimeoutMs is the largest millisecond count that still fits a
// time.Duration (int64 nanoseconds); larger values would overflow.
const maxTimeoutMs = math.MaxInt64 / int64(time.Millisecond)

var loadBalancingPolicyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// TLSConfig configures transport security for a server or a client. TLS 1.2 is
// the minimum version.
//
// Server: Enabled requires CertFile and KeyFile. Setting CAFile also turns on
// mutual TLS: clients must present a certificate signed by that CA.
//
// Client: CAFile is the CA bundle that verifies the server; unset uses the
// system roots. CertFile and KeyFile, set together, are the client certificate
// for mutual TLS. ServerName overrides the name verified against the server
// certificate (default: the host in ClientConfig.Address).
//
// With Enabled false, setting any other TLS key is rejected by Validate: a CA
// or certificate that was set but not applied would leave the endpoint in
// plaintext, and for a server without client authentication.
type TLSConfig struct {
	Enabled    bool   `mapstructure:"enabled" default:"false"`
	CertFile   string `mapstructure:"cert_file" default:""`
	KeyFile    string `mapstructure:"key_file" default:""`
	CAFile     string `mapstructure:"ca_file" default:""`
	ServerName string `mapstructure:"server_name" default:""`
}

// setKeys lists the TLS keys that are set but have no effect while TLS is
// disabled. Validate rejects them: a CA or certificate that is silently
// ignored leaves the endpoint in plaintext, with no client authentication.
// server_name only matters to a client.
func (t *TLSConfig) setKeys(withServerName bool) []string {
	var keys []string

	for _, f := range []struct{ key, value string }{
		{"cert_file", t.CertFile},
		{"key_file", t.KeyFile},
		{"ca_file", t.CAFile},
	} {
		if f.value != "" {
			keys = append(keys, f.key)
		}
	}

	if withServerName && t.ServerName != "" {
		keys = append(keys, "server_name")
	}

	return keys
}

// ServerConfig is the application-owned gRPC server configuration. It is a
// plain struct with no dependency on a config loader.
//
// Unset (zero) tuning fields are valid and resolve to their documented
// defaults. Call Validate to check a ServerConfig; NewServer does so too.
type ServerConfig struct {
	// Host is the interface to listen on; empty listens on all interfaces.
	Host string `mapstructure:"host" default:""`
	// Port is the TCP port to listen on. Unset means "8080"; "0" picks a free
	// port (see Server.Addr).
	Port string `mapstructure:"port" default:"8080"`
	// ShutdownTimeoutMs bounds how long Start waits for in-flight calls after
	// its context ends before forcing them closed. Unset means 10000.
	ShutdownTimeoutMs int `mapstructure:"shutdown_timeout_ms" default:"10000"`
	// DisableReflection turns off the gRPC reflection service, which is
	// registered by default and lets clients such as grpcurl list services.
	DisableReflection bool      `mapstructure:"disable_reflection" default:"false"`
	TLS               TLSConfig `mapstructure:"tls"`
}

// DefaultServerConfig returns a ServerConfig with every documented default
// set. It is the in-code equivalent of what a loader produces from the
// `default` tags.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Port:              defaultServerPort,
		ShutdownTimeoutMs: defaultShutdownTimeoutMs,
	}
}

// Address returns the listen address as host:port, with defaults applied.
func (c *ServerConfig) Address() string {
	port := c.Port
	if port == "" {
		port = defaultServerPort
	}

	return net.JoinHostPort(c.Host, port)
}

// ShutdownTimeout returns the graceful shutdown bound, with defaults applied.
func (c *ServerConfig) ShutdownTimeout() time.Duration {
	ms := c.ShutdownTimeoutMs
	if ms == 0 {
		ms = defaultShutdownTimeoutMs
	}

	return time.Duration(ms) * time.Millisecond
}

// Validate reports every problem with the values in c, joined into one error.
func (c *ServerConfig) Validate() error {
	if c == nil {
		return errors.New("rpc server config is required")
	}

	var errs []error

	if c.Port != "" {
		if _, err := strconv.ParseUint(c.Port, 10, 16); err != nil {
			errs = append(errs, fmt.Errorf("rpc server port %q must be a number between 0 and 65535", c.Port))
		}
	}

	if c.ShutdownTimeoutMs < 0 || int64(c.ShutdownTimeoutMs) > maxTimeoutMs {
		errs = append(errs, fmt.Errorf("rpc server shutdown_timeout_ms must be between 0 and %d, got %d", maxTimeoutMs, c.ShutdownTimeoutMs))
	}

	if c.TLS.Enabled {
		if c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
			errs = append(errs, errors.New("rpc server tls requires both cert_file and key_file"))
		}
	} else if keys := c.TLS.setKeys(false); len(keys) > 0 {
		errs = append(errs, fmt.Errorf("rpc server tls is disabled but %s set; set tls.enabled to use them", strings.Join(keys, ", ")))
	}

	return errors.Join(errs...)
}

// ClientConfig is the application-owned gRPC client configuration. It is a
// plain struct with no dependency on a config loader.
//
// Unset (zero) tuning fields are valid and resolve to their documented
// defaults. Call Validate to check a ClientConfig; NewClient does so too.
type ClientConfig struct {
	// InstanceID is a caller-side label: it is returned by GetInstanceID and
	// logged on creation, but this package does not send it to servers.
	InstanceID string `mapstructure:"instance_id" default:""`
	// Address is the target as host:port (or any gRPC target URI). Required.
	Address string `mapstructure:"address" default:""`
	// LoadBalancingPolicy is the gRPC load balancing policy name. Unset means
	// "round_robin".
	LoadBalancingPolicy string `mapstructure:"load_balancing_policy" default:"round_robin"`
	// MinConnectTimeoutMs is the minimum time given to each connection
	// attempt. Unset means 5000.
	MinConnectTimeoutMs int       `mapstructure:"min_connect_timeout_ms" default:"5000"`
	TLS                 TLSConfig `mapstructure:"tls"`
}

// DefaultClientConfig returns a ClientConfig with every documented default
// set and no address. Set Address, then override anything else.
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		LoadBalancingPolicy: defaultLoadBalancingPolicy,
		MinConnectTimeoutMs: defaultMinConnectTimeoutMs,
	}
}

func (c *ClientConfig) loadBalancingPolicy() string {
	if c.LoadBalancingPolicy == "" {
		return defaultLoadBalancingPolicy
	}

	return c.LoadBalancingPolicy
}

func (c *ClientConfig) minConnectTimeout() time.Duration {
	ms := c.MinConnectTimeoutMs
	if ms == 0 {
		ms = defaultMinConnectTimeoutMs
	}

	return time.Duration(ms) * time.Millisecond
}

// Validate reports every problem with the values in c, joined into one error.
func (c *ClientConfig) Validate() error {
	if c == nil {
		return errors.New("rpc client config is required")
	}

	var errs []error

	if strings.TrimSpace(c.Address) == "" {
		errs = append(errs, errors.New("rpc client address is required"))
	}

	if p := c.LoadBalancingPolicy; p != "" {
		switch {
		case !loadBalancingPolicyPattern.MatchString(p):
			errs = append(errs, fmt.Errorf("rpc client load_balancing_policy %q must be a lowercase policy name such as round_robin", p))
		case balancer.Get(p) == nil:
			// grpc silently falls back to pick_first for an unknown name.
			errs = append(errs, fmt.Errorf("rpc client load_balancing_policy %q is not registered; round_robin and pick_first are built in, other policies need their grpc package imported", p))
		}
	}

	if c.MinConnectTimeoutMs < 0 || int64(c.MinConnectTimeoutMs) > maxTimeoutMs {
		errs = append(errs, fmt.Errorf("rpc client min_connect_timeout_ms must be between 0 and %d, got %d", maxTimeoutMs, c.MinConnectTimeoutMs))
	}

	if c.TLS.Enabled {
		if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
			errs = append(errs, errors.New("rpc client tls requires cert_file and key_file together"))
		}
	} else if keys := c.TLS.setKeys(true); len(keys) > 0 {
		errs = append(errs, fmt.Errorf("rpc client tls is disabled but %s set; set tls.enabled to use them", strings.Join(keys, ", ")))
	}

	return errors.Join(errs...)
}
