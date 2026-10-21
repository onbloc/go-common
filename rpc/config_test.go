package rpc

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A `default` tag must repeat the value DefaultServerConfig/DefaultClientConfig
// give, or a loader and in-code construction would disagree.
func TestDefaultTagsMatchDefaults(t *testing.T) {
	assertTagsMatch(t, reflect.ValueOf(DefaultServerConfig()).Elem())
	assertTagsMatch(t, reflect.ValueOf(DefaultClientConfig()).Elem())
}

func assertTagsMatch(t *testing.T, v reflect.Value) {
	t.Helper()

	for i := range v.NumField() {
		field := v.Type().Field(i)

		if field.Type.Kind() == reflect.Struct {
			assertTagsMatch(t, v.Field(i))

			continue
		}

		tag, ok := field.Tag.Lookup("default")
		if !ok {
			t.Errorf("%s.%s has no default tag", v.Type().Name(), field.Name)

			continue
		}

		if got := fmt.Sprint(v.Field(i).Interface()); got != tag {
			t.Errorf("%s.%s: default tag %q, Default*Config value %q", v.Type().Name(), field.Name, tag, got)
		}
	}
}

func TestServerConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ServerConfig)
		wantErr string
	}{
		{name: "defaults", mutate: func(*ServerConfig) {}},
		{name: "unset fields", mutate: func(c *ServerConfig) { *c = ServerConfig{} }},
		{name: "port zero", mutate: func(c *ServerConfig) { c.Port = "0" }},
		{name: "port not a number", mutate: func(c *ServerConfig) { c.Port = "http" }, wantErr: "port"},
		{name: "port out of range", mutate: func(c *ServerConfig) { c.Port = "65536" }, wantErr: "port"},
		{name: "port negative", mutate: func(c *ServerConfig) { c.Port = "-1" }, wantErr: "port"},
		{name: "negative shutdown timeout", mutate: func(c *ServerConfig) { c.ShutdownTimeoutMs = -1 }, wantErr: "shutdown_timeout_ms"},
		{name: "tls without key", mutate: func(c *ServerConfig) { c.TLS = TLSConfig{Enabled: true, CertFile: "c.pem"} }, wantErr: "cert_file and key_file"},
		{name: "tls disabled with cert_file", mutate: func(c *ServerConfig) { c.TLS = TLSConfig{CertFile: "c.pem"} }, wantErr: "tls is disabled but cert_file set"},
		// A CA set without enabled must not silently drop mutual TLS.
		{name: "tls disabled with ca_file", mutate: func(c *ServerConfig) { c.TLS = TLSConfig{CAFile: "ca.pem"} }, wantErr: "tls is disabled but ca_file set"},
		{name: "tls disabled with every file", mutate: func(c *ServerConfig) { c.TLS = TLSConfig{CertFile: "c", KeyFile: "k", CAFile: "ca"} }, wantErr: "cert_file, key_file, ca_file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := DefaultServerConfig()
			tt.mutate(conf)

			assertErr(t, conf.Validate(), tt.wantErr)
		})
	}

	t.Run("nil", func(t *testing.T) {
		assertErr(t, (*ServerConfig)(nil).Validate(), "required")
	})

	t.Run("reports every problem", func(t *testing.T) {
		err := (&ServerConfig{Port: "x", ShutdownTimeoutMs: -5}).Validate()
		if err == nil || !strings.Contains(err.Error(), "port") || !strings.Contains(err.Error(), "shutdown_timeout_ms") {
			t.Fatalf("Validate() = %v, want both port and shutdown_timeout_ms problems", err)
		}
	})
}

func TestClientConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ClientConfig)
		wantErr string
	}{
		{name: "defaults with address", mutate: func(*ClientConfig) {}},
		{name: "unset tuning fields", mutate: func(c *ClientConfig) { c.LoadBalancingPolicy, c.MinConnectTimeoutMs = "", 0 }},
		{name: "missing address", mutate: func(c *ClientConfig) { c.Address = "" }, wantErr: "address"},
		{name: "blank address", mutate: func(c *ClientConfig) { c.Address = "  " }, wantErr: "address"},
		{name: "policy with quote", mutate: func(c *ClientConfig) { c.LoadBalancingPolicy = `x"}` }, wantErr: "load_balancing_policy"},
		{name: "policy uppercase", mutate: func(c *ClientConfig) { c.LoadBalancingPolicy = "RoundRobin" }, wantErr: "load_balancing_policy"},
		{name: "pick_first policy", mutate: func(c *ClientConfig) { c.LoadBalancingPolicy = "pick_first" }},
		{name: "policy typo", mutate: func(c *ClientConfig) { c.LoadBalancingPolicy = "roundrobin" }, wantErr: "not registered"},
		{name: "policy not registered", mutate: func(c *ClientConfig) { c.LoadBalancingPolicy = "nonexistent_policy" }, wantErr: "not registered"},
		{name: "negative connect timeout", mutate: func(c *ClientConfig) { c.MinConnectTimeoutMs = -1 }, wantErr: "min_connect_timeout_ms"},
		{name: "tls cert without key", mutate: func(c *ClientConfig) { c.TLS = TLSConfig{Enabled: true, CertFile: "c.pem"} }, wantErr: "cert_file and key_file"},
		{name: "tls key without cert", mutate: func(c *ClientConfig) { c.TLS = TLSConfig{Enabled: true, KeyFile: "k.pem"} }, wantErr: "cert_file and key_file"},
		{name: "tls server only", mutate: func(c *ClientConfig) { c.TLS = TLSConfig{Enabled: true} }},
		{name: "tls disabled with ca_file", mutate: func(c *ClientConfig) { c.TLS = TLSConfig{CAFile: "ca.pem"} }, wantErr: "tls is disabled but ca_file set"},
		{name: "tls disabled with client certificate", mutate: func(c *ClientConfig) { c.TLS = TLSConfig{CertFile: "c.pem", KeyFile: "k.pem"} }, wantErr: "cert_file, key_file"},
		{name: "tls disabled with server_name", mutate: func(c *ClientConfig) { c.TLS = TLSConfig{ServerName: "svc"} }, wantErr: "server_name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := DefaultClientConfig()
			conf.Address = "localhost:9090"
			tt.mutate(conf)

			assertErr(t, conf.Validate(), tt.wantErr)
		})
	}

	t.Run("nil", func(t *testing.T) {
		assertErr(t, (*ClientConfig)(nil).Validate(), "required")
	})
}

func TestServerConfigResolvesDefaults(t *testing.T) {
	unset := &ServerConfig{}

	if got := unset.Address(); got != ":8080" {
		t.Errorf("Address() = %q, want :8080", got)
	}

	if got := unset.ShutdownTimeout(); got != 10*time.Second {
		t.Errorf("ShutdownTimeout() = %s, want 10s", got)
	}

	if got := (&ServerConfig{Host: "::1", Port: "9090"}).Address(); got != "[::1]:9090" {
		t.Errorf("Address() = %q, want [::1]:9090", got)
	}

	if got := (&ServerConfig{ShutdownTimeoutMs: 250}).ShutdownTimeout(); got != 250*time.Millisecond {
		t.Errorf("ShutdownTimeout() = %s, want 250ms", got)
	}
}

func assertErr(t *testing.T, err error, want string) {
	t.Helper()

	switch {
	case want == "" && err != nil:
		t.Fatalf("unexpected error: %v", err)
	case want != "" && err == nil:
		t.Fatalf("expected error containing %q, got nil", want)
	case want != "" && !strings.Contains(err.Error(), want):
		t.Fatalf("error %q does not contain %q", err, want)
	}
}

// A millisecond count past maxTimeoutMs overflows time.Duration into a negative
// value: a graceful shutdown would give up at once and force-stop in-flight calls.
func TestTimeoutUpperBound(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int cannot exceed the bound on this platform")
	}

	limit := int(int64(maxTimeoutMs)) // a variable, so a 32-bit build still compiles

	t.Run("server at the bound", func(t *testing.T) {
		conf := &ServerConfig{ShutdownTimeoutMs: limit}
		assertErr(t, conf.Validate(), "")

		if got := conf.ShutdownTimeout(); got <= 0 {
			t.Fatalf("ShutdownTimeout() = %s at the bound, want positive", got)
		}
	})

	t.Run("server past the bound", func(t *testing.T) {
		conf := &ServerConfig{ShutdownTimeoutMs: limit + 1}
		assertErr(t, conf.Validate(), "shutdown_timeout_ms")

		if _, err := NewServer(conf); err == nil {
			t.Fatal("NewServer accepted an overflowing shutdown timeout")
		}
	})

	t.Run("client at the bound", func(t *testing.T) {
		conf := &ClientConfig{Address: "localhost:1", MinConnectTimeoutMs: limit}
		assertErr(t, conf.Validate(), "")

		if got := conf.minConnectTimeout(); got <= 0 {
			t.Fatalf("minConnectTimeout() = %s at the bound, want positive", got)
		}
	})

	t.Run("client past the bound", func(t *testing.T) {
		conf := &ClientConfig{Address: "localhost:1", MinConnectTimeoutMs: limit + 1}
		assertErr(t, conf.Validate(), "min_connect_timeout_ms")

		if _, err := NewClient(conf); err == nil {
			t.Fatal("NewClient accepted an overflowing connect timeout")
		}
	})
}
