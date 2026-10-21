package rpc

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testPKI struct {
	ca                    *testCA
	serverCert, serverKey string
	clientCert, clientKey string
	otherClientCert       string // signed by a CA the server does not trust
	otherClientKey        string
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()

	dir := t.TempDir()
	ca := newTestCA(t, dir, "ca")
	other := newTestCA(t, dir, "other-ca")

	pki := &testPKI{ca: ca}
	pki.serverCert, pki.serverKey = ca.issue(t, dir, "server")
	pki.clientCert, pki.clientKey = ca.issue(t, dir, "client")
	pki.otherClientCert, pki.otherClientKey = other.issue(t, dir, "other-client")

	return pki
}

func (p *testPKI) serverConf(mutual bool) *ServerConfig {
	conf := &ServerConfig{TLS: TLSConfig{Enabled: true, CertFile: p.serverCert, KeyFile: p.serverKey}}
	if mutual {
		conf.TLS.CAFile = p.ca.file
	}

	return conf
}

func TestTLSRoundTrip(t *testing.T) {
	pki := newTestPKI(t)
	rs := startServer(t, pki.serverConf(false))
	addr := rs.Addr().String()

	t.Run("client verifying with the CA connects", func(t *testing.T) {
		client := newClient(t, &ClientConfig{Address: addr, TLS: TLSConfig{Enabled: true, CAFile: pki.ca.file}})

		if err := check(t, client.GetConnection()); err != nil {
			t.Fatalf("Check: %v", err)
		}
	})

	t.Run("plaintext client is refused", func(t *testing.T) {
		client := newClient(t, &ClientConfig{Address: addr})

		if err := check(t, client.GetConnection()); status.Code(err) != codes.Unavailable {
			t.Fatalf("Check = %v, want Unavailable", err)
		}
	})

	t.Run("client without the CA rejects the server", func(t *testing.T) {
		client := newClient(t, &ClientConfig{Address: addr, TLS: TLSConfig{Enabled: true}})

		if err := check(t, client.GetConnection()); status.Code(err) != codes.Unavailable {
			t.Fatalf("Check = %v, want Unavailable (unknown authority)", err)
		}
	})

	t.Run("server name mismatch is rejected", func(t *testing.T) {
		client := newClient(t, &ClientConfig{Address: addr, TLS: TLSConfig{Enabled: true, CAFile: pki.ca.file, ServerName: "other.example"}})

		if err := check(t, client.GetConnection()); status.Code(err) != codes.Unavailable {
			t.Fatalf("Check = %v, want Unavailable (name mismatch)", err)
		}
	})
}

func TestMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	rs := startServer(t, pki.serverConf(true))
	addr := rs.Addr().String()

	tests := []struct {
		name    string
		tls     TLSConfig
		wantErr bool
	}{
		{
			name: "trusted client certificate",
			tls:  TLSConfig{Enabled: true, CAFile: pki.ca.file, CertFile: pki.clientCert, KeyFile: pki.clientKey},
		},
		{
			name:    "no client certificate",
			tls:     TLSConfig{Enabled: true, CAFile: pki.ca.file},
			wantErr: true,
		},
		{
			name:    "client certificate from an untrusted CA",
			tls:     TLSConfig{Enabled: true, CAFile: pki.ca.file, CertFile: pki.otherClientCert, KeyFile: pki.otherClientKey},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newClient(t, &ClientConfig{Address: addr, TLS: tt.tls})

			err := check(t, client.GetConnection())

			switch {
			case tt.wantErr && status.Code(err) != codes.Unavailable:
				t.Fatalf("Check = %v, want Unavailable", err)
			case !tt.wantErr && err != nil:
				t.Fatalf("Check: %v", err)
			}
		})
	}
}

func TestTLSFileErrors(t *testing.T) {
	pki := newTestPKI(t)
	garbage := filepath.Join(t.TempDir(), "garbage.pem")

	if err := os.WriteFile(garbage, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}

	missing := filepath.Join(t.TempDir(), "missing.pem")

	t.Run("server certificate missing", func(t *testing.T) {
		_, err := NewServer(&ServerConfig{TLS: TLSConfig{Enabled: true, CertFile: missing, KeyFile: missing}})
		assertErr(t, err, "server tls certificate")
	})

	t.Run("server CA unparsable", func(t *testing.T) {
		conf := pki.serverConf(false)
		conf.TLS.CAFile = garbage

		_, err := NewServer(conf)
		assertErr(t, err, "parse rpc tls ca file")
	})

	t.Run("client CA missing", func(t *testing.T) {
		_, err := NewClient(&ClientConfig{Address: "localhost:1", TLS: TLSConfig{Enabled: true, CAFile: missing}})
		assertErr(t, err, "read rpc tls ca file")
	})

	t.Run("client certificate missing", func(t *testing.T) {
		_, err := NewClient(&ClientConfig{Address: "localhost:1", TLS: TLSConfig{Enabled: true, CertFile: missing, KeyFile: missing}})
		assertErr(t, err, "client tls certificate")
	})
}
