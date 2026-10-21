package rpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const testTimeout = 5 * time.Second

// runningServer is a Server started on a free local port with the standard
// health service registered.
type runningServer struct {
	*Server
	cancel context.CancelFunc
	done   chan error
}

func startServer(t *testing.T, conf *ServerConfig, opts ...Options[*Server]) *runningServer {
	t.Helper()

	if conf == nil {
		conf = DefaultServerConfig()
	}

	conf.Host = "127.0.0.1"
	conf.Port = "0"

	server, err := NewServer(conf, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	healthpb.RegisterHealthServer(server.GetServiceRegistrar(), health.NewServer())

	ctx, cancel := context.WithCancel(context.Background())
	rs := &runningServer{Server: server, cancel: cancel, done: make(chan error, 1)}

	go func() { rs.done <- server.Start(ctx) }()

	select {
	case <-server.Ready():
	case err := <-rs.done:
		cancel()
		t.Fatalf("Start returned before ready: %v", err)
	case <-time.After(testTimeout):
		cancel()
		t.Fatal("server did not become ready")
	}

	t.Cleanup(func() {
		cancel()
		server.Stop()
	})

	return rs
}

// wait returns Start's result, failing the test if it does not return.
func (rs *runningServer) wait(t *testing.T) error {
	t.Helper()

	select {
	case err := <-rs.done:
		return err
	case <-time.After(testTimeout):
		t.Fatal("Start did not return")

		return nil
	}
}

func newClient(t *testing.T, conf *ClientConfig, opts ...Options[*Client]) *Client {
	t.Helper()

	client, err := NewClient(conf, opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("Close: %v", err)
		}
	})

	return client
}

func check(t *testing.T, conn grpc.ClientConnInterface) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})

	return err
}

// testCA is a throwaway certificate authority.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string // PEM path of the CA certificate
}

func newTestCA(t *testing.T, dir, name string) *testCA {
	t.Helper()

	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}

	return &testCA{cert: cert, key: key, file: writePEM(t, dir, name+".pem", "CERTIFICATE", der)}
}

// issue signs a leaf for 127.0.0.1/localhost and returns the cert and key PEM paths.
func (ca *testCA) issue(t *testing.T, dir, name string) (certFile, keyFile string) {
	t.Helper()

	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create %s cert: %v", name, err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", name, err)
	}

	return writePEM(t, dir, name+".pem", "CERTIFICATE", der), writePEM(t, dir, name+".key", "PRIVATE KEY", keyDER)
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	return key
}

func writePEM(t *testing.T, dir, name, blockType string, der []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)
	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}

	return path
}

// recordingLogger captures formatted log lines.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) record(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Debugf(format string, args ...any) { l.record(format, args...) }
func (l *recordingLogger) Infof(format string, args ...any)  { l.record(format, args...) }
func (l *recordingLogger) Errorf(format string, args ...any) { l.record(format, args...) }

func (l *recordingLogger) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return strings.Join(l.lines, "\n")
}
