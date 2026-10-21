package rpc

import (
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient is the client surface callers depend on; *Client implements it.
type GRPCClient interface {
	// GetConnection returns the connection for generated New*Client functions.
	GetConnection() grpc.ClientConnInterface
	// GetInstanceID returns the configured instance ID.
	GetInstanceID() string
	// GenerateRequestID returns the next request ID: 1, 2, 3, ... per client.
	GenerateRequestID() uint64
	// Close closes the connection.
	Close() error
}

var _ GRPCClient = (*Client)(nil)

// Client is a gRPC client connection with a request ID counter.
type Client struct {
	conn       *grpc.ClientConn
	logger     Logger
	instanceID string
	dialOpts   []grpc.DialOption
	requestID  atomic.Uint64
}

// NewClient validates conf and builds a Client. It does not connect: gRPC
// dials lazily, on the first call.
func NewClient(conf *ClientConfig, opts ...Options[*Client]) (*Client, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	c := &Client{
		logger:     defaultLogger(),
		instanceID: conf.InstanceID,
	}

	applyOptions(c, opts)

	creds, err := conf.transportCredentials()
	if err != nil {
		return nil, err
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy":%q}`, conf.loadBalancingPolicy())),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: conf.minConnectTimeout(),
			Backoff:           backoff.DefaultConfig, // a zero Backoff would retry with no delay
		}),
	}

	c.conn, err = grpc.NewClient(conf.Address, append(dialOpts, c.dialOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("rpc client failed to create connection to %s: %w", conf.Address, err)
	}

	c.logger.Debugf("gRPC client created for %s, instance ID: %s", conf.Address, conf.InstanceID)

	return c, nil
}

func (c *ClientConfig) transportCredentials() (credentials.TransportCredentials, error) {
	if !c.TLS.Enabled {
		return insecure.NewCredentials(), nil
	}

	return c.TLS.clientCredentials()
}

// GetConnection returns the connection for generated New*Client functions.
func (c *Client) GetConnection() grpc.ClientConnInterface {
	return c.conn
}

// GetInstanceID returns the configured instance ID.
func (c *Client) GetInstanceID() string {
	return c.instanceID
}

// GenerateRequestID returns the next request ID: 1, 2, 3, ... per client. It
// is safe for concurrent use; a uint64 counter cannot realistically wrap.
func (c *Client) GenerateRequestID() uint64 {
	return c.requestID.Add(1)
}

// Close closes the connection. Calling it again returns an error.
func (c *Client) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("rpc client failed to close connection: %w", err)
	}

	return nil
}
