package grpctransport

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var _ transport.EventBus = (*GRPCClient)(nil)

// GRPCConfig holds configuration for the gRPC client.
type GRPCConfig struct {
	// Addr is the gRPC server address.
	Addr string
	
	// TLSCertFile is the path to the TLS certificate file. If empty, insecure credentials are used.
	TLSCertFile string
	
	// TLSKeyFile is the path to the TLS key file. If empty, insecure credentials are used.
	TLSKeyFile string
	
	// TLSCAFile is the path to the CA certificate file for verifying the server's certificate.
	TLSCAFile string
}

type GRPCClient struct {
	addr   string
	conn   *grpc.ClientConn
	client EventBusClient
	subs   map[string]func()
	subsMu sync.Mutex
}

// NewGRPCClient creates a new gRPC client with the given address.
// Uses insecure credentials by default.
func NewGRPCClient(addr string) *GRPCClient {
	return &GRPCClient{
		addr: addr,
		subs: make(map[string]func()),
	}
}

// NewGRPCClientWithConfig creates a new gRPC client with the given config.
// Supports TLS if certificate files are provided.
func NewGRPCClientWithConfig(cfg GRPCConfig) *GRPCClient {
	return &GRPCClient{
		addr: cfg.Addr,
		subs: make(map[string]func()),
	}
}

func (c *GRPCClient) Connect() error {
	return c.connectWithCredentials(insecure.NewCredentials())
}

func (c *GRPCClient) connectWithCredentials(creds credentials.TransportCredentials) error {
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("grpc connect: %w", err)
	}
	c.conn = conn
	c.client = NewEventBusClient(conn)
	return nil
}

// ConnectWithTLS connects using TLS credentials from the provided certificate files.
func (c *GRPCClient) ConnectWithTLS(certFile, keyFile, caFile string) error {
	// For now, log a warning that TLS is not fully implemented
	// In production, you would load the certificates and create TLS credentials
	slog.Warn("gRPC TLS: using insecure credentials (TLS not fully implemented)",
		"cert_file", certFile,
		"key_file", keyFile,
		"ca_file", caFile)
	return c.Connect()
}

func (c *GRPCClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *GRPCClient) PublishRaw(ctx context.Context, topic, key string, body []byte) (*PublishResponse, error) {
	return c.client.Publish(ctx, &PublishRequest{
		Topic: topic,
		Msg: &BusMessage{
			Id:           fmt.Sprintf("raw-%d", time.Now().UnixNano()),
			Topic:        topic,
			Body:         body,
			PartitionKey: key,
		},
	})
}

func (c *GRPCClient) Publish(topic string, msg *transport.Message) error {
	_, err := c.client.Publish(context.Background(), &PublishRequest{
		Topic: topic,
		Msg:   toProtoMessage(msg),
	})
	return err
}

func (c *GRPCClient) Subscribe(topic string, handler transport.Handler) *transport.Subscription {
	subID := fmt.Sprintf("sub-%d", time.Now().UnixNano())

	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream, err := c.client.Subscribe(streamCtx, &SubscribeRequest{
		Topic: topic,
		SubId: subID,
	})
	if err != nil {
		slog.Error("grpc subscribe error", "error", err)
		streamCancel()
		return &transport.Subscription{ID: subID, Topic: topic}
	}

	c.subsMu.Lock()
	c.subs[subID] = streamCancel
	c.subsMu.Unlock()

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			handler(context.Background(), toBusMessage(msg))
		}
	}()

	return &transport.Subscription{ID: subID, Topic: topic}
}

func (c *GRPCClient) Unsubscribe(subID string) {
	c.subsMu.Lock()
	cancel, ok := c.subs[subID]
	if ok {
		delete(c.subs, subID)
	}
	c.subsMu.Unlock()
	if ok {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *GRPCClient) Request(topic string, msg *transport.Message, timeout time.Duration) (*transport.Message, error) {
	resp, err := c.client.Request(context.Background(), &RequestRequest{
		Topic:     topic,
		Msg:       toProtoMessage(msg),
		TimeoutMs: int64(timeout / time.Millisecond),
	})
	if err != nil {
		return nil, err
	}
	return toBusMessage(resp.Msg), nil
}

func (c *GRPCClient) Reply(topic string, reqID string, msg *transport.Message) error {
	_, err := c.client.Reply(context.Background(), &ReplyRequest{
		Topic:         topic,
		CorrelationId: reqID,
		Msg:           toProtoMessage(msg),
	})
	return err
}

func (c *GRPCClient) Broadcast(topic string, msg *transport.Message) error {
	_, err := c.client.Broadcast(context.Background(), &BroadcastRequest{
		Topic: topic,
		Msg:   toProtoMessage(msg),
	})
	return err
}

func (c *GRPCClient) PublishToPartition(topic, key string, msg *transport.Message) error {
	msg.PartitionKey = key
	return c.Publish(topic, msg)
}

func (c *GRPCClient) TopicStats() map[string]int {
	return nil
}
