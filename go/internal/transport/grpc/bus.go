package grpctransport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/pkg/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TopicHandler func(ctx context.Context, msg *BusMessage)

type GRPCBus struct {
	addr        string
	subscribers map[string]map[string]chan *BusMessage
	handlers    map[string]TopicHandler
	mu          sync.RWMutex
	server      *grpc.Server
	lis         net.Listener
	started     bool
	stopCh      chan struct{}

	UnimplementedEventBusServer
}

func NewGRPCBus(addr string) *GRPCBus {
	return &GRPCBus{
		addr:        addr,
		subscribers: make(map[string]map[string]chan *BusMessage),
		handlers:    make(map[string]TopicHandler),
		stopCh:      make(chan struct{}),
	}
}

func (b *GRPCBus) AddTopicHandler(topic string, handler TopicHandler) {
	b.mu.Lock()
	b.handlers[topic] = handler
	b.mu.Unlock()
}

func (b *GRPCBus) RemoveTopicHandler(topic string) {
	b.mu.Lock()
	delete(b.handlers, topic)
	b.mu.Unlock()
}

func (b *GRPCBus) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.started {
		return nil
	}

	lis, err := net.Listen("tcp", b.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	b.lis = lis
	b.server = grpc.NewServer()
	RegisterEventBusServer(b.server, b)
	b.started = true

	go func() {
		if err := b.server.Serve(lis); err != nil {
			log.Printf("grpc bus: serve: %v", err)
		}
	}()

	return nil
}

func (b *GRPCBus) Publish(_ context.Context, req *PublishRequest) (*PublishResponse, error) {
	b.mu.RLock()
	subs, ok := b.subscribers[req.Topic]
	handler := b.handlers[req.Topic]
	b.mu.RUnlock()

	if handler != nil {
		handler(context.Background(), req.Msg)
	}

	if !ok {
		return &PublishResponse{}, nil
	}

	for _, ch := range subs {
		select {
		case ch <- req.Msg:
		default:
		}
	}
	return &PublishResponse{}, nil
}

func (b *GRPCBus) deliverToTopic(topic string, msg *BusMessage) {
	b.mu.RLock()
	subs, ok := b.subscribers[topic]
	handler := b.handlers[topic]
	b.mu.RUnlock()

	if handler != nil {
		handler(context.Background(), msg)
	}
	if ok {
		for _, ch := range subs {
			select {
			case ch <- msg:
			default:
			}
		}
	}
}

func (b *GRPCBus) Request(_ context.Context, req *RequestRequest) (*RequestResponse, error) {
	replyCh := make(chan *BusMessage, 1)
	replyTopic := fmt.Sprintf("__reply_%s", req.Msg.CorrelationId)
	subID := fmt.Sprintf("req-%s-%d", req.Msg.CorrelationId, time.Now().UnixNano())

	b.mu.Lock()
	if _, ok := b.subscribers[req.Topic]; ok {
		for _, ch := range b.subscribers[req.Topic] {
			select {
			case ch <- req.Msg:
			default:
			}
		}
	}
	rsubs, ok := b.subscribers[replyTopic]
	if !ok {
		rsubs = make(map[string]chan *BusMessage)
		b.subscribers[replyTopic] = rsubs
	}
	rsubs[subID] = replyCh
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		if s, ok := b.subscribers[replyTopic]; ok {
			delete(s, subID)
		}
		b.mu.Unlock()
	}()

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	select {
	case resp := <-replyCh:
		return &RequestResponse{Msg: resp}, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("request timeout")
	}
}

func (b *GRPCBus) Reply(ctx context.Context, req *ReplyRequest) (*ReplyResponse, error) {
	b.deliverToTopic(fmt.Sprintf("__reply_%s", req.CorrelationId), req.Msg)
	return &ReplyResponse{}, nil
}

func (b *GRPCBus) Broadcast(_ context.Context, req *BroadcastRequest) (*BroadcastResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for topic, subs := range b.subscribers {
		if topic == req.Topic {
			if h, ok := b.handlers[topic]; ok {
				h(context.Background(), req.Msg)
			}
			for _, ch := range subs {
				select {
				case ch <- req.Msg:
				default:
				}
			}
		}
	}
	return &BroadcastResponse{}, nil
}

func (b *GRPCBus) Subscribe(req *SubscribeRequest, stream EventBus_SubscribeServer) error {
	b.mu.Lock()
	subs, ok := b.subscribers[req.Topic]
	if !ok {
		subs = make(map[string]chan *BusMessage)
		b.subscribers[req.Topic] = subs
	}
	ch := make(chan *BusMessage, 100)
	subs[req.SubId] = ch
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		if s, ok := b.subscribers[req.Topic]; ok {
			delete(s, req.SubId)
		}
		b.mu.Unlock()
	}()

	for {
		select {
		case msg := <-ch:
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (b *GRPCBus) Stop() {
	b.mu.Lock()
	srv := b.server
	b.server = nil
	b.mu.Unlock()

	if srv != nil {
		srv.GracefulStop()
	}
	close(b.stopCh)
}

func toBusMessage(msg *BusMessage) *transport.Message {
	if msg == nil {
		return nil
	}
	m := &transport.Message{
		ID:            msg.Id,
		Type:          transport.MessageType(msg.Type),
		Topic:         msg.Topic,
		Body:          msg.Body,
		Headers:       msg.Headers,
		CorrelationID: msg.CorrelationId,
		ReplyTo:       msg.ReplyTo,
		PartitionKey:  msg.PartitionKey,
	}
	if msg.CreatedAt != nil {
		m.CreatedAt = msg.CreatedAt.AsTime()
	}
	if msg.DelayNanos > 0 {
		m.Delay = time.Duration(msg.DelayNanos)
	}
	if len(msg.Metadata) > 0 {
		m.Metadata = make(map[string]interface{})
		for k, v := range msg.Metadata {
			m.Metadata[k] = v
		}
	}
	return m
}

func toProtoMessage(msg *transport.Message) *BusMessage {
	if msg == nil {
		return nil
	}
	pm := &BusMessage{
		Id:            msg.ID,
		Type:          int32(msg.Type),
		Topic:         msg.Topic,
		Body:          msg.Body,
		Headers:       msg.Headers,
		CorrelationId: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		PartitionKey:  msg.PartitionKey,
		CreatedAt:     timestamppb.New(msg.CreatedAt),
		DelayNanos:    msg.Delay.Nanoseconds(),
	}
	if len(msg.Metadata) > 0 {
		pm.Metadata = make(map[string][]byte)
		for k, v := range msg.Metadata {
			if s, ok := v.(string); ok {
				pm.Metadata[k] = []byte(s)
			}
		}
	}
	return pm
}

type GRPCClient struct {
	addr    string
	conn    *grpc.ClientConn
	client  EventBusClient
	subs    map[string]func()
	subsMu  sync.Mutex
}

func NewGRPCClient(addr string) *GRPCClient {
	return &GRPCClient{
		addr: addr,
		subs: make(map[string]func()),
	}
}

func (c *GRPCClient) Connect() error {
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc connect: %w", err)
	}
	c.conn = conn
	c.client = NewEventBusClient(conn)
	return nil
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
		log.Printf("grpc: subscribe error: %v", err)
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
