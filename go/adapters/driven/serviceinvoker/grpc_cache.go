package serviceinvoker

import (
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCConnectionCache manages a pool of gRPC connections with idle eviction.
// One grpc.ClientConn per target address, built lazily and reused.
type GRPCConnectionCache struct {
	conns      map[string]*cachedConn
	mu         sync.RWMutex
	maxIdle    time.Duration
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

type cachedConn struct {
	conn      *grpc.ClientConn
	lastUsed  time.Time
}

// NewGRPCConnectionCache creates a new cache with idle eviction.
func NewGRPCConnectionCache(maxIdle time.Duration) *GRPCConnectionCache {
	cache := &GRPCConnectionCache{
		conns:   make(map[string]*cachedConn),
		maxIdle: maxIdle,
		stopCh:  make(chan struct{}),
	}
	// Start background eviction goroutine
	cache.wg.Add(1)
	go cache.evictLoop()
	return cache
}

// Get returns a cached connection or creates a new one.
func (c *GRPCConnectionCache) Get(addr string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	if cached, ok := c.conns[addr]; ok {
		c.mu.RUnlock()
		c.mu.Lock()
		cached.lastUsed = time.Now()
		c.mu.Unlock()
		return cached.conn, nil
	}
	c.mu.RUnlock()

	// Create new connection
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if cached, ok := c.conns[addr]; ok {
		cached.lastUsed = time.Now()
		return cached.conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	c.conns[addr] = &cachedConn{
		conn:     conn,
		lastUsed: time.Now(),
	}

	slog.Debug("grpc cache: created connection", "addr", addr)
	return conn, nil
}

// CloseConn closes a specific connection by address.
func (c *GRPCConnectionCache) CloseConn(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cached, ok := c.conns[addr]; ok {
		cached.conn.Close()
		delete(c.conns, addr)
		slog.Debug("grpc cache: closed connection", "addr", addr)
	}
}

// CloseAll closes all connections.
func (c *GRPCConnectionCache) CloseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, cached := range c.conns {
		cached.conn.Close()
		delete(c.conns, addr)
	}
}

// Stop stops the eviction goroutine and closes all connections.
func (c *GRPCConnectionCache) Stop() {
	close(c.stopCh)
	c.wg.Wait()
	c.CloseAll()
}

// evictLoop periodically removes idle connections.
func (c *GRPCConnectionCache) evictLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.evictIdle()
		}
	}
}

// evictIdle removes connections not used within maxIdle duration.
func (c *GRPCConnectionCache) evictIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for addr, cached := range c.conns {
		if now.Sub(cached.lastUsed) > c.maxIdle {
			slog.Info("grpc cache: evicting idle connection",
				"addr", addr,
				"idle", now.Sub(cached.lastUsed))
			cached.conn.Close()
			delete(c.conns, addr)
		}
	}
}

// Len returns the number of cached connections.
func (c *GRPCConnectionCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.conns)
}

// Ensure GRPCConnectionCache implements io.Closer
var _ io.Closer = (*GRPCConnectionCache)(nil)

func (c *GRPCConnectionCache) Close() error {
	c.Stop()
	return nil
}
