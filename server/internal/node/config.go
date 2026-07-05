package node

import (
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/cluster"
)

const (
	DefaultHTTPAddr = ":8080"
	DefaultGRPCAddr = ":9090"
	DefaultTopic    = "flowrulz-input"
	DefaultNodeID   = "node-1"
	DefaultGroupID  = "flowrulz"

	defaultReplyRouterMaxPending = 10000
	defaultDedupCapacity         = 10000
	defaultDLQMaxEntries         = 10000
	defaultNumPartitions         = 64
)

type Config struct {
	// Identity
	NodeID string

	// Listen addresses
	HTTPAddr string
	GRPCAddr string

	// Advertise address — the address this node advertises to peers.
	// If empty, falls back to GRPCAddr for backward compatibility.
	// In k8s, set to pod DNS name (e.g. flowrulz-0.flowrulz-bus.<ns>.svc.cluster.local).
	AdvertiseAddr string

	// Raft
	RaftPort      int
	RaftDir       string
	RaftBootstrap bool

	// Seeds (other nodes for clustering)
	Seeds []string

	// Compiler (remote Rust compiler)
	CompilerAddr string

	// Plugin directory
	PluginDir string

	// Kafka
	KafkaBrokers    []string
	KafkaGroupID    string
	KafkaAcks       string
	KafkaIdempotent bool

	// Persistence
	PersistPath string

	// Topics
	Topic string
}

func DefaultConfig() *Config {
	return &Config{
		NodeID:        DefaultNodeID,
		HTTPAddr:      DefaultHTTPAddr,
		GRPCAddr:      DefaultGRPCAddr,
		RaftPort:      cluster.DefaultRaftPort,
		RaftDir:       filepath.Join(os.TempDir(), "flowrulz-raft"),
		RaftBootstrap: false,
		Topic:         DefaultTopic,
		KafkaGroupID:  DefaultGroupID,
	}
}

func (c *Config) GRPCListenAddr() string {
	if c.GRPCAddr != "" {
		return c.GRPCAddr
	}
	return DefaultGRPCAddr
}

func (c *Config) HTTPListenAddr() string {
	if c.HTTPAddr != "" {
		return c.HTTPAddr
	}
	return DefaultHTTPAddr
}

func (c *Config) ReplyRouterCleanupInterval() time.Duration {
	return 1 * time.Second
}

func (c *Config) ReplyRouterMaxPending() int {
	return defaultReplyRouterMaxPending
}

func (c *Config) DedupCapacity() int {
	return defaultDedupCapacity
}

func (c *Config) DedupTTL() time.Duration {
	return 5 * time.Minute
}

func (c *Config) DLQMaxEntries() int {
	return defaultDLQMaxEntries
}

func (c *Config) RegistryHeartbeatTimeout() time.Duration {
	return 30 * time.Second
}

func (c *Config) NumPartitions() int {
	return defaultNumPartitions
}

// AdvertiseHost returns the host portion of the advertise address.
// If AdvertiseAddr is set, returns that host. Otherwise falls back to
// GRPCAddr for backward compatibility with single-host deployments.
func (c *Config) AdvertiseHost() string {
	if c.AdvertiseAddr != "" {
		host, _, err := net.SplitHostPort(c.AdvertiseAddr)
		if err != nil {
			return c.AdvertiseAddr
		}
		return host
	}
	// Fallback: extract host from GRPCAddr (e.g. ":9090" -> "localhost").
	host, _, err := net.SplitHostPort(c.GRPCAddr)
	if err != nil {
		return "localhost"
	}
	if host == "" {
		return "localhost"
	}
	return host
}
