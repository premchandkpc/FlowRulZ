package node

import (
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
	PersistPath  string
	ExecStateDir string

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

func (c *Config) ExecDir() string {
	if c.ExecStateDir != "" {
		return c.ExecStateDir
	}
	return filepath.Join(os.TempDir(), "flowrulz-execstate")
}

func (c *Config) DLQDir() string {
	return filepath.Join(os.TempDir(), "flowrulz-dlq")
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
