package node

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/agent"
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

	// TLS
	TLSCertFile string
	TLSKeyFile  string

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

	// Agent pool configuration
	AgentMinAgents   int
	AgentMaxAgents   int
	AgentQueueSize   int
	AgentExecTimeout time.Duration
	AgentHealthCheck time.Duration
}

func DefaultConfig() *Config {
	homeDir, _ := os.UserHomeDir()
	persistRoot := filepath.Join(homeDir, ".flowrulz")
	return &Config{
		NodeID:          DefaultNodeID,
		HTTPAddr:        DefaultHTTPAddr,
		GRPCAddr:        DefaultGRPCAddr,
		TLSCertFile:     os.Getenv("FLOWRULZ_TLS_CERT"),
		TLSKeyFile:      os.Getenv("FLOWRULZ_TLS_KEY"),
		RaftPort:        cluster.DefaultRaftPort,
		RaftDir:         filepath.Join(persistRoot, "raft"),
		RaftBootstrap:   false,
		Topic:           DefaultTopic,
		KafkaGroupID:    DefaultGroupID,
		AgentMinAgents:  4,
		AgentMaxAgents:  32,
		AgentQueueSize:  10000,
		AgentExecTimeout: 60 * time.Second,
		AgentHealthCheck: 5 * time.Second,
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

func (c *Config) AgentPoolConfig() agent.PoolConfig {
	return agent.PoolConfig{
		MinAgents:   c.AgentMinAgents,
		MaxAgents:   c.AgentMaxAgents,
		QueueSize:   c.AgentQueueSize,
		ExecTimeout: c.AgentExecTimeout,
		HealthCheck: c.AgentHealthCheck,
	}
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

// HasTLS returns true if TLS certificate and key are configured.
func (c *Config) HasTLS() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}

// Validate checks the config for unsafe or invalid combinations.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if c.NodeID == "node-1" && len(c.Seeds) > 0 {
		// Warn but don't fail — "node-1" is a valid ID in small clusters
	}
	if c.TLSCertFile != "" && c.TLSKeyFile == "" {
		return fmt.Errorf("TLS key file is required when certificate is configured")
	}
	if c.TLSKeyFile != "" && c.TLSCertFile == "" {
		return fmt.Errorf("TLS certificate file is required when key is configured")
	}
	if len(c.KafkaBrokers) > 0 && c.KafkaGroupID == "" {
		return fmt.Errorf("Kafka group ID is required when brokers are configured")
	}
	if c.AgentMinAgents <= 0 {
		return fmt.Errorf("agent min agents must be > 0")
	}
	if c.AgentMaxAgents < c.AgentMinAgents {
		return fmt.Errorf("agent max agents must be >= min agents")
	}
	if c.AgentQueueSize <= 0 {
		return fmt.Errorf("agent queue size must be > 0")
	}
	return nil
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
