package messaging

import (
	"fmt"
	"time"
)

type Config struct {
	Type       string
	Brokers    []string
	GroupID    string
	ClientID   string
	Timeout    time.Duration
	Acks       string
	Idempotent bool
}

func (c Config) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("messaging: type is required")
	}
	if len(c.Brokers) == 0 && c.Type != "memory" {
		return fmt.Errorf("messaging: brokers required for %s", c.Type)
	}
	return nil
}

func DefaultConfig() Config {
	return Config{
		Type:    "memory",
		Timeout: 30 * time.Second,
		Acks:    "all",
	}
}
