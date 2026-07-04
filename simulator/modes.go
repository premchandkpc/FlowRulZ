package simulator

import (
	"time"
)

// Mode represents a simulator operational mode.
type Mode string

const (
	ModeSimple        Mode = "simple"
	ModeEnterprise    Mode = "enterprise"
	ModeChaos         Mode = "chaos"
	ModePerformance   Mode = "performance"
	ModeDistributed   Mode = "distributed"
	ModeMultiRegion   Mode = "multi-region"
	ModeInterview     Mode = "interview"
	ModeLearning      Mode = "learning"
)

// ModeConfig defines the configuration for each simulator mode.
type ModeConfig struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Services    string        `json:"services"`    // "simple" or "enterprise"
	Nodes       int           `json:"nodes"`        // cluster nodes
	Regions     []string      `json:"regions"`      // multi-region targets
	MaxTPS      int           `json:"max_tps"`      // rate limit
	Workers     int           `json:"workers"`      // scheduler workers
	Timeout     time.Duration `json:"timeout"`      // request timeout
	RetryMax    int           `json:"retry_max"`    // max retries
	FailureRate float64       `json:"failure_rate"` // base failure rate
	Animation   bool          `json:"animation"`    // show step-by-step animation
}

// Modes returns all available simulator modes.
func Modes() map[Mode]ModeConfig {
	return map[Mode]ModeConfig{
		ModeSimple: {
			Name:        "Simple",
			Description: "4 core services — order, payment, inventory, notification",
			Services:    "simple",
			Nodes:       1,
			Regions:     []string{"us-east-1"},
			MaxTPS:      100,
			Workers:     10,
			Timeout:     10 * time.Second,
			RetryMax:    3,
			FailureRate: 0.05,
			Animation:   false,
		},
		ModeEnterprise: {
			Name:        "Enterprise",
			Description: "40+ services — full virtual company on FlowRulZ",
			Services:    "enterprise",
			Nodes:       3,
			Regions:     []string{"us-east-1"},
			MaxTPS:      5000,
			Workers:     50,
			Timeout:     30 * time.Second,
			RetryMax:    3,
			FailureRate: 0.01,
			Animation:   false,
		},
		ModeChaos: {
			Name:        "Chaos",
			Description: "Everything failing — test FlowRulZ resilience",
			Services:    "enterprise",
			Nodes:       3,
			Regions:     []string{"us-east-1"},
			MaxTPS:      1000,
			Workers:     30,
			Timeout:     15 * time.Second,
			RetryMax:    5,
			FailureRate: 0.3,
			Animation:   false,
		},
		ModePerformance: {
			Name:        "Performance",
			Description: "10K concurrent users — push FlowRulZ limits",
			Services:    "enterprise",
			Nodes:       5,
			Regions:     []string{"us-east-1"},
			MaxTPS:      100000,
			Workers:     200,
			Timeout:     5 * time.Second,
			RetryMax:    1,
			FailureRate: 0.001,
			Animation:   false,
		},
		ModeDistributed: {
			Name:        "Distributed",
			Description: "3 clusters — cross-cluster plan distribution",
			Services:    "enterprise",
			Nodes:       9, // 3 per cluster
			Regions:     []string{"us-east-1"},
			MaxTPS:      10000,
			Workers:     100,
			Timeout:     20 * time.Second,
			RetryMax:    3,
			FailureRate: 0.02,
			Animation:   false,
		},
		ModeMultiRegion: {
			Name:        "Multi-Region",
			Description: "US + Europe + Asia — geo-distributed with latency",
			Services:    "enterprise",
			Nodes:       9, // 3 per region
			Regions:     []string{"us-east-1", "eu-west-1", "ap-south-1"},
			MaxTPS:      15000,
			Workers:     150,
			Timeout:     30 * time.Second,
			RetryMax:    3,
			FailureRate: 0.01,
			Animation:   false,
		},
		ModeInterview: {
			Name:        "Interview",
			Description: "Shows FlowRulZ architecture — perfect for demoing",
			Services:    "enterprise",
			Nodes:       3,
			Regions:     []string{"us-east-1"},
			MaxTPS:      500,
			Workers:     20,
			Timeout:     30 * time.Second,
			RetryMax:    3,
			FailureRate: 0.05,
			Animation:   true,
		},
		ModeLearning: {
			Name:        "Learning",
			Description: "Every step animated — learn how FlowRulZ works",
			Services:    "simple",
			Nodes:       1,
			Regions:     []string{"us-east-1"},
			MaxTPS:      50,
			Workers:     5,
			Timeout:     60 * time.Second,
			RetryMax:    3,
			FailureRate: 0.1,
			Animation:   true,
		},
	}
}

// GetMode returns the config for a given mode, defaulting to Enterprise.
func GetMode(mode string) ModeConfig {
	modes := Modes()
	m := Mode(mode)
	if cfg, ok := modes[m]; ok {
		return cfg
	}
	return modes[ModeEnterprise]
}
