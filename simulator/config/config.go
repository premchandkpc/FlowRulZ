package config

import (
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/network"
)

type SimConfig struct {
	Nodes        int           `yaml:"nodes"`
	Workers      int           `yaml:"workers"`
	Scenario     string        `yaml:"scenario"`
	Duration     time.Duration `yaml:"duration"`
	Rate         int           `yaml:"rate"`
	Speed        float64       `yaml:"speed"`
	Dashboard    bool          `yaml:"dashboard"`
	DashboardAddr string       `yaml:"dashboard_addr"`
	Chaos        network.ChaosConfig `yaml:"chaos"`
	Plans        []string      `yaml:"plans"`
	Verbose      bool          `yaml:"verbose"`
}

func Default() SimConfig {
	return SimConfig{
		Nodes:         3,
		Workers:       4,
		Scenario:      "black-friday",
		Duration:      30 * time.Second,
		Rate:          100,
		Speed:         1.0,
		Dashboard:     true,
		DashboardAddr: ":8081",
		Verbose:       false,
	}
}
