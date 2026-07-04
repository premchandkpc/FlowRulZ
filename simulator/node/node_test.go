package node

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/fabric"
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

func TestSimNodeBasic(t *testing.T) {
	// Create temp directory for state.
	tmpDir, err := os.MkdirTemp("", "simnode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create fabric and services.
	f := fabric.New()
	f.RegisterNode("node-1", "localhost:8001")

	mockServices := services.DefaultServices()

	// Create node.
	cfg := Config{
		ID:      "node-1",
		NodeID:  "node-1",
		Workers: 2,
		ExecDir: tmpDir,
	}

	node := New(cfg, f, mockServices)

	// Start the node.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	node.Start(ctx)
	defer node.Stop()

	// Verify node is started.
	snapshot := node.Snapshot()
	if snapshot["id"] != "node-1" {
		t.Fatalf("expected node ID 'node-1', got %v", snapshot["id"])
	}
}

func TestSimNodeDeployRule(t *testing.T) {
	// Create temp directory for state.
	tmpDir, err := os.MkdirTemp("", "simnode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create fabric and services.
	f := fabric.New()
	f.RegisterNode("node-1", "localhost:8001")

	mockServices := services.DefaultServices()

	// Create node.
	cfg := Config{
		ID:      "node-1",
		NodeID:  "node-1",
		Workers: 2,
		ExecDir: tmpDir,
	}

	node := New(cfg, f, mockServices)

	// Start the node.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	node.Start(ctx)
	defer node.Stop()

	// Deploy a simple rule.
	err = node.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Verify rule is deployed.
	snapshot := node.Snapshot()
	if snapshot["plans"] != 1 {
		t.Fatalf("expected 1 plan, got %v", snapshot["plans"])
	}
}
