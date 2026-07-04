package harness

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestHarnessBasic(t *testing.T) {
	// Create temp directory for state.
	tmpDir, err := os.MkdirTemp("", "harness-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create harness with 3 nodes.
	cfg := Config{
		NumNodes: 3,
		Workers:  2,
		ExecDir:  tmpDir,
	}

	h := New(cfg)
	defer h.Cleanup()

	// Start the harness.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Verify all nodes are started.
	snapshots := h.Snapshot()
	if len(snapshots) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(snapshots))
	}
}

func TestHarnessDeployRule(t *testing.T) {
	// Create temp directory for state.
	tmpDir, err := os.MkdirTemp("", "harness-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create harness with 2 nodes.
	cfg := Config{
		NumNodes: 2,
		Workers:  2,
		ExecDir:  tmpDir,
	}

	h := New(cfg)
	defer h.Cleanup()

	// Start the harness.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Deploy a rule to all nodes.
	err = h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Verify rule is deployed to all nodes.
	snapshots := h.Snapshot()
	for i, snap := range snapshots {
		if snap["plans"] != 1 {
			t.Fatalf("node %d: expected 1 plan, got %v", i, snap["plans"])
		}
	}
}

func TestHarnessPartition(t *testing.T) {
	// Create temp directory for state.
	tmpDir, err := os.MkdirTemp("", "harness-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create harness with 2 nodes.
	cfg := Config{
		NumNodes: 2,
		Workers:  2,
		ExecDir:  tmpDir,
	}

	h := New(cfg)
	defer h.Cleanup()

	// Start the harness.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Create a partition.
	h.Partition("node-1", "node-2")

	// Verify partition exists.
	if !h.Fabric.ShouldDrop("node-1", "node-2") {
		t.Fatal("expected partition to drop messages")
	}

	// Heal the partition.
	h.Heal("node-1", "node-2")

	// Verify partition is healed.
	if h.Fabric.ShouldDrop("node-1", "node-2") {
		t.Fatal("expected partition to be healed")
	}
}

func TestHarnessKillNode(t *testing.T) {
	// Create temp directory for state.
	tmpDir, err := os.MkdirTemp("", "harness-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create harness with 2 nodes.
	cfg := Config{
		NumNodes: 2,
		Workers:  2,
		ExecDir:  tmpDir,
	}

	h := New(cfg)
	defer h.Cleanup()

	// Start the harness.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Kill node-1.
	err = h.KillNode("node-1")
	if err != nil {
		t.Fatalf("kill node failed: %v", err)
	}

	// Verify node-1 is stopped.
	// (In a real test, we'd check if the node is actually stopped)
}
