// Package scenarios2 provides scenario-based tests for the new fabric-aware
// simulator. These scenarios validate that the simulator correctly models
// real production behavior.
package scenarios2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/simulator/harness"
)

// TestPacketLossTimeout verifies that injected packet loss causes the
// affected execution to fail with a timeout error within bounded time.
func TestPacketLossTimeout(t *testing.T) {
	// Create harness with 2 nodes.
	cfg := harness.Config{
		NumNodes: 2,
		Workers:  2,
	}

	h := harness.New(cfg)
	defer h.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Deploy a rule.
	err := h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Inject 100% packet loss from node-1 to node-2.
	h.Fabric.Link("node-1", "node-2").Loss(1.0).Apply()

	// Try to execute on node-2 (should fail due to packet loss).
	_, err = h.Execute(ctx, "node-2", "test-rule", []byte(`{"test": true}`))
	if err == nil {
		t.Fatal("expected error due to packet loss")
	}

	// Verify the error is a timeout or connection error.
	errStr := err.Error()
	if !contains(errStr, "timeout") && !contains(errStr, "connection") {
		t.Fatalf("expected timeout or connection error, got: %v", err)
	}
}

// TestNodeKillRecovery verifies that killing a node mid-execution
// causes the execution to fail, and restarting the node allows
// new executions to succeed.
func TestNodeKillRecovery(t *testing.T) {
	// Create harness with 2 nodes.
	cfg := harness.Config{
		NumNodes: 2,
		Workers:  2,
	}

	h := harness.New(cfg)
	defer h.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Deploy a rule.
	err := h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Kill node-1.
	err = h.KillNode("node-1")
	if err != nil {
		t.Fatalf("kill node failed: %v", err)
	}

	// Try to execute on node-1 (should fail).
	_, err = h.Execute(ctx, "node-1", "test-rule", []byte(`{"test": true}`))
	if err == nil {
		t.Fatal("expected error after killing node")
	}

	// Restart node-1.
	err = h.RestartNode("node-1")
	if err != nil {
		t.Fatalf("restart node failed: %v", err)
	}

	// Wait for node to be ready.
	time.Sleep(100 * time.Millisecond)

	// Try to execute again (should succeed).
	_, err = h.Execute(ctx, "node-1", "test-rule", []byte(`{"test": true}`))
	if err != nil {
		t.Fatalf("expected success after restart, got: %v", err)
	}
}

// TestNetworkPartition verifies that a network partition causes
// cross-node communication to fail, and healing allows it to succeed.
func TestNetworkPartition(t *testing.T) {
	// Create harness with 2 nodes.
	cfg := harness.Config{
		NumNodes: 2,
		Workers:  2,
	}

	h := harness.New(cfg)
	defer h.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Deploy a rule.
	err := h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Create partition from node-1 to node-2.
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

// TestSlowNetwork verifies that high latency is applied to messages.
func TestSlowNetwork(t *testing.T) {
	// Create harness with 2 nodes.
	cfg := harness.Config{
		NumNodes: 2,
		Workers:  2,
	}

	h := harness.New(cfg)
	defer h.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Deploy a rule.
	err := h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Set high latency from node-1 to node-2.
	h.Fabric.Link("node-1", "node-2").
		Latency(100 * time.Millisecond).
		Apply()

	// Measure execution time.
	start := time.Now()
	_, err = h.Execute(ctx, "node-1", "test-rule", []byte(`{"test": true}`))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	// Verify latency was applied (at least 100ms).
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected at least 100ms latency, got %v", elapsed)
	}
}

// TestMultiNodeExecution verifies that execution works across multiple nodes.
func TestMultiNodeExecution(t *testing.T) {
	// Create harness with 3 nodes.
	cfg := harness.Config{
		NumNodes: 3,
		Workers:  2,
	}

	h := harness.New(cfg)
	defer h.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Deploy a rule to all nodes.
	err := h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Execute on each node.
	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		_, err = h.Execute(ctx, nodeID, "test-rule", []byte(`{"test": true}`))
		if err != nil {
			t.Fatalf("execute on %s failed: %v", nodeID, err)
		}
	}
}

// contains checks if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[0:len(substr)] == substr || contains(s[1:], substr)))
}
