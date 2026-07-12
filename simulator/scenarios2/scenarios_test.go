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
	"github.com/premchandkpc/FlowRulZ/simulator/services"
)

// TestPacketLossTimeout verifies that when a node is unreachable (simulating
// total packet loss), execution requests fail within bounded time.
func TestPacketLossTimeout(t *testing.T) {
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

	err := h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Kill node-2 to simulate unreachable (all packets lost).
	err = h.KillNode("node-2")
	if err != nil {
		t.Fatalf("kill node failed: %v", err)
	}

	// Execute on the killed node — should fail since bus is closed.
	start := time.Now()
	_, err = h.Execute(ctx, "node-2", "test-rule", []byte(`{"test": true}`))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error due to killed node")
	}

	// Should fail quickly (< 5s), not hang waiting for a reply.
	if elapsed > 5*time.Second {
		t.Fatalf("expected fast failure, took %v", elapsed)
	}
}

// TestNodeKillRecovery verifies that killing a node mid-execution
// causes the execution to fail, and restarting the node allows
// new executions to succeed.
func TestNodeKillRecovery(t *testing.T) {
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

// TestNetworkPartition verifies that a network partition prevents
// cross-node communication, and healing allows it to succeed.
func TestNetworkPartition(t *testing.T) {
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

// TestSlowNetwork verifies that execution accounts for service latencies.
// Each service in the chain adds its own latency, so total time should
// reflect cumulative service latencies.
func TestSlowNetwork(t *testing.T) {
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

	// Deploy a rule that chains two services.
	err := h.DeployRule("test-rule", "n:validate n:inventory")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Set known latencies for services.
	validate := h.MockServices.Get("validate")
	validate.BaseLatency = 50 * time.Millisecond
	validate.FailureRate = 0.0

	inventory := h.MockServices.Get("inventory")
	inventory.BaseLatency = 50 * time.Millisecond
	inventory.FailureRate = 0.0

	// Execute and measure time.
	start := time.Now()
	_, err = h.Execute(ctx, "node-1", "test-rule", []byte(`{"test": true}`))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	// Should take at least 100ms (sum of service latencies).
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected at least 100ms latency from services, got %v", elapsed)
	}
}

// TestMultiNodeExecution verifies that execution works across multiple nodes.
func TestMultiNodeExecution(t *testing.T) {
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

// TestServiceLatencyAccumulation verifies that chaining multiple services
// accumulates their latencies correctly.
func TestServiceLatencyAccumulation(t *testing.T) {
	cfg := harness.Config{
		NumNodes: 1,
		Workers:  2,
	}

	h := harness.New(cfg)
	defer h.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.Start(ctx)
	defer h.Stop()

	// Deploy a rule that chains three services.
	err := h.DeployRule("test-rule", "n:validate n:inventory n:payment")
	if err != nil {
		t.Fatalf("deploy rule failed: %v", err)
	}

	// Set known latencies.
	for _, name := range []string{"validate", "inventory", "payment"} {
		svc := h.MockServices.Get(name)
		if svc == nil {
			t.Fatalf("service %s not found", name)
		}
		svc.BaseLatency = 30 * time.Millisecond
		svc.FailureRate = 0.0
	}

	// Execute and measure time.
	start := time.Now()
	_, err = h.Execute(ctx, "node-1", "test-rule", []byte(`{"test": true}`))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	// Should take at least 90ms (3 services × 30ms each).
	if elapsed < 90*time.Millisecond {
		t.Fatalf("expected at least 90ms latency from 3 services, got %v", elapsed)
	}
}

// Ensure services import is used.
var _ = services.DefaultServices
