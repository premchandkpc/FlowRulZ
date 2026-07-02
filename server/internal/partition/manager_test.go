package partition

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/premchandkpc/FlowRulZ/server/internal/transport"
	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
)

type mockProducer struct {
	sendFn func(ctx context.Context, key, value []byte) error
}

func (m *mockProducer) Send(ctx context.Context, key, value []byte) error {
	if m.sendFn != nil {
		return m.sendFn(ctx, key, value)
	}
	return nil
}
func (m *mockProducer) Close() {}

var _ transport.MessageProducer = (*mockProducer)(nil)

func TestRebalanceBasic(t *testing.T) {
	m := New(8)
	nodes := []string{"node-a", "node-b", "node-c"}
	assignments := m.Rebalance(nodes, 1)

	if len(assignments) != 8 {
		t.Fatalf("expected 8 assignments, got %d", len(assignments))
	}

	// Verify round-robin: node-a, node-b, node-c, node-a, ...
	expected := []string{"node-a", "node-b", "node-c", "node-a", "node-b", "node-c", "node-a", "node-b"}
	for i, a := range assignments {
		if a.NodeID != expected[i] {
			t.Errorf("partition %d: expected %s, got %s", i, expected[i], a.NodeID)
		}
	}
}

func TestNodeForPartition(t *testing.T) {
	m := New(4)
	m.Rebalance([]string{"node-x", "node-y"}, 1)

	if m.NodeForPartition(0) != "node-x" {
		t.Fatalf("expected node-x for partition 0")
	}
	if m.NodeForPartition(1) != "node-y" {
		t.Fatalf("expected node-y for partition 1")
	}
	if m.NodeForPartition(5) != "" {
		t.Fatalf("expected empty for out-of-range partition")
	}
}

func TestPartitionsForNode(t *testing.T) {
	m := New(6)
	m.Rebalance([]string{"node-a", "node-b"}, 1)

	partsA := m.PartitionsForNode("node-a")
	partsB := m.PartitionsForNode("node-b")
	if len(partsA) != 3 || len(partsB) != 3 {
		t.Fatalf("expected 3 partitions each, got %d and %d", len(partsA), len(partsB))
	}
}

func TestPartitionForKey(t *testing.T) {
	m := New(64)

	p1 := m.PartitionForKey("hello")
	p2 := m.PartitionForKey("hello")
	if p1 != p2 {
		t.Fatalf("same key should map to same partition")
	}

	different := false
	p3 := m.PartitionForKey("world")
	for i := 0; i < 100; i++ {
		if m.PartitionForKey("key-"+string(rune(i))) != p3 {
			different = true
			break
		}
	}
	if !different {
		t.Log("warning: keys may not distribute well")
	}
}

func TestApplyAssignments(t *testing.T) {
	m := New(4)
	assignments := []Assignment{
		{NodeID: "node-1", Partition: 0, Term: 1},
		{NodeID: "node-2", Partition: 1, Term: 1},
		{NodeID: "node-1", Partition: 2, Term: 1},
		{NodeID: "node-2", Partition: 3, Term: 1},
	}
	m.ApplyAssignments(assignments)

	if m.NodeForPartition(0) != "node-1" {
		t.Fatalf("expected node-1 for partition 0")
	}
	if m.NodeForPartition(1) != "node-2" {
		t.Fatalf("expected node-2 for partition 1")
	}

	parts1 := m.PartitionsForNode("node-1")
	if len(parts1) != 2 {
		t.Fatalf("expected 2 partitions for node-1, got %d", len(parts1))
	}
}

func TestRebalanceTrigger(t *testing.T) {
	m := New(8)
	rn := NewRebalanceNotifier(m,
		func() []string { return []string{"a", "b", "c"} },
		func() uint64 { return 1 },
	)

	changed := rn.CheckAndRebalance()
	if !changed {
		t.Fatal("expected rebalance on first check")
	}

	assignments := m.Assignments()
	if len(assignments) == 0 {
		t.Fatal("expected assignments after rebalance")
	}

	// Same nodes → no rebalance
	changed = rn.CheckAndRebalance()
	if changed {
		t.Fatal("expected no rebalance for same nodes")
	}
}

func TestRebalanceSingleNode(t *testing.T) {
	m := New(8)
	assignments := m.Rebalance([]string{"node-only"}, 1)
	if len(assignments) != 8 {
		t.Fatalf("expected 8 assignments, got %d", len(assignments))
	}
	for _, a := range assignments {
		if a.NodeID != "node-only" {
			t.Errorf("expected all partitions on node-only, got %s", a.NodeID)
		}
	}
}

func TestRebalanceEmptyNodes(t *testing.T) {
	m := New(8)
	assignments := m.Rebalance(nil, 1)
	if assignments != nil {
		t.Fatal("expected nil for no nodes")
	}
}

func TestPartitionOnLeaderChange(t *testing.T) {
	m := New(64)
	if m.LeaderID() != "" {
		t.Fatalf("expected empty leader initially, got %s", m.LeaderID())
	}

	m.OnLeaderChange("leader-42")
	if m.LeaderID() != "leader-42" {
		t.Fatalf("expected leader-42, got %s", m.LeaderID())
	}

	m.OnLeaderChange("leader-99")
	if m.LeaderID() != "leader-99" {
		t.Fatalf("expected leader-99 after change, got %s", m.LeaderID())
	}
}

func TestPartitionSetProducer(t *testing.T) {
	m := New(64)
	sent := false
	prod := &mockProducer{
		sendFn: func(_ context.Context, key, value []byte) error {
			sent = true
			return nil
		},
	}
	m.SetProducer(prod)

	assignments := m.Rebalance([]string{"node-a", "node-b"}, 1)
	err := m.PublishAssignments(context.Background(), assignments)
	if err != nil {
		t.Fatalf("PublishAssignments failed: %v", err)
	}
	if !sent {
		t.Fatal("expected Send to be called")
	}
}

func TestPartitionHandleAssignmentMessage(t *testing.T) {
	m := New(4)
	msg := PartitionMessage{
		Type: "assign",
		Assignments: []Assignment{
			{NodeID: "node-x", Partition: 0, Term: 2},
			{NodeID: "node-y", Partition: 1, Term: 2},
			{NodeID: "node-x", Partition: 2, Term: 2},
			{NodeID: "node-y", Partition: 3, Term: 2},
		},
		Term: 2,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.HandleAssignmentMessage(data); err != nil {
		t.Fatalf("HandleAssignmentMessage failed: %v", err)
	}

	if m.NodeForPartition(0) != "node-x" {
		t.Fatalf("expected node-x for partition 0, got %s", m.NodeForPartition(0))
	}
	if m.NodeForPartition(1) != "node-y" {
		t.Fatalf("expected node-y for partition 1, got %s", m.NodeForPartition(1))
	}
	if len(m.PartitionsForNode("node-x")) != 2 {
		t.Fatalf("expected 2 partitions for node-x, got %d", len(m.PartitionsForNode("node-x")))
	}
}

func TestPartitionHandleAssignmentMessageInvalid(t *testing.T) {
	m := New(64)
	err := m.HandleAssignmentMessage([]byte("not valid json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestPartitionHandleAssignmentMessageWrongType(t *testing.T) {
	m := New(4)
	msg := PartitionMessage{
		Type: "heartbeat",
		Assignments: []Assignment{
			{NodeID: "node-x", Partition: 0, Term: 1},
		},
		Term: 1,
	}
	data, _ := json.Marshal(msg)

	m.Rebalance([]string{"node-a", "node-b"}, 1)

	if err := m.HandleAssignmentMessage(data); err != nil {
		t.Fatalf("HandleAssignmentMessage failed: %v", err)
	}

	// Assignments should not be applied since type != "assign"
	if m.NodeForPartition(0) != "node-a" {
		t.Fatalf("expected node-a (unchanged), got %s", m.NodeForPartition(0))
	}
}

func TestPartitionRebalanceNotifierSameNodes(t *testing.T) {
	m := New(8)
	rn := NewRebalanceNotifier(m,
		func() []string { return []string{"a", "b", "c"} },
		func() uint64 { return 1 },
	)

	changed1 := rn.CheckAndRebalance()
	if !changed1 {
		t.Fatal("expected rebalance on first check")
	}

	changed2 := rn.CheckAndRebalance()
	if changed2 {
		t.Fatal("expected no rebalance for same nodes")
	}
}

func TestPartitionRebalanceNotifierEmptyNodes(t *testing.T) {
	m := New(8)
	rn := NewRebalanceNotifier(m,
		func() []string { return nil },
		func() uint64 { return 1 },
	)

	changed := rn.CheckAndRebalance()
	if changed {
		t.Fatal("expected no rebalance for empty node list")
	}
}

func TestPartitionRebalanceNotifierDifferentNodes(t *testing.T) {
	m := New(8)
	nodeList := []string{"a", "b", "c"}
	rn := NewRebalanceNotifier(m,
		func() []string { return nodeList },
		func() uint64 { return 1 },
	)

	rn.CheckAndRebalance()

	nodeList = []string{"a", "b", "c", "d"}
	changed := rn.CheckAndRebalance()
	if !changed {
		t.Fatal("expected rebalance when nodes change")
	}
}

func TestPartitionOnLeaderChangeLeaderID(t *testing.T) {
	m := New(64)
	m.OnLeaderChange("node-1")
	if id := m.LeaderID(); id != "node-1" {
		t.Fatalf("expected node-1, got %s", id)
	}
}

func TestPartitionKeyDistribution(t *testing.T) {
	m := New(64)

	partitions := make(map[pkgpartition.PartitionID]int)
	for i := 0; i < 1000; i++ {
		key := string(rune(i))
		p := m.PartitionForKey(key)
		partitions[p]++
	}

	if len(partitions) < 2 {
		t.Fatal("keys should distribute across multiple partitions, but all hit the same one")
	}
}

func TestPartitionNumPartitions(t *testing.T) {
	if DefaultNumPartitions != 64 {
		t.Fatalf("expected DefaultNumPartitions=64, got %d", DefaultNumPartitions)
	}

	m := New(0)
	if m.NumPartitions() != 64 {
		t.Fatalf("expected NumPartitions=64 when created with 0, got %d", m.NumPartitions())
	}

	m2 := New(16)
	if m2.NumPartitions() != 16 {
		t.Fatalf("expected NumPartitions=16, got %d", m2.NumPartitions())
	}
}

func TestPartitionAssignmentsCopy(t *testing.T) {
	m := New(4)
	m.Rebalance([]string{"node-a", "node-b"}, 1)

	original := m.Assignments()
	modified := m.Assignments()
	modified[0] = "hijacked"

	if m.NodeForPartition(0) != "node-a" {
		t.Fatalf("internal state was mutated: expected node-a, got %s", m.NodeForPartition(0))
	}
	if original[0] != "node-a" {
		t.Fatal("returned slice was not a copy")
	}
}

func TestPartitionPublishAssignmentsNoProducer(t *testing.T) {
	m := New(64)
	assignments := m.Rebalance([]string{"node-a"}, 1)
	err := m.PublishAssignments(context.Background(), assignments)
	if err == nil {
		t.Fatal("expected error when no producer configured")
	}
}

func TestPartitionPublishAssignmentsEmptyAssignments(t *testing.T) {
	m := New(64)
	m.SetProducer(&mockProducer{})
	err := m.PublishAssignments(context.Background(), nil)
	if err != nil {
		t.Fatalf("PublishAssignments with nil should not error: %v", err)
	}
}

func TestPartitionHandleAssignmentMessageEmptyAssignments(t *testing.T) {
	m := New(4)
	m.Rebalance([]string{"node-a", "node-b"}, 1)

	msg := PartitionMessage{
		Type:        "assign",
		Assignments: []Assignment{},
		Term:        2,
	}
	data, _ := json.Marshal(msg)
	if err := m.HandleAssignmentMessage(data); err != nil {
		t.Fatalf("HandleAssignmentMessage failed: %v", err)
	}

	if len(m.Assignments()) != 4 {
		t.Fatalf("expected 4 assignments, got %d", len(m.Assignments()))
	}
	// All partitions should be empty string after empty assignments
	for i, n := range m.Assignments() {
		if n != "" {
			t.Fatalf("expected empty partition %d, got %s", i, n)
		}
	}
}
