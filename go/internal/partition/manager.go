package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"sync"

	pkgpartition "github.com/premchandkpc/FlowRulZ/go/pkg/partition"
)

var _ pkgpartition.PartitionManager = (*Manager)(nil)

const (
	DefaultNumPartitions = 64
	PartitionTopic       = "_flowrulz_partitions"
)

type Assignment = pkgpartition.Assignment

type PartitionMessage struct {
	Type        string       `json:"type"`
	Assignments []Assignment `json:"assignments"`
	NodeID      string       `json:"node_id,omitempty"`
	Term        uint64       `json:"term"`
}

type Manager struct {
	mu            sync.RWMutex
	numPartitions uint32
	assignments   []string
	nodeParts     map[string][]pkgpartition.PartitionID
	currentTerm   uint64
	producer      pkgpartition.Producer
	leaderID      string
}

func New(numPartitions uint32) *Manager {
	if numPartitions == 0 {
		numPartitions = DefaultNumPartitions
	}
	return &Manager{
		numPartitions: numPartitions,
		assignments:   make([]string, numPartitions),
		nodeParts:     make(map[string][]pkgpartition.PartitionID),
	}
}

func (m *Manager) SetProducer(p pkgpartition.Producer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.producer = p
}

func (m *Manager) Assignments() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.assignments))
	copy(out, m.assignments)
	return out
}

func (m *Manager) NodeForPartition(partition pkgpartition.PartitionID) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if int(partition) >= len(m.assignments) {
		return ""
	}
	return m.assignments[int(partition)]
}

func (m *Manager) PartitionsForNode(nodeID string) []pkgpartition.PartitionID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]pkgpartition.PartitionID, len(m.nodeParts[nodeID]))
	copy(out, m.nodeParts[nodeID])
	return out
}

func (m *Manager) PartitionForKey(key string) pkgpartition.PartitionID {
	h := fnv.New32a()
	h.Write([]byte(key))
	return pkgpartition.PartitionID(h.Sum32() % m.numPartitions)
}

func (m *Manager) NumPartitions() uint32 {
	return m.numPartitions
}

func (m *Manager) LeaderID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leaderID
}

func (m *Manager) Rebalance(aliveNodes []string, term uint64) []Assignment {
	m.mu.Lock()
	defer m.mu.Unlock()

	sort.Strings(aliveNodes)
	m.currentTerm = term

	newAssignments := make([]string, m.numPartitions)
	nodeParts := make(map[string][]pkgpartition.PartitionID)

	if len(aliveNodes) == 0 {
		m.assignments = newAssignments
		m.nodeParts = nodeParts
		return nil
	}

	for i := pkgpartition.PartitionID(0); i < pkgpartition.PartitionID(m.numPartitions); i++ {
		node := aliveNodes[int(i)%len(aliveNodes)]
		newAssignments[int(i)] = node
		nodeParts[node] = append(nodeParts[node], i)
	}

	m.assignments = newAssignments
	m.nodeParts = nodeParts

	if len(aliveNodes) > 0 {
		m.leaderID = aliveNodes[0]
	}

	assignments := make([]Assignment, m.numPartitions)
	for i, nodeID := range m.assignments {
		assignments[i] = Assignment{
			NodeID:    nodeID,
			Partition: pkgpartition.PartitionID(i),
			Term:      term,
		}
	}

	return assignments
}

func (m *Manager) ApplyAssignments(assignments []Assignment) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newAssignments := make([]string, m.numPartitions)
	nodeParts := make(map[string][]pkgpartition.PartitionID)

	for _, a := range assignments {
		if int(a.Partition) < len(newAssignments) {
			newAssignments[int(a.Partition)] = a.NodeID
			nodeParts[a.NodeID] = append(nodeParts[a.NodeID], a.Partition)
		}
		if a.Term > m.currentTerm {
			m.currentTerm = a.Term
		}
	}

	m.assignments = newAssignments
	m.nodeParts = nodeParts
}

func (m *Manager) PublishAssignments(ctx context.Context, assignments []Assignment) error {
	m.mu.RLock()
	producer := m.producer
	term := m.currentTerm
	leaderID := m.leaderID
	m.mu.RUnlock()

	if producer == nil {
		return fmt.Errorf("partition: no producer configured")
	}

	msg := PartitionMessage{
		Type:        "assign",
		Assignments: assignments,
		NodeID:      leaderID,
		Term:        term,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("partition marshal: %w", err)
	}
	return producer.Send(ctx, []byte("partition-assign"), data)
}

func (m *Manager) HandleAssignmentMessage(msg []byte) error {
	var pm PartitionMessage
	if err := json.Unmarshal(msg, &pm); err != nil {
		return fmt.Errorf("partition unmarshal: %w", err)
	}
	if pm.Type == "assign" {
		m.ApplyAssignments(pm.Assignments)
		log.Printf("partition: applied %d assignments from term %d", len(pm.Assignments), pm.Term)
	}
	return nil
}

func (m *Manager) OnLeaderChange(leaderID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderID = leaderID
}
