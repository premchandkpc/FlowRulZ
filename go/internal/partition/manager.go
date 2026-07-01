package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

const (
	DefaultNumPartitions = 64
	PartitionTopic       = "_flowrulz_partitions"
)

type Assignment struct {
	NodeID    string   `json:"node_id"`
	Address   string   `json:"address,omitempty"`
	Partition uint32   `json:"partition"`
	Term      uint64   `json:"term"`
}

type PartitionMessage struct {
	Type        string       `json:"type"`
	Assignments []Assignment `json:"assignments"`
	NodeID      string       `json:"node_id,omitempty"`
	Term        uint64       `json:"term"`
}

type Manager struct {
	mu           sync.RWMutex
	numPartitions uint32
	assignments  []string          // partition → nodeID
	nodeParts    map[string][]uint32 // nodeID → partitions
	currentTerm  uint64

	producer transport.MessageProducer

	leaderID string
}

func New(numPartitions uint32) *Manager {
	if numPartitions == 0 {
		numPartitions = DefaultNumPartitions
	}
	return &Manager{
		numPartitions: numPartitions,
		assignments:   make([]string, numPartitions),
		nodeParts:     make(map[string][]uint32),
	}
}

func (m *Manager) SetProducer(p transport.MessageProducer) {
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

func (m *Manager) NodeForPartition(partition uint32) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if int(partition) >= len(m.assignments) {
		return ""
	}
	return m.assignments[partition]
}

func (m *Manager) PartitionsForNode(nodeID string) []uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]uint32, len(m.nodeParts[nodeID]))
	copy(out, m.nodeParts[nodeID])
	return out
}

func (m *Manager) PartitionForKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % m.numPartitions
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
	nodeParts := make(map[string][]uint32)

	if len(aliveNodes) == 0 {
		m.assignments = newAssignments
		m.nodeParts = nodeParts
		return nil
	}

	for i := uint32(0); i < m.numPartitions; i++ {
		node := aliveNodes[int(i)%len(aliveNodes)]
		newAssignments[i] = node
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
			Partition: uint32(i),
			Term:      term,
		}
	}

	return assignments
}

func (m *Manager) ApplyAssignments(assignments []Assignment) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newAssignments := make([]string, m.numPartitions)
	nodeParts := make(map[string][]uint32)

	for _, a := range assignments {
		if int(a.Partition) < len(newAssignments) {
			newAssignments[a.Partition] = a.NodeID
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
	m.mu.RUnlock()

	if producer == nil {
		return fmt.Errorf("partition: no producer configured")
	}

	msg := PartitionMessage{
		Type:        "assign",
		Assignments: assignments,
		NodeID:      m.leaderID,
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

type RebalanceNotifier struct {
	manager  *Manager
	aliveFn  func() []string
	termFn   func() uint64
	notifyFn func()
	mu       sync.Mutex
	lastNodes []string
}

func NewRebalanceNotifier(m *Manager, aliveFn func() []string, termFn func() uint64) *RebalanceNotifier {
	return &RebalanceNotifier{
		manager:  m,
		aliveFn:  aliveFn,
		termFn:   termFn,
	}
}

func (rn *RebalanceNotifier) SetNotify(fn func()) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.notifyFn = fn
}

func (rn *RebalanceNotifier) CheckAndRebalance() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	nodes := rn.aliveFn()
	if len(nodes) == 0 {
		return false
	}

	sort.Strings(nodes)
	same := len(nodes) == len(rn.lastNodes)
	if same {
		for i, n := range nodes {
			if n != rn.lastNodes[i] {
				same = false
				break
			}
		}
	}
	if same {
		return false
	}

	rn.lastNodes = make([]string, len(nodes))
	copy(rn.lastNodes, nodes)
	log.Printf("partition: membership changed (%d nodes), triggering rebalance", len(nodes))

	rn.manager.Rebalance(nodes, rn.termFn())
	if rn.notifyFn != nil {
		rn.notifyFn()
	}
	return true
}

type atomicCounter struct {
	v atomic.Uint64
}

func (a *atomicCounter) inc() uint64 { return a.v.Add(1) }
func (a *atomicCounter) get() uint64 { return a.v.Load() }
