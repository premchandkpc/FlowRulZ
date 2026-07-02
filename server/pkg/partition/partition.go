package partition

import (
	"context"
	"errors"
)

type PartitionManager interface {
	NumPartitions() uint32
	NodeForPartition(partition PartitionID) string
	PartitionsForNode(nodeID string) []PartitionID
	PartitionForKey(key string) PartitionID
	Assignments() []string
	Rebalance(aliveNodes []string, term uint64) []Assignment
	ApplyAssignments(assignments []Assignment)
	PublishAssignments(ctx context.Context, assignments []Assignment) error
	HandleAssignmentMessage(msg []byte) error
	LeaderID() string
	OnLeaderChange(leaderID string)
	SetProducer(p Producer)
}

var (
	ErrNoProducer       = errors.New("no partition producer configured")
	ErrInvalidPartition = errors.New("invalid partition")
)
