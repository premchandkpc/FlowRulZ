package partition

import "context"

type PartitionID uint32

type Assignment struct {
	NodeID    string      `json:"node_id"`
	Address   string      `json:"address,omitempty"`
	Partition PartitionID `json:"partition"`
	Term      uint64      `json:"term"`
}

type Producer interface {
	Send(ctx context.Context, key []byte, msg []byte) error
}
