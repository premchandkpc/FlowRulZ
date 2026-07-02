package cluster

import "context"

type Gossiper interface {
	Start(ctx context.Context) error
	Stop() error
	OnNodeJoin(fn func(nodeID, addr string)) CancelFunc
	OnNodeLeave(fn func(nodeID string)) CancelFunc
	Publish(topic string, key string, data []byte) error
	AddPeer(id, addr string) error
	RemovePeer(id string) error
}
