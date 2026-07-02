package plandist

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	pkgplandist "github.com/premchandkpc/FlowRulZ/go/pkg/plandist"
)

type AckMessage = pkgplandist.AckMessage

type AckHandler func(ctx context.Context, msg AckMessage)

func (pd *PlanDistributor) SendAck(ctx context.Context, ruleID string, version uint64, status string) error {
	if pd.ackProducer == nil {
		return fmt.Errorf("plandist: no ACK producer configured")
	}
	msg := AckMessage{
		NodeID:  pd.nodeID,
		RuleID:  ruleID,
		Version: version,
		Status:  status,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("plandist marshal ack: %w", err)
	}
	return pd.ackProducer.Send(ctx, []byte(fmt.Sprintf("%s:%d", ruleID, version)), data)
}

func (pd *PlanDistributor) WaitForAcks(ctx context.Context, ruleID string, version uint64, quorum int, timeout time.Duration) error {
	// quorum=0 means majority of followers (n-1)/2+1; -1 means all followers
	if quorum == 0 || quorum < 0 {
		if pd.quorumProvider != nil {
			n := pd.quorumProvider.AliveCount()
			if n > 1 {
				if quorum == 0 {
					quorum = (n-1)/2 + 1 // majority of non-leader nodes
				} else {
					quorum = n - 1 // all non-leader nodes
				}
			} else {
				quorum = 0 // no followers, skip ack wait
			}
		} else {
			quorum = 1 // fallback: wait for at least one ack
		}
	}
	if quorum <= 0 {
		return nil
	}

	key := ackKey(ruleID, version)
	done := make(chan int, 1)
	received := new(atomic.Int32)
	q32 := int32(quorum)

	pd.pendingAcks.Store(key, pendingAck{done: done, received: received, quorum: q32})
	defer pd.pendingAcks.Delete(key)

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-waitCtx.Done():
		return fmt.Errorf("plandist: ack timeout for %s v%d (got %d/%d)", ruleID, version, received.Load(), quorum)
	case n := <-done:
		if n >= quorum {
			return nil
		}
		return fmt.Errorf("plandist: insufficient acks for %s v%d (%d/%d)", ruleID, version, n, quorum)
	}
}

func (pd *PlanDistributor) handleAck(ack AckMessage) {
	key := ackKey(ack.RuleID, ack.Version)
	v, ok := pd.pendingAcks.Load(key)
	if !ok {
		return
	}
	pa, ok := v.(pendingAck)
	if !ok {
		return
	}
	n := pa.received.Add(1)
	if n >= pa.quorum {
		select {
		case pa.done <- int(n):
		default:
		}
	}
}

func (pd *PlanDistributor) RecordAck(msg AckMessage) {
	pd.handleAck(msg)
}

type pendingAck struct {
	serial   uint64
	done     chan int
	received *atomic.Int32
	quorum   int32
}

func ackKey(ruleID string, version uint64) string {
	return fmt.Sprintf("%s:%d", ruleID, version)
}

func AckMessageFromBytes(data []byte) (*AckMessage, error) {
	var msg AckMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
