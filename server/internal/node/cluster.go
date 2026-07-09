package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"
)

func (n *ProdNode) startCluster(ctx context.Context) {
	if n.ClusterNode == nil {
		return
	}

	if err := n.ClusterNode.Start(); err != nil {
		slog.Error("cluster: start error", "error", err)
	}

	n.ClusterNode.Gossiper().OnNodeJoin(func(nodeID, address string) {
		n.Membership.Heartbeat(nodeID, address)
		if address != "" && nodeID != n.nodeID {
			if err := n.ClusterNode.AddPeer(nodeID, address); err != nil {
				slog.Debug("cluster: auto-add peer from gossip", "peer", nodeID, "addr", address, "error", err)
			}
		}
	})

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				discMsg, _ := json.Marshal(NodeDiscoveryMessage{
					NodeID:  n.nodeID,
					Address: n.config.GRPCAddr,
				})
				n.ClusterNode.Publish(DefaultMembersTopic, n.nodeID, discMsg)
			case <-ctx.Done():
				return
			}
		}
	}()

	for _, seedAddr := range n.config.Seeds {
		if seedAddr == n.config.GRPCAddr {
			continue
		}
		seedID := fmt.Sprintf("seed-%s", seedAddr)
		if err := n.ClusterNode.AddPeer(seedID, seedAddr); err != nil {
			slog.Error("cluster: connect to seed", "seed_addr", seedAddr, "error", err)
		}
	}
}

func (n *ProdNode) joinRaftCluster(ctx context.Context) {
	// Use advertise address if configured, otherwise fall back to localhost.
	// In k8s, set AdvertiseAddr to the pod's DNS name (e.g.
	// flowrulz-0.flowrulz-bus.<ns>.svc.cluster.local).
	raftAddr := fmt.Sprintf("%s:%d", n.config.AdvertiseHost(), n.config.RaftPort)
	body, _ := json.Marshal(map[string]string{
		"node_id":   n.nodeID,
		"raft_addr": raftAddr,
	})

	for _, seed := range n.config.Seeds {
		seedURL := fmt.Sprintf("http://%s/cluster/join", seed)
		for i := 0; i < 30; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := n.httpClient.Post(seedURL, "application/json", bytes.NewReader(body))
			if err != nil {
				slog.Warn("raft join: attempt failed", "attempt", i+1, "seed_url", seedURL, "error", err)
				time.Sleep(2 * time.Second)
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				slog.Info("raft join: successfully joined cluster", "seed_url", seedURL)
				return
			}
			slog.Warn("raft join: attempt got non-200", "attempt", i+1, "seed_url", seedURL, "status_code", resp.StatusCode)
			time.Sleep(2 * time.Second)
		}
	}
	slog.Error("raft join: failed to join cluster after 30 attempts")
}

func (n *ProdNode) nextDeployTerm() uint64 {
	if n.RaftCluster != nil {
		return n.RaftCluster.CurrentTerm()
	}
	return n.PlanDist.CurrentTerm() + 1
}
