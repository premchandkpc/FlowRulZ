package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

func (n *ProdNode) startCluster(ctx context.Context) {
	if n.cluster.ClusterNode == nil {
		return
	}

	if err := n.cluster.ClusterNode.Start(); err != nil {
		slog.Error("cluster: start error", "error", err)
	}

	n.cluster.ClusterNode.Gossiper().OnNodeJoin(func(nodeID, address string) {
		n.cluster.Membership.Heartbeat(nodeID, address)
		if address != "" && nodeID != n.config.NodeID {
			if err := n.cluster.ClusterNode.AddPeer(nodeID, address); err != nil {
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
					NodeID:  n.config.NodeID,
					Address: n.config.GRPCAddr,
				})
				n.cluster.ClusterNode.Publish(DefaultMembersTopic, n.config.NodeID, discMsg)
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
		if err := n.cluster.ClusterNode.AddPeer(seedID, seedAddr); err != nil {
			slog.Error("cluster: connect to seed", "seed_addr", seedAddr, "error", err)
		}
	}
}

func (n *ProdNode) joinRaftCluster(ctx context.Context) {
	raftAddr := fmt.Sprintf("%s:%d", n.config.AdvertiseHost(), n.config.RaftPort)
	body, _ := json.Marshal(map[string]string{
		"node_id":   n.config.NodeID,
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

			resp, err := n.config.httpClient.Post(seedURL, "application/json", bytes.NewReader(body))
			if err != nil {
				slog.Warn("raft join: attempt failed", "attempt", i+1, "seed_url", seedURL, "error", err)
				time.Sleep(2 * time.Second)
				continue
			}
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
