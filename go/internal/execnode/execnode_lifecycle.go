package execnode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/internal/cluster"
	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

func (en *ExecutionNode) IsLeader() bool {
	if en.RaftCluster != nil {
		return en.RaftCluster.IsLeader()
	}
	return true // single-node mode, always leader
}

func (en *ExecutionNode) CurrentTerm() uint64 {
	if en.RaftCluster != nil {
		return en.RaftCluster.CurrentTerm()
	}
	return en.PlanDist.CurrentTerm()
}

func (en *ExecutionNode) configureEngineHooks() {
	en.Engine.AfterDeploy = en.handleEngineDeploy
	en.Engine.AfterPromote = en.handleEnginePromote
}

func (en *ExecutionNode) handleEngineDeploy(id, dsl string, plan []byte, version uint64) {
	if !en.IsLeader() {
		return
	}
	en.PlanDist.SetTerm(en.nextDeployTerm())
	go en.distributePlan(id, dsl, plan, version)
}

func (en *ExecutionNode) handleEnginePromote(id string, version uint64) {
	if !en.IsLeader() {
		return
	}
	go en.distributeActivate(id, version)
}

func (en *ExecutionNode) nextDeployTerm() uint64 {
	if en.RaftCluster != nil {
		return en.RaftCluster.CurrentTerm()
	}
	return en.PlanDist.CurrentTerm() + 1
}

func (en *ExecutionNode) mkProducer(topic string, kc transport.KafkaConfig) transport.MessageProducer {
	if len(kc.Brokers) > 0 {
		p := transport.NewKafkaProducer(topic, kc)
		en.mu.Lock()
		en.producers = append(en.producers, p)
		en.mu.Unlock()
		return p
	}
	if en.ClusterNode != nil {
		return cluster.NewClusterProducer(topic, en.ClusterNode)
	}
	return transport.NewProducer(topic)
}

func (en *ExecutionNode) mkConsumer(topic string, handler transport.MessageHandler, kc transport.KafkaConfig) transport.MessageConsumer {
	if len(kc.Brokers) > 0 {
		return transport.NewKafkaConsumer(topic, handler, kc)
	}
	if en.ClusterNode != nil {
		return cluster.NewClusterConsumer(topic, handler, en.ClusterNode)
	}
	return transport.NewConsumer(topic, handler)
}

func (en *ExecutionNode) distributePlan(id, dsl string, plan []byte, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := en.PlanDist.PublishPlan(ctx, id, version, plan, dsl); err != nil {
		slog.Error("plandist: publish plan error", "id", id, "version", version, "error", err)
		return
	}

	if err := en.PlanDist.WaitForAcks(ctx, id, version, 0, 10*time.Second); err != nil {
		slog.Error("plandist: ack wait error", "id", id, "version", version, "error", err)
	}

	if err := en.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error", "id", id, "version", version, "error", err)
	}
}

func (en *ExecutionNode) distributeActivate(id string, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := en.PlanDist.ActivatePlan(ctx, id, version); err != nil {
		slog.Error("plandist: activate error during promote", "id", id, "version", version, "error", err)
	}
}

func (en *ExecutionNode) joinRaftCluster(ctx context.Context) {
	raftAddr := fmt.Sprintf("localhost:%d", en.config.RaftPort)

	for _, seed := range en.config.Seeds {
		seedHTTP := seed
		if !strings.HasPrefix(seedHTTP, "http://") && !strings.HasPrefix(seedHTTP, "https://") {
			seedHTTP = "http://" + seedHTTP
		}
		seedURL := seedHTTP + "/cluster/join"
		body, _ := json.Marshal(map[string]string{
			"node_id":   en.nodeID,
			"raft_addr": raftAddr,
		})

		for i := 0; i < 30; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := en.httpClient.Post(seedURL, "application/json", bytes.NewReader(body))
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

func (en *ExecutionNode) RaftLeaderAddr() string {
	if en.RaftCluster == nil {
		return ""
	}
	return en.RaftCluster.LeaderAddr()
}
