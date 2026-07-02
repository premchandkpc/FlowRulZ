package execnode

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/membership"
	"github.com/premchandkpc/FlowRulZ/server/internal/partition"
	"github.com/premchandkpc/FlowRulZ/server/internal/plandist"
	kafkatransport "github.com/premchandkpc/FlowRulZ/server/internal/transport/kafka"
)

func (en *ExecutionNode) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := en.handleIncomingMessage

	if en.ClusterNode != nil {
		if err := en.ClusterNode.Start(); err != nil {
			slog.Error("cluster: start error", "error", err)
		}

		en.ClusterNode.Gossiper().OnNodeJoin(func(nodeID, address string) {
			en.Membership.Heartbeat(nodeID, address)
			if address != "" && nodeID != en.nodeID {
				if err := en.ClusterNode.AddPeer(nodeID, address); err != nil {
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
						NodeID:  en.nodeID,
						Address: en.config.GRPCAddr,
					})
					en.ClusterNode.Publish(DefaultMembersTopic, en.nodeID, discMsg)
				case <-ctx.Done():
					return
				}
			}
		}()

		for _, seedAddr := range en.config.Seeds {
			if seedAddr == en.config.GRPCAddr {
				continue
			}
			seedID := fmt.Sprintf("seed-%s", seedAddr)
			if err := en.ClusterNode.AddPeer(seedID, seedAddr); err != nil {
				slog.Error("cluster: connect to seed", "seed_addr", seedAddr, "error", err)
			}
		}
	}

	kafkaCfg := kafkatransport.Config{
		Brokers:    en.config.KafkaBrokers,
		GroupID:    en.config.KafkaGroupID,
		Acks:       kafkatransport.AcksLevelFromString(en.config.KafkaAcks),
		Idempotent: en.config.KafkaIdempotent,
	}
	inputConsumer := en.mkConsumer(en.config.Topic, handler, kafkaCfg)
	membersConsumer := en.mkConsumer(DefaultMembersTopic, en.handleNodeDiscoveryMessage, kafkaCfg)
	planConsumer := en.mkConsumer(plandist.DefaultPlanTopic, en.handlePlanMessage, kafkaCfg)
	ackConsumer := en.mkConsumer(plandist.DefaultAckTopic, en.handleAckMessage, kafkaCfg)
	partConsumer := en.mkConsumer(partition.PartitionTopic, en.handlePartitionMessage, kafkaCfg)
	en.mu.Lock()
	en.consumers = append(en.consumers, inputConsumer, membersConsumer, planConsumer, ackConsumer, partConsumer)
	en.mu.Unlock()
	go inputConsumer.Start(ctx)
	go membersConsumer.Start(ctx)
	go planConsumer.Start(ctx)
	go ackConsumer.Start(ctx)
	go partConsumer.Start(ctx)

	en.PlanDist.Start(ctx)
	en.Membership.StartEviction(ctx, membership.DefaultHeartbeatTimeout)

	en.Rebalancer.SetNotify(func() {
		if !en.IsLeader() {
			return
		}
		assignments := en.Partitions.Rebalance(en.Membership.AliveNodes(), en.PlanDist.CurrentTerm())
		if len(assignments) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := en.Partitions.PublishAssignments(ctx, assignments); err != nil {
				slog.Error("partition: publish assignments error", "error", err)
			}
		}
	})

	if en.RaftCluster != nil {
		if err := en.RaftCluster.Start(); err != nil {
			log.Fatalf("raft: start error: %v", err)
		}
		if en.config.RaftBootstrap {
			if err := en.RaftCluster.BootstrapCluster(); err != nil {
				slog.Warn("raft: bootstrap", "error", err)
			}
		}
		en.RaftCluster.SubscribeLeaderChanges(func(isLeader bool) {
			if isLeader {
				term := en.RaftCluster.CurrentTerm()
				en.PlanDist.SetTerm(term)
				en.Partitions.OnLeaderChange(en.nodeID)
				en.Rebalancer.CheckAndRebalance()
				slog.Info("raft: node became leader", "node_id", en.nodeID, "term", term)
			} else {
				leaderAddr := en.RaftCluster.LeaderAddr()
				slog.Info("raft: node stepped down", "node_id", en.nodeID, "leader_addr", leaderAddr)
				en.Partitions.OnLeaderChange("")
			}
		})
		if !en.config.RaftBootstrap && len(en.config.Seeds) > 0 {
			go en.joinRaftCluster(ctx)
		}
	}

	en.Scheduler.Start(ctx)
	en.ReplyRouter.StartCleanup(ctx)
	en.Dedup.StartCleanup(ctx, 30*time.Second)

	en.recoverInFlight(ctx)

	if en.GRPCBus != nil {
		if err := en.GRPCBus.Start(); err != nil {
			slog.Error("grpc: start error", "error", err)
		}
	}

	if en.OtelExporter != nil {
		go en.OtelExporter.Start(ctx)
	}

	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", en.AdminSrv.Handler()))
	mux.HandleFunc("/register", en.Registry.RegisterHTTPHandler)
	mux.HandleFunc("/heartbeat", en.Registry.HeartbeatHTTPHandler)
	mux.HandleFunc("/services", en.Registry.ListServicesHTTPHandler)
	en.registerHandlers(mux)

	en.HTTP = &http.Server{Addr: en.httpAddr, Handler: mux}
	go func() {
		slog.Info("HTTP server started", "addr", en.httpAddr)
		if err := en.HTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	slog.Info("execnode started", "node_id", en.nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("shutting down")
	en.Shutdown()
}
