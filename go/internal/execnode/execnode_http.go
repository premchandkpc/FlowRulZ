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

	"github.com/premchandkpc/FlowRulZ/go/internal/membership"
	"github.com/premchandkpc/FlowRulZ/go/internal/partition"
	"github.com/premchandkpc/FlowRulZ/go/internal/plandist"
	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
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

		// Publish own node info to members topic periodically for discovery
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

	kafkaCfg := transport.KafkaConfig{
		Brokers:    en.config.KafkaBrokers,
		GroupID:    en.config.KafkaGroupID,
		Acks:       transport.AcksLevelFromString(en.config.KafkaAcks),
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
	en.ReplyRouter.StartCleanup()
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
	mux.HandleFunc("/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		if en.RaftCluster == nil {
			http.Error(w, "raft not configured", http.StatusBadRequest)
			return
		}
		var req struct {
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := en.RaftCluster.Join(req.NodeID, req.RaftAddr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"status":    "ok",
			"node_id":   en.nodeID,
			"is_leader": en.IsLeader(),
			"term":      en.CurrentTerm(),
		}
		json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if en.IsLeader() && en.PlanDist.CurrentTerm() == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "reason": "leader not initialized"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := en.Metrics.Snapshot()
		snap.Gauges["pending_requests"] = int64(en.ReplyRouter.PendingCount())
		snap.Gauges["dlq_size"] = int64(en.DLQ.Len())
		snap.Gauges["inflight_execs"] = int64(en.Execs.Len())
		json.NewEncoder(w).Encode(snap)
	})
	mux.HandleFunc("DELETE /executions/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if en.Execs.Cancel(id) {
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{"status": "cancelling", "id": id})
		} else {
			http.Error(w, "execution not found", http.StatusNotFound)
		}
	})
	mux.HandleFunc("GET /executions", func(w http.ResponseWriter, r *http.Request) {
		execs := en.Execs.List()
		json.NewEncoder(w).Encode(execs)
	})
	mux.HandleFunc("GET /partitions", func(w http.ResponseWriter, r *http.Request) {
		assignments := en.Partitions.Assignments()
		nodeParts := make(map[string][]uint32)
		for _, n := range en.Membership.AliveNodes() {
			nodeParts[n] = en.Partitions.PartitionsForNode(n)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"num_partitions":  en.Partitions.NumPartitions(),
			"assignments":     assignments,
			"node_partitions": nodeParts,
		})
	})
	mux.HandleFunc("POST /partitions/rebalance", func(w http.ResponseWriter, r *http.Request) {
		if !en.IsLeader() {
			http.Error(w, "not leader", http.StatusForbidden)
			return
		}
		assignments := en.Partitions.Rebalance(en.Membership.AliveNodes(), en.PlanDist.CurrentTerm())
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := en.Partitions.PublishAssignments(ctx, assignments); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "rebalanced",
			"assignments": len(assignments),
		})
	})

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

func (en *ExecutionNode) Shutdown() {
	en.mu.Lock()
	defer en.mu.Unlock()

	slog.Info("shutdown: cancelling in-flight executions", "count", en.Execs.Len())
	en.Execs.CancelAll()

	for _, c := range en.consumers {
		c.Stop()
	}
	en.consumers = nil

	en.PlanDist.Stop()
	en.Scheduler.Stop()
	en.ReplyRouter.StopCleanup()

	for _, p := range en.producers {
		p.Close()
	}
	en.producers = nil

	if en.HTTP != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := en.HTTP.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown error", "error", err)
		}
	}

	if en.ClusterNode != nil {
		en.ClusterNode.Stop()
	}

	if en.GRPCBus != nil {
		en.GRPCBus.Stop()
	}

	if en.OtelExporter != nil {
		en.OtelExporter.Stop()
	}

	if en.RaftCluster != nil {
		en.RaftCluster.Stop()
	}

	if en.StateStore != nil {
		en.StateStore.Close()
	}

	close(en.shutdownCh)
	slog.Info("execnode: shutdown complete", "node_id", en.nodeID)
}
