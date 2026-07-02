package execnode

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/net/context"

	pkgpartition "github.com/premchandkpc/FlowRulZ/go/pkg/partition"
)

func (en *ExecutionNode) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/cluster/join", en.handleClusterJoin)
	mux.HandleFunc("/health", en.handleHealth)
	mux.HandleFunc("/readyz", en.handleReadyz)
	mux.HandleFunc("/metrics", en.handleMetrics)
	mux.HandleFunc("DELETE /executions/{id}", en.handleDeleteExecution)
	mux.HandleFunc("GET /executions", en.handleListExecutions)
	mux.HandleFunc("GET /partitions", en.handleListPartitions)
	mux.HandleFunc("POST /partitions/rebalance", en.handleRebalance)
}

func (en *ExecutionNode) handleClusterJoin(w http.ResponseWriter, r *http.Request) {
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
}

func (en *ExecutionNode) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"status":    "ok",
		"node_id":   en.nodeID,
		"is_leader": en.IsLeader(),
		"term":      en.CurrentTerm(),
	}
	json.NewEncoder(w).Encode(status)
}

func (en *ExecutionNode) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if en.IsLeader() && en.PlanDist.CurrentTerm() == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "reason": "leader not initialized"})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (en *ExecutionNode) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := en.Metrics.Snapshot()
	snap.Gauges["pending_requests"] = int64(en.ReplyRouter.PendingCount())
	snap.Gauges["dlq_size"] = int64(en.DLQ.Len())
	snap.Gauges["inflight_execs"] = int64(en.Execs.Len())
	json.NewEncoder(w).Encode(snap)
}

func (en *ExecutionNode) handleDeleteExecution(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if en.Execs.Cancel(id) {
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelling", "id": id})
	} else {
		http.Error(w, "execution not found", http.StatusNotFound)
	}
}

func (en *ExecutionNode) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	execs := en.Execs.List()
	json.NewEncoder(w).Encode(execs)
}

func (en *ExecutionNode) handleListPartitions(w http.ResponseWriter, r *http.Request) {
	assignments := en.Partitions.Assignments()
	nodeParts := make(map[string][]pkgpartition.PartitionID)
	for _, n := range en.Membership.AliveNodes() {
		nodeParts[n] = en.Partitions.PartitionsForNode(n)
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"num_partitions":  en.Partitions.NumPartitions(),
		"assignments":     assignments,
		"node_partitions": nodeParts,
	})
}

func (en *ExecutionNode) handleRebalance(w http.ResponseWriter, r *http.Request) {
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
