package node

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

func (n *ProdNode) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/cluster/join", n.handleClusterJoin)
	mux.HandleFunc("/health", n.handleHealth)
	mux.HandleFunc("/readyz", n.handleReadyz)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("DELETE /executions/{id}", n.handleDeleteExecution)
	mux.HandleFunc("GET /executions", n.handleListExecutions)
	mux.HandleFunc("GET /partitions", n.handleListPartitions)
	mux.HandleFunc("POST /partitions/rebalance", n.handleRebalance)
}

func (n *ProdNode) handleClusterJoin(w http.ResponseWriter, r *http.Request) {
	if n.RaftCluster == nil {
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
	if err := n.RaftCluster.Join(req.NodeID, req.RaftAddr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
}

func (n *ProdNode) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"node_id":   n.nodeID,
		"is_leader": n.IsLeader(),
		"term":      n.CurrentTerm(),
	})
}

func (n *ProdNode) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if n.IsLeader() && n.PlanDist.CurrentTerm() == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "reason": "leader not initialized"})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (n *ProdNode) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := n.Metrics.Snapshot()
	snap.Gauges["pending_requests"] = int64(n.ReplyRouter.PendingCount())
	snap.Gauges["dlq_size"] = int64(n.DLQ.Len())
	snap.Gauges["inflight_execs"] = int64(n.Execs.Len())
	json.NewEncoder(w).Encode(snap)
}

func (n *ProdNode) handleDeleteExecution(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if n.Execs.Cancel(id) {
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelling", "id": id})
	} else {
		http.Error(w, "execution not found", http.StatusNotFound)
	}
}

func (n *ProdNode) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(n.Execs.List())
}

func (n *ProdNode) handleListPartitions(w http.ResponseWriter, r *http.Request) {
	assignments := n.Partitions.Assignments()
	nodeParts := make(map[string][]uint32)
	for _, nodeID := range n.Membership.AliveNodes() {
		nodeParts[nodeID] = n.Partitions.PartitionsForNode(nodeID)
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"num_partitions":  n.Partitions.NumPartitions(),
		"assignments":     assignments,
		"node_partitions": nodeParts,
	})
}

func (n *ProdNode) handleRebalance(w http.ResponseWriter, r *http.Request) {
	if !n.IsLeader() {
		http.Error(w, "not leader", http.StatusForbidden)
		return
	}
	assignments := n.Partitions.Rebalance(n.Membership.AliveNodes(), n.PlanDist.CurrentTerm())
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := n.Partitions.PublishAssignments(ctx, assignments); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "rebalanced",
		"assignments": len(assignments),
	})
}
