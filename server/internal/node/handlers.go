package node

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
)

func (n *ProdNode) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/cluster/join", n.requireClusterAuth(n.handleClusterJoin))
	mux.HandleFunc("/health", n.handleHealth)
	mux.HandleFunc("/readyz", n.handleReadyz)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("DELETE /executions/{id}", n.requireClusterAuth(n.handleDeleteExecution))
	mux.HandleFunc("GET /executions", n.requireClusterAuth(n.handleListExecutions))
	mux.HandleFunc("GET /partitions", n.requireClusterAuth(n.handleListPartitions))
	mux.HandleFunc("POST /partitions/rebalance", n.requireClusterAuth(n.handleRebalance))
}

func (n *ProdNode) requireClusterAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := os.Getenv("FLOWRULZ_API_KEY")
		if apiKey == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		key := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(key), []byte("Bearer "+apiKey)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (n *ProdNode) handleClusterJoin(w http.ResponseWriter, r *http.Request) {
	if n.RaftCluster == nil {
		http.Error(w, "raft not configured", http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Validate node_id is not empty and doesn't contain dangerous characters.
	if req.NodeID == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}
	if len(req.NodeID) > 128 {
		http.Error(w, "node_id too long", http.StatusBadRequest)
		return
	}

	// Validate raft_addr is not empty and looks like a valid address.
	if req.RaftAddr == "" {
		http.Error(w, "raft_addr is required", http.StatusBadRequest)
		return
	}
	host, port, err := net.SplitHostPort(req.RaftAddr)
	if err != nil || host == "" || port == "0" {
		http.Error(w, "invalid raft_addr: must be host:port", http.StatusBadRequest)
		return
	}

	// Reject localhost addresses in production (unless explicitly allowed).
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		slog.Warn("cluster join: rejecting localhost address",
			"node_id", req.NodeID, "raft_addr", req.RaftAddr,
			"hint", "set AdvertiseAddr to the pod's DNS name or external IP")
		http.Error(w, "localhost addresses not allowed; set AdvertiseAddr", http.StatusBadRequest)
		return
	}

	if err := n.RaftCluster.Join(pkgcluster.MemberID(req.NodeID), req.RaftAddr); err != nil {
		slog.Error("cluster join failed", "node_id", req.NodeID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
}

func (n *ProdNode) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	code := http.StatusOK

	if !n.IsLeader() && n.Membership.AliveCount() > 1 {
		// Follower node — healthy but not leader.
		status = "ok"
	}

	// Check if key subsystems are responsive.
	if n.Scheduler == nil {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       status,
		"node_id":      n.nodeID,
		"is_leader":    n.IsLeader(),
		"term":         n.CurrentTerm(),
		"alive_nodes":  n.Membership.AliveCount(),
		"inflight":     n.Execs.Len(),
		"dlq_size":     n.DLQ.Len(),
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
	nodeParts := make(map[string][]pkgpartition.PartitionID)
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
	// Fencing pattern: capture leadership token before deciding to act.
	token := n.CaptureLeadershipToken()
	if !token.Valid() {
		http.Error(w, "not leader", http.StatusForbidden)
		return
	}
	assignments := n.Partitions.Rebalance(n.Membership.AliveNodes(), token.Term)
	// Re-validate before the side-effecting publish.
	if !n.ValidateLeadershipToken(token) {
		http.Error(w, "leadership lost during rebalance", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := n.Partitions.PublishAssignments(ctx, assignments); err != nil {
		slog.Error("rebalance failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "rebalanced",
		"assignments": len(assignments),
	})
}
