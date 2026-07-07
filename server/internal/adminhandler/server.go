// Package adminhandler provides the HTTP admin server for node management.
package adminhandler

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
)

// AdminHandler provides the rule management HTTP handler.
type AdminHandler interface {
	Handler() http.Handler
}

// HTTPRegistry provides HTTP handlers for service registration.
type HTTPRegistry interface {
	RegisterHTTPHandler(w http.ResponseWriter, r *http.Request)
	HeartbeatHTTPHandler(w http.ResponseWriter, r *http.Request)
	ListServicesHTTPHandler(w http.ResponseWriter, r *http.Request)
}

// LeadershipProvider abstracts leadership queries.
type LeadershipProvider interface {
	IsLeader() bool
	CurrentTerm() uint64
	CaptureLeadershipToken() ports.LeadershipToken
	ValidateLeadershipToken(token ports.LeadershipToken) bool
}

// PlanDistancer provides the current term for readiness checks.
type PlanDistancer interface {
	CurrentTerm() uint64
}

// ReplyRouter provides pending count for metrics.
type ReplyRouter interface {
	PendingCount() int
}

// MembershipProvider provides alive nodes for partition listing.
type MembershipProvider interface {
	AliveNodes() []string
}

// DeadLetterQueue provides DLQ size for metrics.
type DeadLetterQueue interface {
	Len() int
}

// Server handles HTTP API handlers and server lifecycle.
type Server struct {
	httpAddr   string
	nodeID     string
	httpServer *http.Server

	adminSrv   AdminHandler
	registry   HTTPRegistry
	leadership LeadershipProvider
	execs      execstate.ExecRegistry
	partitions pkgpartition.PartitionManager
	membership MembershipProvider
	metrics    ports.MetricsCollector
	dlq        DeadLetterQueue
	planDist   PlanDistancer
	replyRouter ReplyRouter
	raftCluster pkgcluster.ClusterMember
}

// NewServer creates an admin HTTP server with the given dependencies.
func NewServer(
	httpAddr string,
	nodeID string,
	adminSrv AdminHandler,
	registry HTTPRegistry,
	leadership LeadershipProvider,
	execs execstate.ExecRegistry,
	partitions pkgpartition.PartitionManager,
	membership MembershipProvider,
	metrics ports.MetricsCollector,
	dlq DeadLetterQueue,
	planDist PlanDistancer,
	replyRouter ReplyRouter,
	raftCluster pkgcluster.ClusterMember,
) *Server {
	return &Server{
		httpAddr:    httpAddr,
		nodeID:      nodeID,
		adminSrv:    adminSrv,
		registry:    registry,
		leadership:  leadership,
		execs:       execs,
		partitions:  partitions,
		membership:  membership,
		metrics:     metrics,
		dlq:         dlq,
		planDist:    planDist,
		replyRouter: replyRouter,
		raftCluster: raftCluster,
	}
}

// Serve starts the HTTP server.
func (s *Server) Serve(ctx context.Context) {
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", s.adminSrv.Handler()))
	mux.HandleFunc("/register", s.registry.RegisterHTTPHandler)
	mux.HandleFunc("/heartbeat", s.registry.HeartbeatHTTPHandler)
	mux.HandleFunc("/services", s.registry.ListServicesHTTPHandler)
	s.registerHandlers(mux)

	s.httpServer = &http.Server{Addr: s.httpAddr, Handler: mux}
	go func() {
		slog.Info("HTTP server started", "addr", s.httpAddr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	}
	return nil
}

func (s *Server) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/cluster/join", s.requireClusterAuth(s.handleClusterJoin))
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("DELETE /executions/{id}", s.handleDeleteExecution)
	mux.HandleFunc("GET /executions", s.handleListExecutions)
	mux.HandleFunc("GET /partitions", s.handleListPartitions)
	mux.HandleFunc("POST /partitions/rebalance", s.handleRebalance)
}

func (s *Server) requireClusterAuth(next http.HandlerFunc) http.HandlerFunc {
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

func (s *Server) handleClusterJoin(w http.ResponseWriter, r *http.Request) {
	if s.raftCluster == nil {
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

	if req.NodeID == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}
	if len(req.NodeID) > 128 {
		http.Error(w, "node_id too long", http.StatusBadRequest)
		return
	}

	if req.RaftAddr == "" {
		http.Error(w, "raft_addr is required", http.StatusBadRequest)
		return
	}
	host, port, err := net.SplitHostPort(req.RaftAddr)
	if err != nil || host == "" || port == "0" {
		http.Error(w, "invalid raft_addr: must be host:port", http.StatusBadRequest)
		return
	}

	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		slog.Warn("cluster join: rejecting localhost address",
			"node_id", req.NodeID, "raft_addr", req.RaftAddr,
			"hint", "set AdvertiseAddr to the pod's DNS name or external IP")
		http.Error(w, "localhost addresses not allowed; set AdvertiseAddr", http.StatusBadRequest)
		return
	}

	if err := s.raftCluster.Join(pkgcluster.MemberID(req.NodeID), req.RaftAddr); err != nil {
		slog.Error("cluster join failed", "node_id", req.NodeID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"node_id":   s.nodeID,
		"is_leader": s.leadership.IsLeader(),
		"term":      s.leadership.CurrentTerm(),
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.leadership.IsLeader() && s.planDist.CurrentTerm() == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "reason": "leader not initialized"})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := s.metrics.Snapshot()
	snap.Gauges["pending_requests"] = int64(s.replyRouter.PendingCount())
	snap.Gauges["dlq_size"] = int64(s.dlq.Len())
	snap.Gauges["inflight_execs"] = int64(s.execs.Len())
	json.NewEncoder(w).Encode(snap)
}

func (s *Server) handleDeleteExecution(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.execs.Cancel(id) {
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelling", "id": id})
	} else {
		http.Error(w, "execution not found", http.StatusNotFound)
	}
}

func (s *Server) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(s.execs.List())
}

func (s *Server) handleListPartitions(w http.ResponseWriter, r *http.Request) {
	assignments := s.partitions.Assignments()
	nodeParts := make(map[string][]pkgpartition.PartitionID)
	for _, nodeID := range s.membership.AliveNodes() {
		nodeParts[nodeID] = s.partitions.PartitionsForNode(nodeID)
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"num_partitions":  s.partitions.NumPartitions(),
		"assignments":     assignments,
		"node_partitions": nodeParts,
	})
}

func (s *Server) handleRebalance(w http.ResponseWriter, r *http.Request) {
	token := s.leadership.CaptureLeadershipToken()
	if !token.Valid {
		http.Error(w, "not leader", http.StatusForbidden)
		return
	}
	assignments := s.partitions.Rebalance(s.membership.AliveNodes(), token.Term)
	if !s.leadership.ValidateLeadershipToken(token) {
		http.Error(w, "leadership lost during rebalance", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := s.partitions.PublishAssignments(ctx, assignments); err != nil {
		slog.Error("rebalance failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "rebalanced",
		"assignments": len(assignments),
	})
}
