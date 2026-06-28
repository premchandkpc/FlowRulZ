package execnode

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/internal/admin"
	"github.com/premchandkpc/FlowRulZ/go/internal/engine"
	"github.com/premchandkpc/FlowRulZ/go/internal/observability"
	"github.com/premchandkpc/FlowRulZ/go/internal/reliability"
	"github.com/premchandkpc/FlowRulZ/go/internal/replyrouter"
	"github.com/premchandkpc/FlowRulZ/go/internal/scheduler"
	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

type ExecutionNode struct {
	Engine       *engine.Engine
	AdminSrv     *admin.Server
	HTTP         *http.Server
	Scheduler    *scheduler.Scheduler
	ReplyRouter  *replyrouter.ReplyRouter
	DLQ          *reliability.DLQ
	RateLimiter  *reliability.RateLimiter
	Metrics      *observability.MetricsCollector

	consumers  []transport.MessageConsumer
	producers  []transport.MessageProducer
	httpAddr   string
	nodeID     string
	mu         sync.Mutex
	shutdownCh chan struct{}
}

type Config struct {
	PersistPath  string
	HTTPAddr     string
	Topic        string
	NodeID       string
	Seeds        []string
	KafkaBrokers []string
	APIKey       string
}

func NewConfig() *Config {
	return &Config{
		HTTPAddr:     ":8080",
		Topic:        "flowrulz-input",
		NodeID:       "node-1",
		KafkaBrokers: []string{"localhost:9092"},
	}
}

func New(cfg *Config) *ExecutionNode {
	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = "node-1"
	}

	en := &ExecutionNode{
		httpAddr:   cfg.HTTPAddr,
		nodeID:     nodeID,
		shutdownCh: make(chan struct{}),
		consumers:  make([]transport.MessageConsumer, 0),
		producers:  make([]transport.MessageProducer, 0),
	}

	en.Engine = engine.New(cfg.PersistPath)
	en.Metrics = observability.NewMetricsCollector()
	en.Scheduler = scheduler.New(nil)
	en.ReplyRouter = replyrouter.New(
		replyrouter.WithCleanupInterval(1 * time.Second),
		replyrouter.WithMaxPending(10000),
	)
	en.DLQ = reliability.NewDLQ(10000)
	en.RateLimiter = reliability.NewRateLimiter()
	en.AdminSrv = admin.New(en.Engine)
	en.AdminSrv.RegisterDLQ(en.DLQ)

	return en
}

func (en *ExecutionNode) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svcCaller := func(svcID uint16, body []byte) ([]byte, error) {
		observability.RecordExec("svc_call")
		log.Printf("service call: id=%d body=%s", svcID, body)
		return body, nil
	}

	handler := func(ctx context.Context, msg []byte) ([]byte, error) {
		if !en.RateLimiter.Allow("ingress") {
			observability.RecordError("rate_limited")
			en.DLQ.Send(&reliability.DeadLetterEntry{
				ID:    "ratelimited",
				Body:  msg,
				Error: "rate limited",
			})
			return nil, nil
		}

		results, err := en.Engine.ExecuteAll(msg, svcCaller)
		if err != nil {
			observability.RecordError("exec")
			en.DLQ.Send(&reliability.DeadLetterEntry{
				ID:    "exec-error",
				Body:  msg,
				Error: err.Error(),
			})
			return nil, err
		}
		observability.RecordExec("msg")
		return results[0], nil
	}

	consumer := transport.NewConsumer("flowrulz-input", handler)
	en.mu.Lock()
	en.consumers = append(en.consumers, consumer)
	en.mu.Unlock()
	go consumer.Start(ctx)

	en.Scheduler.Start(ctx)
	en.ReplyRouter.StartCleanup()

	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", en.AdminSrv.Handler()))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := en.Metrics.Snapshot()
		snap.Gauges["pending_requests"] = int64(en.ReplyRouter.PendingCount())
		snap.Gauges["dlq_size"] = int64(en.DLQ.Len())
		json.NewEncoder(w).Encode(snap)
	})

	en.HTTP = &http.Server{Addr: en.httpAddr, Handler: mux}
	go func() {
		log.Printf("HTTP server on %s", en.httpAddr)
		if err := en.HTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	log.Printf("execnode %s: started", en.nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down")
	en.Shutdown()
}

func (en *ExecutionNode) Shutdown() {
	en.mu.Lock()
	defer en.mu.Unlock()

	for _, c := range en.consumers {
		c.Stop()
	}
	en.consumers = nil

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
			log.Printf("http shutdown error: %v", err)
		}
	}

	close(en.shutdownCh)
	log.Printf("execnode %s: shutdown complete", en.nodeID)
}


