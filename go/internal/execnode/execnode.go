package execnode

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/premchandkpc/FlowRulZ/go/internal/admin"
	"github.com/premchandkpc/FlowRulZ/go/internal/engine"
	"github.com/premchandkpc/FlowRulZ/go/internal/transport"
)

// ExecutionNode is a Data Plane process hosting the engine, transport consumers,
// and admin HTTP server. It manages graceful lifecycle: start consumers, serve
// HTTP, drain on shutdown.
type ExecutionNode struct {
	Engine     *engine.Engine
	AdminSrv   *admin.Server
	HTTP       *http.Server
	consumers  []*transport.Consumer
	httpAddr   string
	mu         sync.Mutex
	shutdownCh chan struct{}
}

type Config struct {
	PersistPath string
	HTTPAddr    string
	Topic       string
}

func NewConfig() *Config {
	return &Config{
		HTTPAddr: ":8080",
		Topic:    "flowrulz-input",
	}
}

func New(cfg *Config) *ExecutionNode {
	eng := engine.New(cfg.PersistPath)
	adminSrv := admin.New(eng)

	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", adminSrv.Handler()))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})

	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	return &ExecutionNode{
		Engine:     eng,
		AdminSrv:   adminSrv,
		HTTP:       httpSrv,
		consumers:  make([]*transport.Consumer, 0),
		httpAddr:   cfg.HTTPAddr,
		shutdownCh: make(chan struct{}),
	}
}

func (en *ExecutionNode) Start() {
	svcCaller := func(svcID uint16, body []byte) ([]byte, error) {
		log.Printf("service call: id=%d body=%s", svcID, body)
		return body, nil
	}

	handler := func(ctx context.Context, msg []byte) ([]byte, error) {
		results, err := en.Engine.ExecuteAll(msg, svcCaller)
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return nil, nil
		}
		return results[0], nil
	}

	en.mu.Lock()
	consumer := transport.NewConsumer("flowrulz-input", handler)
	en.consumers = append(en.consumers, consumer)
	en.mu.Unlock()

	go consumer.Start(context.Background())

	go func() {
		log.Printf("HTTP server on %s", en.httpAddr)
		if err := en.HTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down")
	en.Shutdown()
}

func (en *ExecutionNode) Shutdown() {
	en.mu.Lock()
	for _, c := range en.consumers {
		c.Stop()
	}
	en.consumers = nil
	en.mu.Unlock()

	if err := en.HTTP.Shutdown(context.Background()); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	close(en.shutdownCh)
}
