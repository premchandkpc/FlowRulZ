package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type HTTPConfig struct {
	Addr    string
	APIKey  string
	Handler MessageHandler
}

type HTTPTransport struct {
	cfg    HTTPConfig
	srv    *http.Server
	mu     sync.Mutex
	started bool
}

func NewHTTPTransport(cfg HTTPConfig) *HTTPTransport {
	return &HTTPTransport{cfg: cfg}
}

func (ht *HTTPTransport) Start(ctx context.Context) {
	ht.mu.Lock()
	if ht.started {
		ht.mu.Unlock()
		return
	}
	ht.started = true
	ht.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/event", ht.handleEvent)

	ht.srv = &http.Server{
		Addr:    ht.cfg.Addr,
		Handler: ht.auth(mux),
	}

	go func() {
		log.Printf("http transport on %s", ht.cfg.Addr)
		if err := ht.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http transport error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		ht.Stop()
	}()
}

func (ht *HTTPTransport) Stop() {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	if ht.srv != nil {
		ht.srv.Shutdown(context.Background())
	}
}

func (ht *HTTPTransport) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	if ht.cfg.Handler == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	resp, err := ht.cfg.Handler(r.Context(), body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(resp) > 0 {
		w.Write(resp)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (ht *HTTPTransport) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ht.cfg.APIKey != "" {
			if r.Header.Get("Authorization") != "Bearer "+ht.cfg.APIKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
