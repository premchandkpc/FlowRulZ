package node

import (
	"context"
	"log/slog"
	"net/http"
)

func (n *ProdNode) serveHTTP(ctx context.Context) {
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", n.AdminSrv.Handler()))
	mux.HandleFunc("/register", n.Registry.RegisterHTTPHandler)
	mux.HandleFunc("/heartbeat", n.Registry.HeartbeatHTTPHandler)
	mux.HandleFunc("/services", n.Registry.ListServicesHTTPHandler)
	n.registerHandlers(mux)

	n.httpServer = &http.Server{Addr: n.httpAddr, Handler: mux}
	go func() {
		slog.Info("HTTP server started", "addr", n.httpAddr)
		if err := n.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()
}
