package node

import (
	"context"
	"crypto/tls"
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

	// Wire extended admin endpoints
	n.AdminSrv.RegisterExtended(n.nodeID, func() interface{} {
		return n.Scheduler.Snapshot()
	}, func(ctx context.Context) {
		n.recoverInFlight(ctx)
	})

	n.httpServer = &http.Server{Addr: n.httpAddr, Handler: mux}

	go func() {
		if n.config.HasTLS() {
			tlsCert, err := tls.LoadX509KeyPair(n.config.TLSCertFile, n.config.TLSKeyFile)
			if err != nil {
				slog.Error("TLS cert load failed, falling back to plaintext", "error", err)
				n.httpServer.TLSConfig = nil
			} else {
				n.httpServer.TLSConfig = &tls.Config{
					Certificates: []tls.Certificate{tlsCert},
					MinVersion:   tls.VersionTLS12,
				}
				slog.Info("HTTP server started with TLS", "addr", n.httpAddr)
				if err := n.httpServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
					slog.Error("http server error", "error", err)
				}
				return
			}
		}
		slog.Info("HTTP server started (plaintext)", "addr", n.httpAddr)
		if err := n.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()
}
