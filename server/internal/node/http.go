package node

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"time"
)

var tlsCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
}

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

	n.httpServer = &http.Server{
		Addr:              n.httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if n.config.HasTLS() {
			tlsCert, err := tls.LoadX509KeyPair(n.config.TLSCertFile, n.config.TLSKeyFile)
			if err != nil {
				slog.Error("TLS cert load failed, falling back to plaintext", "error", err)
				n.httpServer.TLSConfig = nil
			} else {
				n.httpServer.TLSConfig = &tls.Config{
					Certificates: []tls.Certificate{tlsCert},
					CipherSuites: tlsCipherSuites,
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
