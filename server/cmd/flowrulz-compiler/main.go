package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/premchandkpc/FlowRulZ/server/internal/compiler"
)

func main() {
	addr := os.Getenv("FLOWRULZ_COMPILER_ADDR")
	if addr == "" {
		addr = ":9090"
	}

	handler := compiler.NewCompileHandler()

	mux := http.NewServeMux()
	mux.HandleFunc("/compile", handler.HandleCompile)
	mux.HandleFunc("/validate", handler.HandleValidate)

	slog.Info("compiler service", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("compiler: server error", "error", err)
		os.Exit(1)
	}
}
