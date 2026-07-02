package main

import (
	"log"
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

	log.Printf("compiler service on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("compiler: %v", err)
	}
}
