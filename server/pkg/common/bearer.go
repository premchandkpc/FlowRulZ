package common

import (
	"crypto/subtle"
	"net/http"
	"os"
	"sync"
)

// BearerAuth provides HTTP Bearer token authentication.
type BearerAuth struct {
	mu     sync.Once
	apiKey string
}

// NewBearerAuth creates a BearerAuth that reads the API key from
// FLOWRULZ_API_KEY env var. If the env var is empty, auth is disabled.
func NewBearerAuth() *BearerAuth {
	return &BearerAuth{}
}

func (a *BearerAuth) loadKey() {
	a.mu.Do(func() {
		a.apiKey = os.Getenv("FLOWRULZ_API_KEY")
	})
}

// Check returns true if the request has a valid Bearer token.
// Returns true (allow) if no API key is configured (open mode).
func (a *BearerAuth) Check(r *http.Request) bool {
	a.loadKey()
	if a.apiKey == "" {
		return true
	}
	key := r.Header.Get("Authorization")
	return subtle.ConstantTimeCompare([]byte(key), []byte("Bearer "+a.apiKey)) == 1
}

// Require wraps an http.HandlerFunc with Bearer auth.
// Returns 401 if authentication fails.
func (a *BearerAuth) Require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.Check(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
