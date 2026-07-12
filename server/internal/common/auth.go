package common

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"os"
)

// BearerAuth returns an HTTP middleware that validates Bearer tokens
// against the FLOWRULZ_API_KEY environment variable.
// If apiKeyEnv is empty, it defaults to "FLOWRULZ_API_KEY".
func BearerAuth(apiKeyEnv string) func(http.HandlerFunc) http.HandlerFunc {
	if apiKeyEnv == "" {
		apiKeyEnv = "FLOWRULZ_API_KEY" //nolint:gosec // env var name, not a credential
	}
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			apiKey := os.Getenv(apiKeyEnv)
			if apiKey == "" {
				slog.Warn("request rejected: no API key configured",
					"method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			key := r.Header.Get("Authorization")
			if subtle.ConstantTimeCompare([]byte(key), []byte("Bearer "+apiKey)) != 1 {
				slog.Warn("request rejected: invalid credentials",
					"method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
}
