package registry

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type RegisterRequest struct {
	ID           string              `json:"id,omitempty"`
	Name         string              `json:"name"`
	Version      string              `json:"version,omitempty"`
	Methods      []MethodInfo        `json:"methods,omitempty"`
	Capabilities ServiceCapabilities `json:"capabilities"`
	Address      string              `json:"address"`
	Port         int                 `json:"port"`
	Protocol     Protocol            `json:"protocol,omitempty"`
	Zone         string              `json:"zone,omitempty"`
	Weight       int                 `json:"weight,omitempty"`
	Tags         []string            `json:"tags,omitempty"`
	Metadata     map[string]string   `json:"metadata,omitempty"`
	NodeID       string              `json:"node_id,omitempty"`
}

type HeartbeatRequest struct {
	Name       string `json:"name"`
	InstanceID string `json:"instance_id"`
}

const maxRequestBodySize = 1 << 20

func (r *ServiceRegistry) checkAuth(req *http.Request) bool {
	apiKey := os.Getenv("FLOWRULZ_API_KEY")
	if apiKey == "" {
		return false
	}
	key := req.Header.Get("Authorization")
	return subtle.ConstantTimeCompare([]byte(key), []byte("Bearer "+apiKey)) == 1
}

func (r *ServiceRegistry) RegisterHTTPHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	if !r.checkAuth(req) {
		http.Error(w, "unauthorized", 401)
		return
	}

	req.Body = http.MaxBytesReader(w, req.Body, maxRequestBodySize)
	var regReq RegisterRequest
	if err := json.NewDecoder(req.Body).Decode(&regReq); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	if regReq.Name == "" {
		http.Error(w, "name required", 400)
		return
	}
	if regReq.Address == "" {
		http.Error(w, "address required", 400)
		return
	}
	if regReq.Port <= 0 {
		http.Error(w, "valid port required", 400)
		return
	}
	if regReq.Protocol == "" {
		regReq.Protocol = ProtocolHTTP
	}

	inst := &ServiceInstance{
		ID:           regReq.ID,
		Name:         regReq.Name,
		Version:      regReq.Version,
		Methods:      regReq.Methods,
		Capabilities: regReq.Capabilities,
		Endpoint: Endpoint{
			NodeID:   regReq.NodeID,
			Address:  regReq.Address,
			Port:     regReq.Port,
			Protocol: regReq.Protocol,
		},
		Zone:         regReq.Zone,
		Weight:       regReq.Weight,
		Tags:         regReq.Tags,
		Metadata:     regReq.Metadata,
		Healthy:      true,
		HeartbeatAt:  time.Now(),
		RegisteredAt: time.Now(),
	}

	if err := r.RegisterInstance(inst); err != nil {
		slog.Error("registry: register failed", "name", inst.Name, "error", err)
		http.Error(w, "internal error", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":      "registered",
		"name":        inst.Name,
		"instance_id": inst.ID,
	})
}

func (r *ServiceRegistry) HeartbeatHTTPHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	if !r.checkAuth(req) {
		http.Error(w, "unauthorized", 401)
		return
	}

	req.Body = http.MaxBytesReader(w, req.Body, maxRequestBodySize)
	var hbReq HeartbeatRequest
	if err := json.NewDecoder(req.Body).Decode(&hbReq); err != nil {
		http.Error(w, "bad request", 400)
		return
	}
	if hbReq.Name == "" || hbReq.InstanceID == "" {
		http.Error(w, "name and instance_id required", 400)
		return
	}

	if err := r.Heartbeat(hbReq.Name, hbReq.InstanceID); err != nil {
		http.Error(w, "not found", 404)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (r *ServiceRegistry) StartHeartbeatChecker(stopCh <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				expired := r.CheckExpired()
				for _, e := range expired {
					slog.Warn("registry: instance expired", "id", e)
				}
			case <-stopCh:
				return
			}
		}
	}()
}

func (r *ServiceRegistry) ListServicesHTTPHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"services": r.AllServiceInfo(),
	})
}
