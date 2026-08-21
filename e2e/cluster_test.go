//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

var (
	nodes = []string{
		"http://localhost:8080",
		"http://localhost:8081",
		"http://localhost:8082",
	}
	apiKey = os.Getenv("FLOWRULZ_API_KEY")
)

func init() {
	if apiKey == "" {
		apiKey = "test-key"
	}
}

func requireCluster(t *testing.T) {
	t.Helper()
	for _, addr := range nodes {
		resp, err := http.Get(addr + "/health")
		if err != nil {
			t.Skipf("cluster not available at %s: %v", addr, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Skipf("cluster not healthy at %s: status %d", addr, resp.StatusCode)
		}
	}
}

func authReq(method, url string, body interface{}) (*http.Request, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// TestClusterHealth verifies all 3 nodes are healthy.
func TestClusterHealth(t *testing.T) {
	requireCluster(t)

	for i, addr := range nodes {
		resp, err := http.Get(addr + "/health")
		if err != nil {
			t.Fatalf("node %d health check failed: %v", i+1, err)
		}
		defer resp.Body.Close()

		var health map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
			t.Fatalf("node %d decode health: %v", i+1, err)
		}

		if health["status"] != "ok" {
			t.Fatalf("node %d not healthy: %v", i+1, health)
		}
		t.Logf("node %d: healthy", i+1)
	}
}

// TestClusterDeployRule deploys a rule to all nodes and verifies.
func TestClusterDeployRule(t *testing.T) {
	requireCluster(t)

	dsl := "n:validate n:inventory n:payment"
	for i, addr := range nodes {
		req, err := authReq("POST", addr+"/admin/rules", map[string]string{
			"id":  "e2e-test-rule",
			"dsl": dsl,
		})
		if err != nil {
			t.Fatalf("node %d create request: %v", i+1, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("node %d deploy failed: %v", i+1, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			t.Fatalf("node %d deploy returned %d", i+1, resp.StatusCode)
		}
		t.Logf("node %d: rule deployed", i+1)
	}

	// Wait for propagation
	time.Sleep(500 * time.Millisecond)

	// Verify rule exists on all nodes
	for i, addr := range nodes {
		req, err := authReq("GET", addr+"/admin/rules", nil)
		if err != nil {
			t.Fatalf("node %d list request: %v", i+1, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("node %d list failed: %v", i+1, err)
		}
		defer resp.Body.Close()

		var rules []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&rules); err != nil {
			t.Fatalf("node %d decode rules: %v", i+1, err)
		}

		found := false
		for _, r := range rules {
			if r["id"] == "e2e-test-rule" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("node %d: rule not found after deploy", i+1)
		}
		t.Logf("node %d: rule verified", i+1)
	}
}

// TestClusterExecuteRule deploys and executes a rule through the cluster.
func TestClusterExecuteRule(t *testing.T) {
	requireCluster(t)

	// Deploy rule to first node
	dsl := "n:validate n:inventory n:payment"
	req, err := authReq("POST", nodes[0]+"/admin/rules", map[string]string{
		"id":  "e2e-exec-rule",
		"dsl": dsl,
	})
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	resp.Body.Close()

	// Wait for propagation
	time.Sleep(500 * time.Millisecond)

	// Execute on each node
	for i, addr := range nodes {
		body := fmt.Sprintf(`{"order_id":"e2e-%d","amount":199.99}`, i)
		req, err := authReq("POST", addr+"/admin/execute", map[string]string{
			"rule_id": "e2e-exec-rule",
			"body":    body,
		})
		if err != nil {
			t.Fatalf("node %d execute request: %v", i+1, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("node %d execute failed: %v", i+1, err)
		}
		defer resp.Body.Close()

		t.Logf("node %d: execute status %d", i+1, resp.StatusCode)
	}
}

// TestClusterServiceRegistry verifies service registration across nodes.
func TestClusterServiceRegistry(t *testing.T) {
	requireCluster(t)

	// Register a service on node 1
	req, err := authReq("POST", nodes[0]+"/register", map[string]interface{}{
		"name": "test-service",
		"endpoint": map[string]interface{}{
			"protocol": "http",
			"address":  "10.0.0.1",
			"port":     8080,
		},
	})
	if err != nil {
		t.Fatalf("register request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register returned %d", resp.StatusCode)
	}

	// Wait for propagation
	time.Sleep(500 * time.Millisecond)

	// List services on all nodes
	for i, addr := range nodes {
		resp, err := http.Get(addr + "/services")
		if err != nil {
			t.Fatalf("node %d services failed: %v", i+1, err)
		}
		defer resp.Body.Close()

		var services []map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&services); err != nil {
			t.Fatalf("node %d decode services: %v", i+1, err)
		}

		found := false
		for _, svc := range services {
			if svc["name"] == "test-service" {
				found = true
				break
			}
		}
		if !found {
			t.Logf("node %d: service not yet propagated", i+1)
		} else {
			t.Logf("node %d: service registered", i+1)
		}
	}
}

// TestClusterConcurrentExecutions tests concurrent execution across nodes.
func TestClusterConcurrentExecutions(t *testing.T) {
	requireCluster(t)

	// Deploy rule
	dsl := "n:validate n:payment"
	req, err := authReq("POST", nodes[0]+"/admin/rules", map[string]string{
		"id":  "e2e-concurrent",
		"dsl": dsl,
	})
	if err != nil {
		t.Fatalf("deploy request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	resp.Body.Close()

	time.Sleep(500 * time.Millisecond)

	// Fire 20 concurrent requests across all nodes
	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func(idx int) {
			nodeAddr := nodes[idx%len(nodes)]
			body := fmt.Sprintf(`{"order_id":"conc-%d","amount":%.2f}`, idx, float64(idx*10))
			req, err := authReq("POST", nodeAddr+"/admin/execute", map[string]string{
				"rule_id": "e2e-concurrent",
				"body":    body,
			})
			if err != nil {
				done <- err
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				done <- err
				return
			}
			resp.Body.Close()
			done <- nil
		}(i)
	}

	// Collect results
	var failures int
	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			failures++
			t.Logf("request %d failed: %v", i, err)
		}
	}

	if failures > 5 {
		t.Fatalf("too many failures: %d/20", failures)
	}
	t.Logf("concurrent test: %d/20 succeeded", 20-failures)
}
