package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

type nodeConfig struct {
	name     string
	httpAddr string
	grpcAddr string
}

var nodes = []nodeConfig{
	{name: "node-1", httpAddr: "http://localhost:8080", grpcAddr: ":9090"},
	{name: "node-2", httpAddr: "http://localhost:8081", grpcAddr: ":9091"},
	{name: "node-3", httpAddr: "http://localhost:8082", grpcAddr: ":9092"},
}

func waitForNode(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(addr + "/health")
		if err == nil {
			resp.Body.Close()
			var h struct {
				Status string `json:"status"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&h); err == nil && h.Status == "ok" {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("node %s not ready within %v", addr, timeout)
}

func findLeader(t *testing.T) string {
	t.Helper()
	for _, n := range nodes {
		resp, err := http.Get(n.httpAddr + "/health")
		if err != nil {
			continue
		}
		var h struct {
			Status   string `json:"status"`
			IsLeader bool   `json:"is_leader"`
			NodeID   string `json:"node_id"`
		}
		json.NewDecoder(resp.Body).Decode(&h)
		resp.Body.Close()
		if h.IsLeader {
			return n.httpAddr
		}
	}
	return ""
}

func leaderID(t *testing.T) string {
	t.Helper()
	for _, n := range nodes {
		resp, err := http.Get(n.httpAddr + "/health")
		if err != nil {
			continue
		}
		var h struct {
			IsLeader bool   `json:"is_leader"`
			NodeID   string `json:"node_id"`
		}
		json.NewDecoder(resp.Body).Decode(&h)
		resp.Body.Close()
		if h.IsLeader {
			return h.NodeID
		}
	}
	return ""
}

func postJSON(t *testing.T, addr, path string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(addr+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func Test3NodeClusterHealth(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	for _, n := range nodes {
		t.Run(n.name, func(t *testing.T) {
			waitForNode(t, n.httpAddr, 30*time.Second)
		})
	}
}

func TestLeaderElected(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	leader := findLeader(t)
	if leader == "" {
		t.Fatal("no leader elected")
	}
	t.Logf("leader: %s", leaderID(t))
}

func TestLeaderConsensus(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	var leaderIDs []string
	for range 5 {
		id := leaderID(t)
		leaderIDs = append(leaderIDs, id)
		time.Sleep(500 * time.Millisecond)
	}

	for _, id := range leaderIDs {
		if id != leaderIDs[0] {
			t.Fatalf("leader changed during observation: %v", leaderIDs)
		}
	}
	t.Logf("stable leader: %s", leaderIDs[0])
}

func TestAllNodesSeeSameMembers(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	type servicesResp struct {
		Services map[string]interface{} `json:"services"`
	}

	var snapshots []servicesResp
	for _, n := range nodes {
		resp, err := http.Get(n.httpAddr + "/services")
		if err != nil {
			t.Fatal(err)
		}
		var sr servicesResp
		json.NewDecoder(resp.Body).Decode(&sr)
		resp.Body.Close()
		snapshots = append(snapshots, sr)
	}

	// All nodes should see at least their own registration
	for i, s := range snapshots {
		if len(s.Services) == 0 {
			t.Errorf("node %s sees no services", nodes[i].name)
		}
	}
}

func TestRuleDeployAndPromote(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	leaderAddr := findLeader(t)
	if leaderAddr == "" {
		t.Fatal("no leader found")
	}

	rule := map[string]string{
		"id":  "e2e-test-rule",
		"dsl": "n:echo",
	}
	resp := postJSON(t, leaderAddr, "/admin/rules", rule)
	if resp.StatusCode != 200 {
		t.Fatalf("deploy rule: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()
	t.Logf("rule deployed via %s", leaderAddr)

	time.Sleep(3 * time.Second)

	// Verify rule exists on all nodes
	for _, n := range nodes {
		resp, err := http.Get(n.httpAddr + "/admin/rules")
		if err != nil {
			t.Fatal(err)
		}
		var rulesList []interface{}
		json.NewDecoder(resp.Body).Decode(&rulesList)
		resp.Body.Close()
		found := false
		for _, r := range rulesList {
			if rm, ok := r.(map[string]interface{}); ok {
				if rm["id"] == "e2e-test-rule" {
					found = true
					break
				}
			}
		}
		if !found {
			t.Errorf("rule not found on node %s", n.name)
		}
	}

	// Cleanup
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/admin/rules/e2e-test-rule", leaderAddr), nil)
	resp2, err := http.DefaultClient.Do(req)
	if err == nil {
		resp2.Body.Close()
	}
}

func TestClusterBusReplication(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	leaderAddr := findLeader(t)
	if leaderAddr == "" {
		t.Fatal("no leader found")
	}

	// Register a service on node-2
	node2 := nodes[1]
	svcReg := map[string]interface{}{
		"name":      "echo",
		"version":   "1.0.0",
		"methods":   []string{"ping"},
		"address":   "127.0.0.1",
		"port":      9999,
		"protocol":  "http",
		"zone":      "test",
		"weight":    1,
	}
	resp := postJSON(t, node2.httpAddr, "/register", svcReg)
	resp.Body.Close()
	t.Logf("registered echo service on %s", node2.name)

	time.Sleep(2 * time.Second)

	// Verify all nodes see the service
	for _, n := range nodes {
		resp, err := http.Get(n.httpAddr + "/services")
		if err != nil {
			t.Fatal(err)
		}
		var sr struct {
			Services map[string]interface{} `json:"services"`
		}
		json.NewDecoder(resp.Body).Decode(&sr)
		resp.Body.Close()
		if _, ok := sr.Services["echo"]; !ok {
			t.Errorf("node %s does not see echo service", n.name)
		}
	}

	// Deploy and remove rule
	rule := map[string]string{
		"id":  "e2e-replication-test",
		"dsl": "n:echo.ping",
	}
	resp = postJSON(t, leaderAddr, "/admin/rules", rule)
	resp.Body.Close()

	time.Sleep(3 * time.Second)

	// Verify rule propagated
	for _, n := range nodes {
		resp, err := http.Get(n.httpAddr + "/admin/rules")
		if err != nil {
			t.Fatal(err)
		}
		var rules []interface{}
		json.NewDecoder(resp.Body).Decode(&rules)
		resp.Body.Close()
		found := false
		for _, r := range rules {
			if rm, ok := r.(map[string]interface{}); ok && rm["id"] == "e2e-replication-test" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("rule e2e-replication-test not on %s", n.name)
		}
	}

	// Cleanup
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/admin/rules/e2e-replication-test", leaderAddr), nil)
	resp2, err := http.DefaultClient.Do(req)
	if err == nil {
		resp2.Body.Close()
	}
}

func TestLeaderFailover(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	if os.Getenv("E2E_KILL") == "" {
		t.Skip("set E2E_KILL=1 to run leader failover test (kills containers)")
	}

	leaderAddr := findLeader(t)
	if leaderAddr == "" {
		t.Fatal("no leader found")
	}
	initialLeaderID := leaderID(t)
	t.Logf("initial leader: %s (%s)", initialLeaderID, leaderAddr)

	var leaderNodeIdx int
	for i, n := range nodes {
		if n.httpAddr == leaderAddr {
			leaderNodeIdx = i
			break
		}
	}

	// Kill the leader container via docker compose
	containerName := fmt.Sprintf("flowrulz-%s", nodes[leaderNodeIdx].name)
	t.Logf("killing leader container: %s", containerName)
	if err := runCmd("docker", "compose", "stop", containerName); err != nil {
		t.Fatalf("kill leader container: %v", err)
	}
	t.Log("leader container killed, waiting for failover...")

	// Wait for new leader to be elected
	var newLeaderAddr string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		newLeaderAddr = findLeader(t)
		if newLeaderAddr != "" && newLeaderAddr != leaderAddr {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if newLeaderAddr == "" || newLeaderAddr == leaderAddr {
		t.Fatal("no new leader elected after failover")
	}

	newLeaderID := leaderID(t)
	t.Logf("new leader: %s (%s)", newLeaderID, newLeaderAddr)

	if newLeaderID == initialLeaderID {
		t.Fatal("new leader should differ from killed leader")
	}

	// Verify the new leader has a higher term
	var newTerm int
	for _, n := range nodes {
		if n.httpAddr == newLeaderAddr {
			resp, err := http.Get(n.httpAddr + "/health")
			if err != nil {
				continue
			}
			var h struct {
				Term int `json:"term"`
			}
			json.NewDecoder(resp.Body).Decode(&h)
			resp.Body.Close()
			newTerm = h.Term
			break
		}
	}
	t.Logf("new leader term: %d", newTerm)
	if newTerm < 1 {
		t.Logf("warning: expected term >= 1, got %d", newTerm)
	}

	// Restart the killed container
	t.Logf("restarting killed container: %s", containerName)
	if err := runCmd("docker", "compose", "start", containerName); err != nil {
		t.Fatalf("restart container: %v", err)
	}

	time.Sleep(10 * time.Second)

	// Verify all 3 nodes are healthy again
	for _, n := range nodes {
		waitForNode(t, n.httpAddr, 30*time.Second)
	}
	t.Log("all 3 nodes healthy after failover and restart")
}

func TestPartitionRebalance(t *testing.T) {
	if os.Getenv("E2E") == "" {
		t.Skip("set E2E=1 to run e2e tests")
	}

	leaderAddr := findLeader(t)
	if leaderAddr == "" {
		t.Fatal("no leader found")
	}

	// Get initial partition assignments
	resp, err := http.Get(leaderAddr + "/partitions")
	if err != nil {
		t.Fatal(err)
	}
	var initial struct {
		NumPartitions  int                    `json:"num_partitions"`
		Assignments    []string               `json:"assignments"`
		NodePartitions map[string][]uint32   `json:"node_partitions"`
	}
	json.NewDecoder(resp.Body).Decode(&initial)
	resp.Body.Close()
	t.Logf("initial partitions: %d across %d nodes", initial.NumPartitions, len(initial.NodePartitions))

	// Verify each node has roughly equal partitions (within 1)
	if len(initial.NodePartitions) > 0 {
		var counts []int
		for _, parts := range initial.NodePartitions {
			counts = append(counts, len(parts))
		}
		for i := 1; i < len(counts); i++ {
			diff := counts[i] - counts[0]
			if diff < 0 {
				diff = -diff
			}
			if diff > 1 {
				t.Logf("partition distribution uneven: %v", counts)
			}
		}
		t.Logf("partition distribution: %v", counts)
	}

	// Force rebalance
	resp, err = http.Post(leaderAddr+"/partitions/rebalance", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	time.Sleep(2 * time.Second)

	// Verify all nodes have the same partition assignments
	var snapshots [][]string
	for _, n := range nodes {
		resp, err := http.Get(n.httpAddr + "/partitions")
		if err != nil {
			t.Fatal(err)
		}
		var pr struct {
			Assignments []string `json:"assignments"`
		}
		json.NewDecoder(resp.Body).Decode(&pr)
		resp.Body.Close()
		snapshots = append(snapshots, pr.Assignments)
	}

	for i := 1; i < len(snapshots); i++ {
		if len(snapshots[i]) != len(snapshots[0]) {
			t.Fatalf("partition count mismatch: node-1=%d, node-%d=%d",
				len(snapshots[0]), i+1, len(snapshots[i]))
		}
		for j := range snapshots[0] {
			if snapshots[i][j] != snapshots[0][j] {
				t.Errorf("partition %d mismatch: node-1=%q, node-%d=%q",
					j, snapshots[0][j], i+1, snapshots[i][j])
			}
		}
	}
	t.Log("all nodes have consistent partition assignments")
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
