package registry

import (
	"testing"
)

func TestRegisterAndLookup(t *testing.T) {
	r := New()

	err := r.Register("payment", &Endpoint{NodeID: "node-a", Address: "10.0.0.1", Port: 9090, Protocol: ProtocolHTTP})
	if err != nil {
		t.Fatal(err)
	}
	err = r.Register("payment", &Endpoint{NodeID: "node-b", Address: "10.0.0.2", Port: 9090, Protocol: ProtocolHTTP})
	if err != nil {
		t.Fatal(err)
	}

	eps := r.Lookup("payment")
	if len(eps) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(eps))
	}

	all := r.ListEndpoints("payment")
	if len(all) != 2 {
		t.Fatalf("expected 2 total endpoints, got %d", len(all))
	}
}

func TestRegisterDuplicate(t *testing.T) {
	r := New()

	ep := &Endpoint{NodeID: "node-a", Address: "10.0.0.1", Port: 9090, Protocol: ProtocolHTTP}
	r.Register("payment", ep)

	updated := &Endpoint{NodeID: "node-a", Address: "10.0.0.1", Port: 9090, Protocol: ProtocolHTTP, Healthy: true, Load: 0.5}
	r.Register("payment", updated)

	eps := r.Lookup("payment")
	if len(eps) != 1 {
		t.Fatalf("expected 1 endpoint after update, got %d", len(eps))
	}
	if eps[0].Load != 0.5 {
		t.Fatalf("expected load 0.5, got %f", eps[0].Load)
	}
}

func TestLookupFiltersUnhealthy(t *testing.T) {
	r := New()

	r.Register("payment", &Endpoint{NodeID: "node-a", Address: "10.0.0.1", Port: 9090})
	r.Register("payment", &Endpoint{NodeID: "node-b", Address: "10.0.0.2", Port: 9090})

	r.MarkUnhealthy("payment", "node-a")

	eps := r.Lookup("payment")
	if len(eps) != 1 {
		t.Fatalf("expected 1 healthy endpoint, got %d", len(eps))
	}
	if eps[0].NodeID != "node-b" {
		t.Fatalf("expected node-b, got %s", eps[0].NodeID)
	}
}

func TestUnregister(t *testing.T) {
	r := New()

	r.Register("payment", &Endpoint{NodeID: "node-a", Address: "10.0.0.1", Port: 9090})
	r.Register("payment", &Endpoint{NodeID: "node-b", Address: "10.0.0.2", Port: 9090})

	r.Unregister("payment", "node-a")

	eps := r.Lookup("payment")
	if len(eps) != 1 {
		t.Fatalf("expected 1 endpoint after unregister, got %d", len(eps))
	}

	r.Unregister("payment", "node-b")
	eps = r.Lookup("payment")
	if eps != nil {
		t.Fatal("expected nil after removing all endpoints")
	}
}

func TestPickRoundRobin(t *testing.T) {
	r := New()

	r.Register("svc", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090})
	r.Register("svc", &Endpoint{NodeID: "b", Address: "10.0.0.2", Port: 9090})

	picked := make(map[string]int)
	for i := 0; i < 100; i++ {
		ep, err := r.PickWithStrategy("svc", LBStrategyRoundRobin)
		if err != nil {
			t.Fatal(err)
		}
		picked[ep.NodeID]++
	}

	if len(picked) != 2 {
		t.Fatalf("expected 2 nodes picked, got %d", len(picked))
	}
	if picked["a"] != 50 || picked["b"] != 50 {
		t.Fatalf("expected even distribution (50/50), got a=%d b=%d", picked["a"], picked["b"])
	}
}

func TestPickRandom(t *testing.T) {
	r := New()

	r.Register("svc", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090})
	r.Register("svc", &Endpoint{NodeID: "b", Address: "10.0.0.2", Port: 9090})

	picked := make(map[string]int)
	for i := 0; i < 1000; i++ {
		ep, err := r.PickWithStrategy("svc", LBStrategyRandom)
		if err != nil {
			t.Fatal(err)
		}
		picked[ep.NodeID]++
	}

	if len(picked) != 2 {
		t.Fatalf("expected 2 nodes picked, got %d", len(picked))
	}
}

func TestPickNoHealthyEndpoints(t *testing.T) {
	r := New()

	r.Register("svc", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090})
	r.MarkUnhealthy("svc", "a")

	_, err := r.Pick("svc")
	if err == nil {
		t.Fatal("expected error for no healthy endpoints")
	}
}

func TestPickNotFound(t *testing.T) {
	r := New()
	_, err := r.Pick("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent service")
	}
}

func TestMarkHealthy(t *testing.T) {
	r := New()

	r.Register("svc", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090})
	r.MarkUnhealthy("svc", "a")

	_, err := r.Pick("svc")
	if err == nil {
		t.Fatal("expected error after marking unhealthy")
	}

	r.MarkHealthy("svc", "a")
	ep, err := r.Pick("svc")
	if err != nil {
		t.Fatal(err)
	}
	if ep.NodeID != "a" {
		t.Fatalf("expected node-a, got %s", ep.NodeID)
	}
}

func TestListServices(t *testing.T) {
	r := New()

	r.Register("payment", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090})
	r.Register("inventory", &Endpoint{NodeID: "b", Address: "10.0.0.2", Port: 9090})

	svcs := r.ListServices()
	if len(svcs) != 2 {
		t.Fatalf("expected 2 services, got %d", len(svcs))
	}
}

func TestSnapshot(t *testing.T) {
	r := New()

	r.Register("payment", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090})
	r.Register("inventory", &Endpoint{NodeID: "b", Address: "10.0.0.2", Port: 9090})

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 services in snapshot, got %d", len(snap))
	}

	r.Register("email", &Endpoint{NodeID: "c", Address: "10.0.0.3", Port: 9090})

	if len(snap) != 2 {
		t.Fatalf("expected snapshot to be immutable, got %d", len(snap))
	}
}

func TestRegisterValidation(t *testing.T) {
	r := New()

	if err := r.Register("", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090}); err == nil {
		t.Fatal("expected error for empty name")
	}
	if err := r.Register("svc", nil); err == nil {
		t.Fatal("expected error for nil endpoint")
	}
	if err := r.Register("svc", &Endpoint{NodeID: "a", Address: "", Port: 9090}); err == nil {
		t.Fatal("expected error for empty address")
	}
	if err := r.Register("svc", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 0}); err == nil {
		t.Fatal("expected error for invalid port")
	}
}

func TestLeastLoaded(t *testing.T) {
	r := New()

	r.Register("svc", &Endpoint{NodeID: "a", Address: "10.0.0.1", Port: 9090, Load: 0.9})
	r.Register("svc", &Endpoint{NodeID: "b", Address: "10.0.0.2", Port: 9090, Load: 0.1})

	ep, err := r.PickWithStrategy("svc", LBStrategyLeastLoaded)
	if err != nil {
		t.Fatal(err)
	}
	if ep.NodeID != "b" {
		t.Fatalf("expected least-loaded node-b, got %s", ep.NodeID)
	}
}
