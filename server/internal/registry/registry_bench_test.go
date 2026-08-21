package registry

import (
	"testing"
)

func setupRegistry(b *testing.B, numInstances int) *ServiceRegistry {
	b.Helper()
	r := New()
	for i := 0; i < numInstances; i++ {
		r.Register("svc-a", &Endpoint{
			Protocol: ProtocolHTTP,
			Address:  "10.0.0.1",
			Port:     8080 + i,
			NodeID:   "node-1",
		})
		r.RegisterInstance(&ServiceInstance{
			Name: "svc-b",
			Endpoint: Endpoint{
				Protocol: ProtocolHTTP,
				Address:  "10.0.0.2",
				Port:     9090 + i,
				NodeID:   "node-2",
			},
		})
	}
	return r
}

func BenchmarkLookup_1Instance(b *testing.B) {
	r := setupRegistry(b, 1)
	for b.Loop() {
		r.Lookup("svc-a")
	}
}

func BenchmarkLookup_10Instances(b *testing.B) {
	r := setupRegistry(b, 10)
	for b.Loop() {
		r.Lookup("svc-a")
	}
}

func BenchmarkLookup_NotFound(b *testing.B) {
	r := setupRegistry(b, 5)
	for b.Loop() {
		r.Lookup("nonexistent")
	}
}

func BenchmarkLookupInstance_RoundRobin(b *testing.B) {
	r := setupRegistry(b, 5)
	for b.Loop() {
		r.LookupInstance("svc-b", "GET")
	}
}

func BenchmarkLookupInstance_Parallel(b *testing.B) {
	r := setupRegistry(b, 5)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.LookupInstance("svc-b", "GET")
		}
	})
}

func BenchmarkLookupWithProtocol(b *testing.B) {
	r := setupRegistry(b, 5)
	for b.Loop() {
		r.LookupInstanceWithProtocol("svc-b", "GET", ProtocolHTTP)
	}
}

func BenchmarkLookupWithProtocol_Mismatch(b *testing.B) {
	r := setupRegistry(b, 5)
	for b.Loop() {
		r.LookupInstanceWithProtocol("svc-b", "GET", ProtocolGRPC)
	}
}
