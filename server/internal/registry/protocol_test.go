package registry

import (
	"testing"
	"time"
)

func TestLookupInstanceWithProtocol_FiltersByProtocol(t *testing.T) {
	r := New()

	// Register HTTP instance
	r.RegisterInstance(&ServiceInstance{
		Name: "payment",
		Endpoint: Endpoint{
			Address:  "http-payment:8080",
			Port:     8080,
			Protocol: ProtocolHTTP,
		},
	})

	// Register gRPC instance
	r.RegisterInstance(&ServiceInstance{
		Name: "payment",
		Endpoint: Endpoint{
			Address:  "grpc-payment:50051",
			Port:     50051,
			Protocol: ProtocolGRPC,
		},
	})

	// Lookup with HTTP protocol should only return HTTP instance
	inst, err := r.LookupInstanceWithProtocol("payment", "", ProtocolHTTP)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inst.Endpoint.Protocol != ProtocolHTTP {
		t.Errorf("expected HTTP protocol, got %s", inst.Endpoint.Protocol)
	}
	if inst.Endpoint.Address != "http-payment:8080" {
		t.Errorf("expected HTTP address, got %s", inst.Endpoint.Address)
	}

	// Lookup with gRPC protocol should only return gRPC instance
	inst, err = r.LookupInstanceWithProtocol("payment", "", ProtocolGRPC)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inst.Endpoint.Protocol != ProtocolGRPC {
		t.Errorf("expected gRPC protocol, got %s", inst.Endpoint.Protocol)
	}
	if inst.Endpoint.Address != "grpc-payment:50051" {
		t.Errorf("expected gRPC address, got %s", inst.Endpoint.Address)
	}
}

func TestLookupInstanceWithProtocol_EmptyProtocolAnyMatch(t *testing.T) {
	r := New()

	r.RegisterInstance(&ServiceInstance{
		Name: "payment",
		Endpoint: Endpoint{
			Address:  "http-payment:8080",
			Port:     8080,
			Protocol: ProtocolHTTP,
		},
	})

	r.RegisterInstance(&ServiceInstance{
		Name: "payment",
		Endpoint: Endpoint{
			Address:  "grpc-payment:50051",
			Port:     50051,
			Protocol: ProtocolGRPC,
		},
	})

	// Empty protocol should match any healthy instance
	inst, err := r.LookupInstanceWithProtocol("payment", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inst == nil {
		t.Fatal("expected non-nil instance")
	}
}

func TestLookupInstanceWithProtocol_NoMatchReturnsError(t *testing.T) {
	r := New()

	// Only register HTTP
	r.RegisterInstance(&ServiceInstance{
		Name: "payment",
		Endpoint: Endpoint{
			Address:  "http-payment:8080",
			Port:     8080,
			Protocol: ProtocolHTTP,
		},
	})

	// Lookup with gRPC should fail
	_, err := r.LookupInstanceWithProtocol("payment", "", ProtocolGRPC)
	if err == nil {
		t.Fatal("expected error for no matching protocol")
	}
}

func TestRegisterKafkaEndpoint(t *testing.T) {
	r := New()

	err := r.Register("payment-kafka", &Endpoint{
		Protocol:   ProtocolKafka,
		Topic:      "payment-requests",
		ReplyTopic: "payment-replies",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	eps := r.Lookup("payment-kafka")
	if len(eps) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(eps))
	}
	if eps[0].Protocol != ProtocolKafka {
		t.Errorf("expected Kafka protocol, got %s", eps[0].Protocol)
	}
	if eps[0].Topic != "payment-requests" {
		t.Errorf("expected topic payment-requests, got %s", eps[0].Topic)
	}
}

func TestRegisterKafkaEndpointRequiresTopic(t *testing.T) {
	r := New()

	err := r.Register("payment-kafka", &Endpoint{
		Protocol: ProtocolKafka,
		Topic:    "", // missing topic
	})
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestRegisterHTTPRequiresAddress(t *testing.T) {
	r := New()

	err := r.Register("payment", &Endpoint{
		Protocol: ProtocolHTTP,
		Address:  "", // missing address
		Port:     8080,
	})
	if err == nil {
		t.Fatal("expected error for missing address")
	}
}

func TestPickInstanceNeverMixesProtocols(t *testing.T) {
	r := New()

	// Register multiple instances of different protocols
	for i := 0; i < 5; i++ {
		r.RegisterInstance(&ServiceInstance{
			Name: "payment",
			Endpoint: Endpoint{
				Address:  "http-payment:8080",
				Port:     8080,
				Protocol: ProtocolHTTP,
				Load:     float64(i),
			},
		})
		r.RegisterInstance(&ServiceInstance{
			Name: "payment",
			Endpoint: Endpoint{
				Address:  "grpc-payment:50051",
				Port:     50051,
				Protocol: ProtocolGRPC,
				Load:     float64(i),
			},
		})
	}

	// Lookup with HTTP should only return HTTP instances
	for i := 0; i < 10; i++ {
		inst, err := r.LookupInstanceWithProtocol("payment", "", ProtocolHTTP)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if inst.Endpoint.Protocol != ProtocolHTTP {
			t.Errorf("iteration %d: expected HTTP, got %s", i, inst.Endpoint.Protocol)
		}
	}

	// Lookup with gRPC should only return gRPC instances
	for i := 0; i < 10; i++ {
		inst, err := r.LookupInstanceWithProtocol("payment", "", ProtocolGRPC)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if inst.Endpoint.Protocol != ProtocolGRPC {
			t.Errorf("iteration %d: expected gRPC, got %s", i, inst.Endpoint.Protocol)
		}
	}
}

func TestKafkaEndpointWithConsumerGroup(t *testing.T) {
	r := New()

	err := r.RegisterInstance(&ServiceInstance{
		Name: "order-service",
		Endpoint: Endpoint{
			Protocol:      ProtocolKafka,
			Topic:         "order-requests",
			ReplyTopic:    "order-replies",
			ConsumerGroup: "order-service-group",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	inst, err := r.LookupInstanceWithProtocol("order-service", "", ProtocolKafka)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inst.Endpoint.ConsumerGroup != "order-service-group" {
		t.Errorf("expected consumer group order-service-group, got %s", inst.Endpoint.ConsumerGroup)
	}
}

func TestUnhealthyInstanceFiltered(t *testing.T) {
	r := New()

	r.RegisterInstance(&ServiceInstance{
		Name: "payment",
		Endpoint: Endpoint{
			NodeID:   "node-1",
			Address:  "http-payment:8080",
			Port:     8080,
			Protocol: ProtocolHTTP,
		},
	})

	// Mark as unhealthy using nodeID
	r.MarkUnhealthy("payment", "node-1")

	// Should fail to find healthy instance
	_, err := r.LookupInstanceWithProtocol("payment", "", ProtocolHTTP)
	if err == nil {
		t.Fatal("expected error for unhealthy instance")
	}
}

func TestStaleHeartbeatFiltered(t *testing.T) {
	r := New()
	r.SetHeartbeatTimeout(10 * time.Millisecond)

	r.RegisterInstance(&ServiceInstance{
		Name: "payment",
		Endpoint: Endpoint{
			Address:  "http-payment:8080",
			Port:     8080,
			Protocol: ProtocolHTTP,
		},
		HeartbeatAt: time.Now().Add(-1 * time.Second), // stale
	})

	// Wait for heartbeat to become stale
	time.Sleep(20 * time.Millisecond)

	// Should fail due to stale heartbeat
	_, err := r.LookupInstanceWithProtocol("payment", "", ProtocolHTTP)
	if err == nil {
		t.Fatal("expected error for stale heartbeat")
	}
}
