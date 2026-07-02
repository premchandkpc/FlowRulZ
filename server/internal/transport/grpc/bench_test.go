package grpctransport

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/pkg/transport"
)

func setupLocalBus(b *testing.B) (*GRPCBus, *GRPCClient, func()) {
	bus := NewGRPCBus(":0")
	if err := bus.Start(); err != nil {
		b.Fatal(err)
	}
	addr := bus.lis.Addr().String()
	client := NewGRPCClient(addr)
	if err := client.Connect(); err != nil {
		bus.Stop()
		b.Fatal(err)
	}
	return bus, client, func() {
		client.Close()
		bus.Stop()
	}
}

func BenchmarkPublishThroughput(b *testing.B) {
	_, client, cleanup := setupLocalBus(b)
	defer cleanup()

	var count int64
	client.Subscribe("bench", func(ctx context.Context, msg *transport.Message) {
		atomic.AddInt64(&count, 1)
	})
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Publish("bench", &transport.Message{
			ID:   fmt.Sprintf("msg-%d", i),
			Body: []byte(`{"hello":"world"}`),
		})
	}

	time.Sleep(500 * time.Millisecond)
	delivered := atomic.LoadInt64(&count)
	b.ReportMetric(float64(delivered)/float64(b.N), "delivery_ratio")
}

func BenchmarkPublishLatency(b *testing.B) {
	_, client, cleanup := setupLocalBus(b)
	defer cleanup()

	done := make(chan struct{}, b.N)
	client.Subscribe("latency", func(ctx context.Context, msg *transport.Message) {
		done <- struct{}{}
	})
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		client.Publish("latency", &transport.Message{
			ID:   fmt.Sprintf("msg-%d", i),
			Body: []byte("ping"),
		})
		<-done
		b.ReportMetric(float64(time.Since(start).Microseconds()), "µs/op")
	}
}

func BenchmarkRequestReply(b *testing.B) {
	_, client, cleanup := setupLocalBus(b)
	defer cleanup()

	client.Subscribe("req", func(ctx context.Context, msg *transport.Message) {
		client.Reply("reply", msg.CorrelationID, &transport.Message{
			Body: []byte("pong"),
		})
	})
	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Request("req", &transport.Message{
			ID:            fmt.Sprintf("req-%d", i),
			Body:          []byte("ping"),
			CorrelationID: fmt.Sprintf("corr-%d", i),
		}, 5*time.Second)
		if err != nil {
			b.Fatal(err)
		}
	}
}
