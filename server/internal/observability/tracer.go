package observability

import (
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var opcodeNames = map[byte]string{
	0:  "next",
	1:  "parallel",
	2:  "collect",
	3:  "fallback",
	4:  "gate",
	5:  "split",
	6:  "map",
	7:  "emit",
	8:  "drop",
	9:  "buffer",
	10: "key",
	11: "retry",
	12: "pipe",
	13: "timeout",
	14: "async",
	15: "chunk",
	16: "dag",
	17: "jmp",
	18: "label",
	19: "svc_arg",
	20: "retry_data",
	21: "jump_offset",
	22: "type_guard",
}

type SpanExporter struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	stopCh   chan struct{}
	spanSize int
}

func NewSpanExporter(endpoint string) *SpanExporter {
	if endpoint == "" {
		return nil
	}

	ctx := context.Background()

	exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	))
	if err != nil {
		log.Printf("otel: create exporter: %v", err)
		return nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.Key("service.name").String("flowrulz"),
			attribute.Key("service.version").String("1.0.0"),
		),
	)
	if err != nil {
		log.Printf("otel: create resource: %v", err)
		return nil
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(provider)

	return &SpanExporter{
		provider: provider,
		tracer:   provider.Tracer("flowrulz"),
		stopCh:   make(chan struct{}),
		spanSize: bridge.SpanSize(),
	}
}

func (se *SpanExporter) Start(ctx context.Context) {
	if se == nil {
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			se.exportSpans(ctx)
		case <-se.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (se *SpanExporter) Stop() {
	if se == nil {
		return
	}
	close(se.stopCh)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := se.provider.Shutdown(ctx); err != nil {
		log.Printf("otel: shutdown error: %v", err)
	}
}

type rawSpan struct {
	Opcode     uint8
	_pad1      [1]byte
	ServiceID  uint16
	Layer      uint8
	_pad2      [3]byte
	DurationNS uint64
	Status     uint8
}

func (se *SpanExporter) exportSpans(ctx context.Context) {
	if se.spanSize <= 0 {
		se.spanSize = 24
	}

	data := bridge.GetSpans()
	if len(data) == 0 {
		return
	}

	for i := 0; i+se.spanSize <= len(data); i += se.spanSize {
		raw := rawSpan{
			Opcode:     data[i],
			_pad1:      [1]byte{},
			ServiceID:  binary.LittleEndian.Uint16(data[i+2:]),
			Layer:      data[i+4],
			_pad2:      [3]byte{},
			DurationNS: binary.LittleEndian.Uint64(data[i+8:]),
			Status:     data[i+16],
		}

		se.exportSpan(ctx, &raw)

	}
}

func (se *SpanExporter) exportSpan(ctx context.Context, raw *rawSpan) {
	opName := opcodeNames[raw.Opcode]
	if opName == "" {
		opName = "opcode"
	}

	now := time.Now()
	startTime := now.Add(-time.Duration(raw.DurationNS))

	_, span := se.tracer.Start(ctx, opName,
		trace.WithTimestamp(startTime),
		trace.WithAttributes(
			attribute.Int("service_id", int(raw.ServiceID)),
			attribute.Int("layer", int(raw.Layer)),
			attribute.Int("status", int(raw.Status)),
			attribute.Int64("duration_ns", int64(raw.DurationNS)),
		),
	)
	span.End(trace.WithTimestamp(now))
}
