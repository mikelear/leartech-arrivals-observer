// Package tracing wires OpenTelemetry trace export to the cluster's Tempo
// instance. Initialise at startup with [Init], defer [Shutdown] before exit.
//
// Defaults assume in-cluster deployment in jx-staging:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT  default: http://tempo.jx-observability:4318
//	OTEL_TRACES_DISABLED         set to "1" to no-op (local dev)
//
// Service name + cluster tag are required arguments — they become the
// `service.name` + `cluster` resource attributes on every span, which is what
// tempo-to-har filters on when synthesising HARs.
package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const defaultEndpoint = "tempo.jx-observability:4318"

// Init configures the global TracerProvider. The returned shutdown function
// flushes pending spans; call it before the process exits.
//
// If OTEL_TRACES_DISABLED is set, Init returns a no-op shutdown and configures
// nothing — useful for local dev where Tempo is unreachable.
func Init(ctx context.Context, serviceName, version, clusterTag string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_TRACES_DISABLED") == "1" {
		return func(context.Context) error { return nil }, nil
	}

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
			attribute.String("cluster", clusterTag),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
