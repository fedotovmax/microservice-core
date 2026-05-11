package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type telemetryCloser struct {
	StopCollectMetrics func(ctx context.Context) error
	StopCollectTraces  func(ctx context.Context) error
}

var serviceName string

const TraceParent = "traceparent"

func InitTelemetry(ctx context.Context, cfg Config) (*telemetryCloser, error) {

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	res := resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceNameKey.String(cfg.ServiceName),
	)

	// Трейсы
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithEndpoint(cfg.CollectorAddr),

		//TODO: add batching, sampling, timeouts, retries
	)

	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	// Метрики
	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithInsecure(),
		otlpmetrichttp.WithEndpoint(cfg.CollectorAddr),
	)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(15*time.Second),
		)),
		sdkmetric.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if err := runtime.Start(
		runtime.WithMinimumReadMemStatsInterval(time.Second),
		runtime.WithMeterProvider(mp),
	); err != nil {
		return nil, err
	}

	closer := &telemetryCloser{
		StopCollectMetrics: mp.Shutdown,
		StopCollectTraces:  tp.Shutdown,
	}

	serviceName = cfg.ServiceName

	return closer, nil
}

func checkInit() {
	if serviceName == "" {
		panic("telemetry not specified, please, call InitTelemetry func to configurate telemetry")
	}
}

func CreatePlatformTrace(componentName string) trace.Tracer {
	checkInit()
	return createTrace("platform." + componentName)
}

func createTrace(name string) trace.Tracer {
	return otel.Tracer(name)
}

func StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	checkInit()
	return createTrace(serviceName).Start(ctx, spanName, opts...)
}

func StartPlatformSpan(ctx context.Context, componentName string, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return CreatePlatformTrace(componentName).Start(ctx, spanName, opts...)
}
