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

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/log"

	// Правильный пакет для глобального провайдера логов:
	"go.opentelemetry.io/otel/log/global"
)

type closeFn func(ctx context.Context) error

type telemetryCloser struct {
	StopCollectMetrics closeFn
	StopCollectTraces  closeFn
	StopCollectLogs    closeFn
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

	bsp := sdktrace.NewBatchSpanProcessor(
		traceExporter,
		sdktrace.WithMaxQueueSize(65536),      // Храним в памяти до 65к спанов (дефолт 2048)
		sdktrace.WithMaxExportBatchSize(4096), // Отправляем пачками по 4096 (дефолт 512)
		// Отправляем пачку каждые 2 секунды (если не накопилось 4096)
		sdktrace.WithBatchTimeout(2*time.Second),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithResource(res),
	)

	// Метрики
	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithInsecure(),
		otlpmetrichttp.WithEndpoint(cfg.CollectorAddr),

		// 1. Включаем сжатие (МАГИЯ ДЛЯ СКОРОСТИ!)
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),

		// 2. Ставим жесткий таймаут (чтобы не блокировать горутины, если коллектор тупит)
		otlpmetrichttp.WithTimeout(5*time.Second),

		// 3. (Опционально) Добавляем retry-политику, чтобы сгладить сетевые скачки
		otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
			Enabled:         true,
			InitialInterval: 1 * time.Second,
			MaxInterval:     10 * time.Second,
			MaxElapsedTime:  15 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(cfg.MetricsExportInterval), // Должно быть 10s-15s минимум!
			sdkmetric.WithTimeout(5*time.Second),              // Если сбор затянулся, прерываем
		)),
		sdkmetric.WithView(cfg.MetricsViews...),
		sdkmetric.WithResource(res),
	)

	logExporter, err := otlploghttp.New(ctx,
		otlploghttp.WithInsecure(),
		otlploghttp.WithEndpoint(cfg.CollectorAddr),
	)
	if err != nil {
		return nil, err
	}

	lp := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
		log.WithResource(res), // Тот же ресурс с service.name
	)

	global.SetLoggerProvider(lp)

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
		StopCollectLogs:    lp.Shutdown,
	}

	serviceName = cfg.ServiceName

	return closer, nil
}

func PlatformModule(componentName string) string {
	return "platform." + componentName
}

func checkInit() {
	if serviceName == "" {
		panic("telemetry not specified, please, call InitTelemetry func to configurate telemetry")
	}
}

func CreatePlatformTrace(componentName string) trace.Tracer {
	checkInit()
	return createTrace(PlatformModule(componentName))
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
