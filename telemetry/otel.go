package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

var (
	closer      *telemetryCloser
	initErr     error
	serviceName string
	once        sync.Once
)

const TraceParent = "traceparent"

// TODO: add timeouts and other to config!
func InitTelemetry(ctx context.Context, cfg Config) (*telemetryCloser, error) {

	once.Do(func() {
		if err := cfg.Validate(); err != nil {
			initErr = err
			return
		}

		res, err := resource.New(
			ctx,
			resource.WithSchemaURL(semconv.SchemaURL),
			resource.WithAttributes(
				semconv.ServiceNameKey.String(cfg.ServiceName),
				semconv.ServiceVersion(cfg.ServiceVersion),
				attribute.String("environment", cfg.Environment),
			),
			resource.WithHost(),
			resource.WithOS(),
			resource.WithProcess(),
			resource.WithContainer(),
			resource.WithTelemetrySDK(),
		)

		if err != nil {
			initErr = err
			return
		}

		// Трейсы
		traceExporter, err := otlptracehttp.New(
			ctx,
			otlptracehttp.WithInsecure(),
			otlptracehttp.WithEndpoint(cfg.CollectorAddr),
			otlptracehttp.WithTimeout(cfg.TracesExporterTimeout),
			otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
			otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
				Enabled:         true,
				InitialInterval: cfg.TracesExporterRetryStartInterval,
				MaxInterval:     cfg.TracesExporterRetryMaxInterval,
				MaxElapsedTime:  cfg.TracesExporterRetryMaxTime,
			}),
		)

		if err != nil {
			initErr = err
			return
		}

		bsp := sdktrace.NewBatchSpanProcessor(
			traceExporter,
			sdktrace.WithMaxQueueSize(cfg.TracesMaxQueueSize),          // Храним в памяти до {value} к спанов (дефолт 2048)
			sdktrace.WithMaxExportBatchSize(cfg.TracesExportBatchSize), // Отправляем пачками по {value} (дефолт 512)
			// Отправляем пачку каждые {value} секунды (если не накопилось 4096)
			sdktrace.WithBatchTimeout(cfg.TracesBatchTimeout),
		)

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSpanProcessor(bsp),
			sdktrace.WithResource(res),
			//TODO: add sampler maybe??
		)

		// Метрики
		metricExporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithEndpoint(cfg.CollectorAddr),

			// 1. Включаем сжатие (МАГИЯ ДЛЯ СКОРОСТИ!)
			otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),

			// 2. Ставим жесткий таймаут (чтобы не блокировать горутины, если коллектор тупит)
			otlpmetrichttp.WithTimeout(cfg.MetricsExporterTimeout),

			// 3. (Опционально) Добавляем retry-политику, чтобы сгладить сетевые скачки
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				Enabled:         true,
				InitialInterval: cfg.MetricsExporterRetryStartInterval,
				MaxInterval:     cfg.MetricsExporterRetryMaxInterval,
				MaxElapsedTime:  cfg.MetricsExporterRetryMaxTime,
			}),
		)
		if err != nil {
			initErr = err
			return
		}

		mp := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
				sdkmetric.WithInterval(cfg.MetricsProviderExportInterval), // Должно быть 10s-15s минимум!
				sdkmetric.WithTimeout(cfg.MetricsProviderTimeout),         // Если сбор затянулся, прерываем
			)),
			sdkmetric.WithView(cfg.MetricsViews...),
			sdkmetric.WithResource(res),
		)

		logExporter, err := otlploghttp.New(ctx,
			otlploghttp.WithInsecure(),
			otlploghttp.WithEndpoint(cfg.CollectorAddr),
		)
		if err != nil {
			initErr = err
			return
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
			initErr = err
			return
		}

		closer = &telemetryCloser{
			StopCollectMetrics: mp.Shutdown,
			StopCollectTraces:  tp.Shutdown,
			StopCollectLogs:    lp.Shutdown,
		}

		serviceName = cfg.ServiceName
	})

	if initErr != nil {
		return nil, initErr
	}

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
