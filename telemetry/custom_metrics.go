package telemetry

import (
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/transport/http"
	"go.opentelemetry.io/otel/sdk/metric"
)

// secBuckets задает корзины от 5 миллисекунд до 10 секунд
var secBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// secBuckets := []float64{
// 		0.0001, 0.0005, 0.001, 0.0025, // Новые микро-корзины
// 		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
// 	}

// DefaultDurationViews содержит строгие правила для всех известных нам гистограмм времени
var DefaultDurationViews = []metric.View{
	// Для Консюмера
	metric.NewView(
		metric.Instrument{Name: kafka.MetricsKafkaConsumerProcessingDuration},
		metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{Boundaries: secBuckets}},
	),
	// Для Продюсера
	metric.NewView(
		metric.Instrument{Name: kafka.MetricsKafkaProducerSendDuration},
		metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{Boundaries: secBuckets}},
	),
	// Для HTTP сервера (otelhttp)
	metric.NewView(
		metric.Instrument{Name: http.MetricsHTTPServerRequestDuration},
		metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{Boundaries: secBuckets}},
	),
}
