package kafka

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ==========================================
// CONSTANTS (Имена метрик)
// ==========================================

const (
	MetricsKafkaConsumerFetchErrors        = PlatformMetricsConsumer + "_fetch_errors"
	MetricsKafkaConsumerCommitErrors       = PlatformMetricsConsumer + "_commit_errors"
	MetricsKafkaConsumerMessagesTotal      = PlatformMetricsConsumer + "_messages_total"
	MetricsKafkaConsumerProcessingDuration = PlatformMetricsConsumer + "_processing_duration"
	MetricsKafkaProducerMessagesTotal      = PlatformMetricsProducer + "_messages_total"
	MetricsKafkaProducerSendDuration       = PlatformMetricsProducer + "_send_duration"
)

// ==========================================
// CONSUMER INFRA METRICS
// ==========================================

var (
	initConsumerInfraOnce sync.Once
	initConsumerInfraErr  error

	fetchErrors  metric.Int64Counter
	commitErrors metric.Int64Counter
)

func InitConsumerInfraMetrics() error {
	initConsumerInfraOnce.Do(func() {
		const op = "core.messaging.kafka.InitConsumerInfraMetrics"
		meter := otel.Meter(PlatformMetricsConsumer)

		fe, err := meter.Int64Counter(MetricsKafkaConsumerFetchErrors,
			metric.WithDescription("Total number of fetch errors"),
		)
		if err != nil {
			initConsumerInfraErr = fmt.Errorf("%s: failed to create fetch errors counter: %w", op, err)
			return
		}

		ce, err := meter.Int64Counter(MetricsKafkaConsumerCommitErrors,
			metric.WithDescription("Total number of commit errors"),
		)
		if err != nil {
			initConsumerInfraErr = fmt.Errorf("%s: failed to create commit errors counter: %w", op, err)
			return
		}

		fetchErrors = fe
		commitErrors = ce
	})

	return initConsumerInfraErr
}

func RecordFetchError(ctx context.Context) {
	if fetchErrors != nil {
		fetchErrors.Add(ctx, 1)
	}
}

func RecordCommitError(ctx context.Context) {
	if commitErrors != nil {
		commitErrors.Add(ctx, 1)
	}
}

// ==========================================
// CONSUMER METRICS
// ==========================================

var (
	initConsumerMetricsOnce sync.Once
	initConsumerMetricsErr  error

	consumerMessagesTotal  metric.Int64Counter
	consumerProcessingTime metric.Float64Histogram
)

func InitConsumerMetrics() error {
	initConsumerMetricsOnce.Do(func() {
		const op = "core.messaging.kafka.InitConsumerMetrics"
		meter := otel.Meter(PlatformTraceConsumer)

		mt, err := meter.Int64Counter(MetricsKafkaConsumerMessagesTotal,
			metric.WithDescription("Total number of processed messages (success and errors)"),
		)
		if err != nil {
			initConsumerMetricsErr = fmt.Errorf("%s: failed to create messages total counter: %w", op, err)
			return
		}

		pt, err := meter.Float64Histogram(MetricsKafkaConsumerProcessingDuration,
			metric.WithDescription("Message processing duration in seconds"),
			metric.WithUnit("s"), // OTel Standard: секунды
		)
		if err != nil {
			initConsumerMetricsErr = fmt.Errorf("%s: failed to create processing duration histogram: %w", op, err)
			return
		}

		consumerMessagesTotal = mt
		consumerProcessingTime = pt
	})

	return initConsumerMetricsErr
}

type MetricsConsumerHandlerErrorAttr string

func (attr MetricsConsumerHandlerErrorAttr) String() string { return string(attr) }

const (
	MetricsConsumerHandlerErrorNone      MetricsConsumerHandlerErrorAttr = "none"
	MetricsConsumerHandlerErrorNoRetry   MetricsConsumerHandlerErrorAttr = "no_retry"
	MetricsConsumerHandlerErrorRetryable MetricsConsumerHandlerErrorAttr = "retryable"
)

type MetricsStatusAttr string

func (attr MetricsStatusAttr) String() string { return string(attr) }

const (
	MetricsConsumerHandlerStatusError   MetricsStatusAttr = "error"
	MetricsConsumerHandlerStatusSuccess MetricsStatusAttr = "success"
)

func RecordConsumerMessage(
	ctx context.Context,
	topic string,
	status MetricsStatusAttr,
	errType MetricsConsumerHandlerErrorAttr,
) {
	if consumerMessagesTotal != nil {
		consumerMessagesTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("status", status.String()),
			attribute.String("error_type", errType.String()),
		))
	}
}

func RecordConsumerProcessingTime(ctx context.Context, topic string, seconds float64) {
	if consumerProcessingTime != nil {
		consumerProcessingTime.Record(ctx, seconds, metric.WithAttributes(
			attribute.String("topic", topic),
		))
	}
}

// ==========================================
// PRODUCER METRICS
// ==========================================

var (
	initProducerMetricsOnce sync.Once
	initProducerMetricsErr  error

	producerMessagesTotal metric.Int64Counter
	producerSendDuration  metric.Float64Histogram
)

func InitProducerMetrics() error {
	initProducerMetricsOnce.Do(func() {
		const op = "core.messaging.kafka.InitProducerMetrics"
		meter := otel.Meter(PlatformTraceProducer)

		mt, err := meter.Int64Counter(MetricsKafkaProducerMessagesTotal,
			metric.WithDescription("Total number of messages sent (success and errors)"),
		)
		if err != nil {
			initProducerMetricsErr = fmt.Errorf("%s: failed to create messages total counter: %w", op, err)
			return
		}

		sd, err := meter.Float64Histogram(MetricsKafkaProducerSendDuration,
			metric.WithDescription("Duration from Send to Completion callback in seconds"),
			metric.WithUnit("s"), // OTel Standard: секунды
		)
		if err != nil {
			initProducerMetricsErr = fmt.Errorf("%s: failed to create send duration histogram: %w", op, err)
			return
		}

		producerMessagesTotal = mt
		producerSendDuration = sd
	})

	return initProducerMetricsErr
}

func RecordProducerMessage(ctx context.Context, topic string, status MetricsStatusAttr) {
	if producerMessagesTotal != nil {
		producerMessagesTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("status", status.String()),
		))
	}
}

func RecordProducerSendDuration(ctx context.Context, topic string, seconds float64) {
	if producerSendDuration != nil {
		producerSendDuration.Record(ctx, seconds, metric.WithAttributes(
			attribute.String("topic", topic),
		))
	}
}
