package kafka

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ==========================================
// CONSUMER INFRA METRICS
// ==========================================

type ConsumerInfraMetrics struct {
	fetchErrors metric.Int64Counter

	commitErrors metric.Int64Counter
}

const (
	MetricsKafkaConsumerFetchErrors  = "kafka.consumer.fetch.errors"
	MetricsKafkaConsumerCommitErrors = "kafka.consumer.commit.errors"
)

func NewConsumerInfraMetrics() (*ConsumerInfraMetrics, error) {

	const op = "core.messaging.kafka.NewConsumerInfraMetrics"

	meter := otel.Meter(PlatformTelemetryConsumer)

	fetchErrors, err := meter.Int64Counter(MetricsKafkaConsumerFetchErrors,

		metric.WithDescription("Total number of fetch errors"),
	)

	if err != nil {

		return nil, fmt.Errorf("%s: failed to create fetch errors counter: %w", op, err)

	}

	commitErrors, err := meter.Int64Counter(MetricsKafkaConsumerCommitErrors,

		metric.WithDescription("Total number of commit errors"),
	)

	if err != nil {

		return nil, fmt.Errorf("%s: failed to create commit errors counter: %w", op, err)

	}

	return &ConsumerInfraMetrics{

		fetchErrors: fetchErrors,

		commitErrors: commitErrors,
	}, nil

}

func (m *ConsumerInfraMetrics) RecordFetchError(ctx context.Context) {

	m.fetchErrors.Add(ctx, 1)

}

func (m *ConsumerInfraMetrics) RecordCommitError(ctx context.Context) {

	m.commitErrors.Add(ctx, 1)

}

// ==========================================
// CONSUMER METRICS
// ==========================================

type ConsumerMetrics struct {
	messagesTotal  metric.Int64Counter
	processingTime metric.Float64Histogram
}

const (
	MetricsKafkaConsumerMessagesTotal      = "kafka.consumer.messages.total"
	MetricsKafkaConsumerProcessingDuration = "kafka.consumer.processing.duration"
)

func NewConsumerMetrics() (*ConsumerMetrics, error) {
	const op = "core.messaging.kafka.NewConsumerMetrics"

	meter := otel.Meter(PlatformTelemetryConsumer)

	messagesTotal, err := meter.Int64Counter(MetricsKafkaConsumerMessagesTotal,
		metric.WithDescription("Total number of processed messages (success and errors)"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create messages total counter: %w", op, err)
	}

	processingTime, err := meter.Float64Histogram(MetricsKafkaConsumerProcessingDuration,
		metric.WithDescription("Message processing duration in seconds"),
		metric.WithUnit("s"), // OTel Standard: секунды
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create processing duration histogram: %w", op, err)
	}

	return &ConsumerMetrics{
		messagesTotal:  messagesTotal,
		processingTime: processingTime,
	}, nil
}

type MetricsConsumerHandlerErrorAttr string

func (attr MetricsConsumerHandlerErrorAttr) String() string {
	return string(attr)
}

const (
	MetricsConsumerHandlerErrorNone      MetricsConsumerHandlerErrorAttr = "none"
	MetricsConsumerHandlerErrorNoRetry   MetricsConsumerHandlerErrorAttr = "no_retry"
	MetricsConsumerHandlerErrorRetryable MetricsConsumerHandlerErrorAttr = "retryable"
)

type MetricsStatusAttr string

func (attr MetricsStatusAttr) String() string {
	return string(attr)
}

const (
	MetricsConsumerHandlerStatusError   MetricsStatusAttr = "error"
	MetricsConsumerHandlerStatusSuccess MetricsStatusAttr = "success"
)

// RecordMessage записывает и успех, и ошибку в одну метрику с разными лейблами
func (m *ConsumerMetrics) RecordMessage(
	ctx context.Context,
	topic string,
	status MetricsStatusAttr,
	errType MetricsConsumerHandlerErrorAttr,
) {
	m.messagesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("status", status.String()),      // "success" или "error"
		attribute.String("error_type", errType.String()), // "none", "no_retry", "retryable"
	))
}

func (m *ConsumerMetrics) RecordProcessingTime(ctx context.Context, topic string, seconds float64) {
	m.processingTime.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

// ==========================================
// PRODUCER METRICS
// ==========================================

type ProducerMetrics struct {
	messagesTotal metric.Int64Counter
	sendDuration  metric.Float64Histogram
}

const (
	MetricsKafkaProducerMessagesTotal = "kafka.producer.messages.total"
	MetricsKafkaProducerSendDuration  = "kafka.producer.send.duration"
)

func NewProducerMetrics() (*ProducerMetrics, error) {
	const op = "core.messaging.kafka.NewProducerMetrics"

	meter := otel.Meter(PlatformTelemetryProducer)

	messagesTotal, err := meter.Int64Counter(MetricsKafkaProducerMessagesTotal,
		metric.WithDescription("Total number of messages sent (success and errors)"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create messages total counter: %w", op, err)
	}

	sendDuration, err := meter.Float64Histogram(MetricsKafkaProducerSendDuration,
		metric.WithDescription("Duration from Send to Completion callback in seconds"),
		metric.WithUnit("s"), // OTel Standard: секунды
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create send duration histogram: %w", op, err)
	}

	return &ProducerMetrics{
		messagesTotal: messagesTotal,
		sendDuration:  sendDuration,
	}, nil
}

func (m *ProducerMetrics) RecordMessage(ctx context.Context, topic string, status MetricsStatusAttr) {
	m.messagesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("status", status.String()), // "success" или "error"
	))
}

func (m *ProducerMetrics) RecordDuration(ctx context.Context, topic string, seconds float64) {
	m.sendDuration.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

// ConsumerInfraMetrics оставляем без изменений (Fetch/Commit Errors),
// так как они не привязаны к конкретному сообщению.
