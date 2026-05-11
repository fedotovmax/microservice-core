package kafka

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ConsumerMetrics struct {
	messagesProcessed metric.Int64Counter
	messagesErrors    metric.Int64Counter
	processingTime    metric.Float64Histogram
}

func NewConsumerMetrics() (*ConsumerMetrics, error) {
	const op = "core.messaging.kafka.NewConsumerMetrics"

	meter := otel.Meter(ConsumerMeterName)

	messagesProcessed, err := meter.Int64Counter("kafka.consumer.messages.processed",
		metric.WithDescription("Total number of successfully processed messages"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create messages processed counter: %w", op, err)
	}

	messagesErrors, err := meter.Int64Counter("kafka.consumer.messages.errors",
		metric.WithDescription("Total number of handler errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create messages errors counter: %w", op, err)
	}

	processingTime, err := meter.Float64Histogram("kafka.consumer.processing.duration",
		metric.WithDescription("Message processing duration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create processing duration histogram: %w", op, err)
	}

	return &ConsumerMetrics{
		messagesProcessed: messagesProcessed,
		messagesErrors:    messagesErrors,
		processingTime:    processingTime,
	}, nil
}

func (m *ConsumerMetrics) RecordProcessed(ctx context.Context, topic string) {
	m.messagesProcessed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (m *ConsumerMetrics) RecordHandlerError(ctx context.Context, topic string) {
	m.messagesErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (m *ConsumerMetrics) RecordProcessingTime(ctx context.Context, topic string, ms float64) {
	m.processingTime.Record(ctx, ms, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

// ConsumerInfraMetrics — инфраструктурные метрики (fetch, commit)

type ConsumerInfraMetrics struct {
	fetchErrors  metric.Int64Counter
	commitErrors metric.Int64Counter
}

func NewConsumerInfraMetrics() (*ConsumerInfraMetrics, error) {
	const op = "core.messaging.kafka.NewConsumerInfraMetrics"

	meter := otel.Meter(ConsumerMeterName)

	fetchErrors, err := meter.Int64Counter("kafka.consumer.fetch.errors",
		metric.WithDescription("Total number of fetch errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create fetch errors counter: %w", op, err)
	}

	commitErrors, err := meter.Int64Counter("kafka.consumer.commit.errors",
		metric.WithDescription("Total number of commit errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create commit errors counter: %w", op, err)
	}

	return &ConsumerInfraMetrics{
		fetchErrors:  fetchErrors,
		commitErrors: commitErrors,
	}, nil
}

func (m *ConsumerInfraMetrics) RecordFetchError(ctx context.Context) {
	m.fetchErrors.Add(ctx, 1)
}

func (m *ConsumerInfraMetrics) RecordCommitError(ctx context.Context) {
	m.commitErrors.Add(ctx, 1)
}
