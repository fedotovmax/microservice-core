package kafka

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ProducerMetrics struct {
	messagesSent   metric.Int64Counter
	messagesErrors metric.Int64Counter
	sendDuration   metric.Float64Histogram
}

func NewProducerMetrics() (*ProducerMetrics, error) {
	const op = "core.messaging.kafka.NewProducerMetrics"

	meter := otel.Meter(ProducerMeterName)

	messagesSent, err := meter.Int64Counter("kafka.producer.messages.sent",
		metric.WithDescription("Total number of successfully sent messages"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create messages sent counter: %w", op, err)
	}

	messagesErrors, err := meter.Int64Counter("kafka.producer.messages.errors",
		metric.WithDescription("Total number of failed messages"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create messages errors counter: %w", op, err)
	}

	sendDuration, err := meter.Float64Histogram("kafka.producer.send.duration",
		metric.WithDescription("Duration from Send to Completion callback"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create send duration histogram: %w", op, err)
	}

	return &ProducerMetrics{
		messagesSent:   messagesSent,
		messagesErrors: messagesErrors,
		sendDuration:   sendDuration,
	}, nil
}

func (m *ProducerMetrics) RecordSent(ctx context.Context, topic string) {
	m.messagesSent.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (m *ProducerMetrics) RecordError(ctx context.Context, topic string) {
	m.messagesErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (m *ProducerMetrics) RecordDuration(ctx context.Context, topic string, ms float64) {
	m.sendDuration.Record(ctx, ms, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}
