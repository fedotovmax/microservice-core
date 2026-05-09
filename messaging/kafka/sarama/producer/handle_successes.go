package producer

import (
	"context"
	"strconv"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

func (p *producer) HandleSuccesses(timeout time.Duration, onSuccess kafka.OnSuccess) {
	const op = "core.messaging.kafka.sarama.producer.HandleSuccesses"
	log := p.log.With(logger.String("op", op))

	for msg := range p.ap.Successes() {
		meta := msg.Metadata

		traceCtx := context.Background()

		// Распаковываем спан и оригинальную метадату
		if wrapper, ok := meta.(kafka.SpanMetaWrapper); ok {
			meta = wrapper.Original

			wrapper.Span.SetAttributes(
				semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
				semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition)),
			)

			traceCtx = trace.ContextWithSpan(context.Background(), wrapper.Span)

			wrapper.Span.End() // Успешно закрываем спан!
		}

		successMsg, err := sarama.NewMessageFromProducer(msg, meta)

		if err != nil {
			log.Error("error when encode producer message to domain kafka message", logger.Err(err))
			continue
		}

		if err := p.handleSuccess(traceCtx, successMsg, timeout, onSuccess); err != nil {
			log.Error("error when call onSuccess callback", logger.Err(err))
		}
	}
}
func (p *producer) handleSuccess(ctx context.Context, e kafka.Message, timeout time.Duration, onSuccess kafka.OnSuccess) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return onSuccess(ctx, e)
}
