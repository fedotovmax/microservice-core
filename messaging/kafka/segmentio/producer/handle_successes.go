package producer

import (
	"context"
	"strconv"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

func (p *producer) HandleSuccesses(timeout time.Duration, onSuccess kafka.OnSuccess) {

	const op = "core.messaging.kafka.segmentio.producer.HandleSuccesses"

	log := p.log.With(logger.String("op", op))

	withMetrics := p.withMetrics()

	for msg := range p.successCh {

		meta := msg.WriterData

		traceCtx := context.Background()

		if wrapper, ok := meta.(kafka.TelemetryMetaWrapper); ok {
			meta = wrapper.Original

			wrapper.Span.SetAttributes(
				semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
				semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition)),
			)

			traceCtx = trace.ContextWithSpan(context.Background(), wrapper.Span)

			wrapper.Span.End() // Успешно закрываем спан!
			if withMetrics {
				p.metrics.RecordDuration(traceCtx, msg.Topic, float64(time.Since(wrapper.StartTime).Milliseconds()))
			}
		}

		if withMetrics {
			p.metrics.RecordSent(traceCtx, msg.Topic)
		}

		successMsg := kafka.NewMessage(
			string(msg.Key),
			msg.Topic,
			msg.Value,
			segmentio.HeadersFromSegmentio(msg.Headers),
			meta,
		)

		err := p.handleSuccess(traceCtx, successMsg, timeout, onSuccess)

		if err != nil {
			log.Error("error when call onSuccess callback", logger.Err(err))
			continue
		}
	}
}

func (p *producer) handleSuccess(ctx context.Context, e kafka.Message, timeout time.Duration, onSuccess kafka.OnSuccess) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return onSuccess(ctx, e)
}
