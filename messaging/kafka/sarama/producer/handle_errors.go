package producer

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (p *producer) HandleErrors(timeout time.Duration, onError kafka.OnError) {
	const op = "core.messaging.kafka.sarama.producer.HandleErrors"
	log := p.log.With(logger.String("op", op))

	for msg := range p.ap.Errors() {
		meta := msg.Msg.Metadata

		traceCtx := context.Background()

		// Распаковываем спан, фиксируем ошибку брокера и закрываем
		if wrapper, ok := meta.(kafka.SpanMetaWrapper); ok {
			meta = wrapper.Original
			wrapper.Span.RecordError(msg.Err)
			wrapper.Span.SetStatus(codes.Error, msg.Err.Error())

			traceCtx = trace.ContextWithSpan(context.Background(), wrapper.Span)

			wrapper.Span.End()
		}

		failedMsg, err := sarama.NewFailedMessageFromProducer(msg, meta)

		if err != nil {
			log.Error("error when encode producer error message to domain kafka failed message", logger.Err(err))
			continue
		}

		if err := p.handleError(traceCtx, failedMsg, timeout, onError); err != nil {
			log.Error("error when call onError callback", logger.Err(err))
		}
	}
}

func (p *producer) handleError(ctx context.Context, e kafka.FailedMessage, timeout time.Duration, onError kafka.OnError) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return onError(ctx, e)
}
