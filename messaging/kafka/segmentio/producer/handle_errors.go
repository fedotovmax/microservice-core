package producer

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (p *producer) HandleErrors(timeout time.Duration, onError kafka.OnError) {

	const op = "core.messaging.kafka.segmentio.producer.HandleErrors"

	log := p.log.With(logger.String("op", op))

	for msg := range p.errCh {

		meta := msg.Msg.WriterData

		traceCtx := context.Background()

		// Распаковываем спан, фиксируем ошибку брокера и закрываем
		if wrapper, ok := meta.(kafka.SpanMetaWrapper); ok {
			meta = wrapper.Original
			wrapper.Span.RecordError(msg.Err)
			wrapper.Span.SetStatus(codes.Error, msg.Err.Error())

			traceCtx = trace.ContextWithSpan(context.Background(), wrapper.Span)

			wrapper.Span.End()
		}

		// Извлекаем сохраненную ошибку и метаданные
		failedMsg := kafka.NewFailedMessage(
			kafka.NewMessage(
				string(msg.Msg.Key),
				msg.Msg.Topic,
				msg.Msg.Value,
				segmentio.HeadersFromSegmentio(msg.Msg.Headers),
				meta,
			),
			msg.Err,
		)

		err := p.handleError(traceCtx, failedMsg, timeout, onError)

		if err != nil {
			log.Error("error when call onError callback", logger.Err(err))
			continue
		}
	}
}

func (p *producer) handleError(ctx context.Context, e kafka.FailedMessage, timeout time.Duration, onError kafka.OnError) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return onError(ctx, e)
}
