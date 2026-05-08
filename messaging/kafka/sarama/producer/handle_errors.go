package producer

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
	"go.opentelemetry.io/otel/codes"
)

func (p *producer) HandleErrors(timeout time.Duration, onError kafka.OnError) {
	const op = "core.messaging.kafka.sarama.producer.HandleErrors"
	log := p.log.With(logger.String("op", op))

	for event := range p.ap.Errors() {
		meta := event.Msg.Metadata

		// Распаковываем спан, фиксируем ошибку брокера и закрываем
		if wrapper, ok := meta.(spanWrapper); ok {
			meta = wrapper.original
			wrapper.span.RecordError(event.Err)
			wrapper.span.SetStatus(codes.Error, event.Err.Error())
			wrapper.span.End()
		}

		failedEvent := kafka.NewFailedMessage(meta, coreSarama.HeadersFromSarama(event.Msg.Headers), event.Err)

		if err := p.handleError(failedEvent, timeout, onError); err != nil {
			log.Error("error when call onError callback", logger.Err(err))
		}
	}
}

func (p *producer) handleError(e kafka.FailedMessage, timeout time.Duration, onError kafka.OnError) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return onError(ctx, e)
}
