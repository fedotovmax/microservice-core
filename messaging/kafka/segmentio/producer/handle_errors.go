package producer

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
)

func (p *producer) HandleErrors(timeout time.Duration, onError kafka.OnError) {

	const op = "core.messaging.kafka.segmentio.producer.HandleErrors"

	log := p.log.With(logger.String("op", op))

	for m := range p.errCh {

		// Извлекаем сохраненную ошибку и метаданные
		failedEvent := kafka.NewFailedMessage(m.WriterData, segmentio.HeadersFromSegmentio(m.Headers), m.Error)

		err := p.handleError(failedEvent, timeout, onError)

		if err != nil {
			log.Error("error when call onError callback", logger.Err(err))
			continue
		}
	}
}

func (p *producer) handleError(e kafka.FailedMessage, timeout time.Duration, onError kafka.OnError) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return onError(ctx, e)
}
