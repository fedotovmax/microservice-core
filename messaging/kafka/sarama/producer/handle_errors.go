package producer

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
)

func (p *producer) HandleErrors(timeout time.Duration, onError kafka.OnError) {

	const op = "core.messaging.kafka.sarama.producer.HandleErrors"

	log := p.log.With(logger.String("op", op))

	for event := range p.ap.Errors() {

		failedEvent := kafka.NewFailedMessage(event.Msg.Metadata, coreSarama.HeadersFromSarama(event.Msg.Headers), event.Err)

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
