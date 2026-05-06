package producer

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
)

func (p *producer) HandleSuccesses(timeout time.Duration, onSuccess kafka.OnSuccess) {

	const op = "core.messaging.kafka.sarama.producer.HandleSuccesses"

	log := p.log.With(logger.String("op", op))

	for event := range p.ap.Successes() {

		successEvent := kafka.NewSuccessMessage(event.Metadata, coreSarama.HeadersFromSarama(event.Headers))

		err := p.handleSuccess(successEvent, timeout, onSuccess)

		if err != nil {
			log.Error("error when call onSuccess callback", logger.Err(err))
			continue
		}
	}
}

func (p *producer) handleSuccess(e kafka.SuccessMessage, timeout time.Duration, onSuccess kafka.OnSuccess) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return onSuccess(ctx, e)
}
