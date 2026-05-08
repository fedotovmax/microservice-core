package producer

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
)

func (p *producer) HandleSuccesses(timeout time.Duration, onSuccess kafka.OnSuccess) {

	const op = "core.messaging.kafka.segmentio.producer.HandleSuccesses"

	log := p.log.With(logger.String("op", op))

	for m := range p.successCh {

		successMsg := kafka.NewMessage(
			string(m.Key),
			m.Topic,
			m.Value,
			segmentio.HeadersFromSegmentio(m.Headers),
			m.WriterData,
		)

		err := p.handleSuccess(successMsg, timeout, onSuccess)

		if err != nil {
			log.Error("error when call onSuccess callback", logger.Err(err))
			continue
		}
	}
}

func (p *producer) handleSuccess(e kafka.Message, timeout time.Duration, onSuccess kafka.OnSuccess) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return onSuccess(ctx, e)
}
