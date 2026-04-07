package outbox

import (
	"context"

	"github.com/fedotovmax/microservice-core/logger"
)

func (p *Outbox) handle(ctx context.Context) {
	const op = "core.messaging.kafka.outbox.Outbox.handle"

	log := p.log.With(logger.String("op", op))

	newCtx, cancel := context.WithTimeout(ctx, p.config.SendTimeout)

	defer cancel()

	events, err := p.adapter.Reserve(newCtx, p.config.BatchLimit, p.config.ReserveDuration)

	if err != nil {
		log.Error("error when reserve events", logger.Err(err))
		return
	}

	if len(events) == 0 {
		log.Debug("skip processing, no new events")
		return
	}

	for idx := range events {
		err := p.producer.Send(newCtx, events[idx])

		if err != nil {
			log.Error("error when send event to kafka", logger.String("event_id", events[idx].GetID()), logger.Err(err))
			continue
		}
	}

}
