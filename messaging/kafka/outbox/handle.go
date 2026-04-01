package outbox

import (
	"context"

	"github.com/fedotovmax/microservice-core/logger"
)

func (p *Outbox) handle(ctx context.Context) {
	const op = "messaging.kafka.outbox.handle"

	log := p.log.With(logger.String("op", op))

	reserveCtx, cancelReserveCtx := context.WithTimeout(ctx, p.config.OperationTimeout)

	defer cancelReserveCtx()

	events, err := p.adapter.Reserve(reserveCtx, p.config.BatchLimit, p.config.ReserveDuration)

	if err != nil {
		log.Error("error when reserve events", logger.Err(err))
		return
	}

	if len(events) == 0 {
		log.Debug("skip processing, no new events")
		return
	}

	sendCtx, cancelSendCtx := context.WithTimeout(ctx, p.config.OperationTimeout*2)
	defer cancelSendCtx()

	for idx := range events {
		err := p.producer.Send(sendCtx, events[idx])

		if err != nil {
			log.Error("error when send event to kafka", logger.String("event_id", events[idx].GetID()), logger.Err(err))
			continue
		}
	}

}
