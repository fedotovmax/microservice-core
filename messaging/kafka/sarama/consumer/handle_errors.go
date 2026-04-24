package consumer

import (
	"context"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func (c *group) handleErrors(ctx context.Context, onError kafka.OnConsumeError) {

	const op = "core.messaging.kafka.sarama.consumer.group.handleErrors"

	log := c.log.With(logger.String("op", op))

	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-c.g.Errors():
			if !ok {
				log.Warn("consumer group errors channel closed, stopping error handling")
				return
			}
			if err != nil {
				onError(ctx, err)
			}
		}
	}
}
